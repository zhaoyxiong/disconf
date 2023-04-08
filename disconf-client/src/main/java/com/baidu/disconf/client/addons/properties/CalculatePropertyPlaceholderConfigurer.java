package com.baidu.disconf.client.addons.properties;

import com.baidu.disconf.client.usertools.PlaceholderManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionVisitor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.core.Ordered;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PropertyPlaceholderHelper;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

import java.util.*;

/**
 * 专用的 后处理器， 用来收集当前
 */
public class CalculatePropertyPlaceholderConfigurer extends DefaultPropertyPlaceholderConfigurer implements InitializingBean, DisposableBean {

    protected static final Logger logger = LoggerFactory.getLogger(CalculatePropertyPlaceholderConfigurer.class);

    // 默认的 property 标识符
    private String placeholderPrefix = DEFAULT_PLACEHOLDER_PREFIX;

    private String placeholderSuffix = DEFAULT_PLACEHOLDER_SUFFIX;

    private String beanName;

    private BeanFactory beanFactory;

    /**
     * 通常会传入 ReloadablePropertiesFactoryBean
     *
     * @see ReloadablePropertiesFactoryBean
     */
    private Properties[] propertiesArray;


    private static final Map<String, String> wellKnownSimplePrefixes = new HashMap<String, String>(4);

    static {
        wellKnownSimplePrefixes.put("}", "{");
        wellKnownSimplePrefixes.put("]", "[");
        wellKnownSimplePrefixes.put(")", "(");
    }

    /**
     * 当前 解析方法不做任何 占位符的解析动作，直接获取到占位符并且保存下来，传入的值还是按照原样返回
     *
     * @param value
     * @param props
     * @param visitedPlaceholders
     * @return
     * @throws BeanDefinitionStoreException
     */
    protected String parseStringValue(String value, Properties props, Set<?> visitedPlaceholders) throws BeanDefinitionStoreException {

        PropertyPlaceholderHelper.PlaceholderResolver resolver = new PropertyPlaceholderConfigurerResolver(props);
        this.parseStringValue(value, resolver, visitedPlaceholders);
        // 注意此处 返回的还是原参数，本解析器 只对数据进行收集，不做任何数据解析的后返回，以免影响其他的解析器
        return value;
    }

    protected String parseStringValue(String value, PropertyPlaceholderHelper.PlaceholderResolver placeholderResolver, Set visitedPlaceholders) {
        StringBuilder result = new StringBuilder(value);
        int startIndex = value.indexOf(this.placeholderPrefix);

        /**
         *  循环遍历 支持嵌套占位符
         *  eg: @Value(${${application.name}.port:9000})
         */
        while (startIndex != -1) {
            int endIndex = this.findPlaceholderEndIndex(result, startIndex);
            if (endIndex != -1) {
                String placeholder = result.substring(startIndex + this.placeholderPrefix.length(), endIndex);
                String originalPlaceholder = placeholder;
                if (!visitedPlaceholders.add(placeholder)) {
                    throw new IllegalArgumentException("Circular placeholder reference '" + placeholder + "' in property definitions");
                }
                placeholder = this.parseStringValue(placeholder, placeholderResolver, visitedPlaceholders);
                // 注册当前栈中的占位符，如果是嵌套的也注册
                placeholderResolver.resolvePlaceholder(placeholder);
                // 注册到全局default 配置 容器中
                PlaceholderManager.registerDefault(placeholder);
                startIndex = result.indexOf(this.placeholderPrefix, endIndex + this.placeholderSuffix.length());

                // 采用解析器 解析 占位符真实的值
                String propVal = placeholderResolver.resolvePlaceholder(placeholder);
                if (propVal == null && this.valueSeparator != null) {
                    int separatorIndex = placeholder.indexOf(this.valueSeparator);
                    if (separatorIndex != -1) {
                        String actualPlaceholder = placeholder.substring(0, separatorIndex);
                        String defaultValue = placeholder.substring(separatorIndex + this.valueSeparator.length());
                        propVal = placeholderResolver.resolvePlaceholder(actualPlaceholder);
                        if (propVal == null) {
                            propVal = defaultValue;
                        }
                    }
                }
                if (propVal != null) {
                    // Recursive invocation, parsing placeholders contained in the
                    // previously resolved placeholder value.
                    propVal = parseStringValue(propVal, placeholderResolver, visitedPlaceholders);
                    result.replace(startIndex, endIndex + this.placeholderSuffix.length(), propVal);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Resolved placeholder '" + placeholder + "'");
                    }
                    startIndex = result.indexOf(this.placeholderPrefix, startIndex + propVal.length());
                } else if (this.ignoreUnresolvablePlaceholders) {
                    // Proceed with unprocessed value.
                    startIndex = result.indexOf(this.placeholderPrefix, endIndex + this.placeholderSuffix.length());
                } else {
                    throw new IllegalArgumentException("Could not resolve placeholder '" +
                            placeholder + "'" + " in value \"" + value + "\"");
                }
                visitedPlaceholders.remove(originalPlaceholder);
            } else {
                startIndex = -1;
            }
        }
        return result.toString();
    }


    /**
     * 自定义内部类，解决Spring 5.0.0 不支持问题
     */
    private class PropertyPlaceholderConfigurerResolver implements PropertyPlaceholderHelper.PlaceholderResolver {

        private final Properties props;

        private PropertyPlaceholderConfigurerResolver(Properties props) {
            this.props = props;
        }

        @Override
        public String resolvePlaceholder(String placeholderName) {
            PlaceholderManager.registerDefault(placeholderName);
            return resolvePlaceholder(placeholderName, props, 1);
        }

        protected String resolvePlaceholder(String placeholder, Properties props, int systemPropertiesMode) {
            String propVal = null;
            if (systemPropertiesMode == SYSTEM_PROPERTIES_MODE_OVERRIDE) {
                propVal = resolveSystemProperty(placeholder);
            }
            if (propVal == null) {
                propVal = props.getProperty(placeholder);
            }
            if (propVal == null && systemPropertiesMode == SYSTEM_PROPERTIES_MODE_FALLBACK) {
                propVal = resolveSystemProperty(placeholder);
            }
            return propVal;
        }
    }

    /**
     * 重写 helper 中的 私有方法 #findPlaceholderEndIndex， 其中 simplePrefix 变量初始化 在当前方法中初始化
     */
    private int findPlaceholderEndIndex(CharSequence buf, int startIndex) {
        String simplePrefix = "";
        String simplePrefixForSuffix = wellKnownSimplePrefixes.get(this.placeholderSuffix);
        if (simplePrefixForSuffix != null && this.placeholderPrefix.endsWith(simplePrefixForSuffix)) {
            simplePrefix = simplePrefixForSuffix;
        } else {
            simplePrefix = this.placeholderPrefix;
        }

        int index = startIndex + this.placeholderPrefix.length();
        int withinNestedPlaceholder = 0;

        while (index < buf.length()) {
            if (StringUtils.substringMatch(buf, index, this.placeholderSuffix)) {
                if (withinNestedPlaceholder <= 0) {
                    return index;
                }

                --withinNestedPlaceholder;
                index += this.placeholderSuffix.length();
            } else if (StringUtils.substringMatch(buf, index, simplePrefix)) {
                ++withinNestedPlaceholder;
                index += simplePrefix.length();
            } else {
                ++index;
            }
        }
        return -1;
    }

    private String currentBeanName;
    private String currentPropertyName;

    /**
     * copy & paste, just so we can insert our own visitor.
     * 启动时 进行配置的解析
     * <p>
     * 此处增加配置，完成properties装载、bean 中的 $value 替换（ 我们在当前占位符中发挥的作用是 ）
     */
    protected void processProperties(ConfigurableListableBeanFactory beanFactoryToProcess, Properties props)
            throws BeansException {

        PlaceholderResolvingBeanDefinitionVisitor visitor = new PlaceholderResolvingBeanDefinitionVisitor(props);
        String[] beanNames = beanFactoryToProcess.getBeanDefinitionNames();
        for (String name : beanNames) {
            if (!(name.equals(this.beanName) && beanFactoryToProcess.equals(this.beanFactory))) {
                this.currentBeanName = name;
                try {
                    BeanDefinition bd = beanFactoryToProcess.getBeanDefinition(name);
                    try {
                        // 不对整个bean 做全量的 def定义
                        visitor.visitBeanClassName(bd);
                        visitor.visitPropertyValues(bd.getPropertyValues());
                    } catch (BeanDefinitionStoreException ex) {
                        throw new BeanDefinitionStoreException(bd.getResourceDescription(), name, ex.getMessage());
                    }
                } finally {
                    currentBeanName = null;
                }
            }
        }

        // 占位符 解析器， 采用当前 类自己的解析器
        StringValueResolver stringValueResolver = new CalculatePropertyPlaceholderConfigurer.PlaceholderResolvingStringValueResolver(props);

        // New in Spring 2.5: resolve placeholders in alias target names and aliases as well.
        beanFactoryToProcess.resolveAliases(stringValueResolver);

        // New in Spring 3.0: resolve placeholders in embedded values such as annotation attributes.
        // 增加嵌入 解析器
        beanFactoryToProcess.addEmbeddedValueResolver(stringValueResolver);
    }

    /**
     * afterPropertiesSet
     * 将自己 添加 property listener
     */
    public void afterPropertiesSet() {
        for (Properties properties : propertiesArray) {
            if (properties instanceof ReloadableProperties) {
                logger.debug("add property listener: " + properties.toString());
//                ((ReloadableProperties) properties).addReloadablePropertiesListener(this);
            }
        }
    }

    /**
     * destroy
     * 删除 property listener
     *
     * @throws Exception
     */
    public void destroy() throws Exception {
        for (Properties properties : propertiesArray) {
            if (properties instanceof ReloadableProperties) {
                logger.debug("remove property listener: " + properties.toString());
//                ((ReloadableProperties) properties).removeReloadablePropertiesListener(this);
            }
        }
    }

    /**
     * 替换掉spring的 config resolver，这样我们才可以解析自己的config
     */
    private class PlaceholderResolvingBeanDefinitionVisitor extends BeanDefinitionVisitor {

        private final Properties props;

        public PlaceholderResolvingBeanDefinitionVisitor(Properties props) {
            this.props = props;
        }

        @Override
        public void visitBeanDefinition(BeanDefinition beanDefinition) {
            super.visitBeanDefinition(beanDefinition);
        }

        @Override
        protected void visitBeanClassName(BeanDefinition beanDefinition) {
            super.visitBeanClassName(beanDefinition);
            List<String> placeholderList = new ArrayList<>();

            // 遍历 value, 如果有${}, 则记录在本地占位符缓存 中
            for (ConstructorArgumentValues.ValueHolder valueHolder : beanDefinition.getConstructorArgumentValues().getGenericArgumentValues()) {
                if (null == valueHolder.getValue()) {
                    continue;
                }
                String suspectedPlaceholder = valueHolder.getValue().toString();
                this.buildPlaceholderList(placeholderList, suspectedPlaceholder);
            }
            if (placeholderList.size() > 0) {
                PlaceholderManager.register(currentBeanName, placeholderList);
            }
        }

        /**
         * 渲染bean 对应的properties 值
         */
        @Override
        protected void visitPropertyValues(MutablePropertyValues pvs) {
            PropertyValue[] pvArray = pvs.getPropertyValues();
            List<String> placeholderList = new ArrayList<>(pvArray.length);
            for (PropertyValue propertyValue : pvArray) {
                currentPropertyName = propertyValue.getName();
                if (null == propertyValue.getValue()) {
                    continue;
                }
                String suspectedPlaceholder = propertyValue.getValue().toString();
                this.buildPlaceholderList(placeholderList, suspectedPlaceholder);
                try {
                    Object newVal = resolveValue(propertyValue.getValue());
                    if (!ObjectUtils.nullSafeEquals(newVal, propertyValue.getValue())) {
                        // todo 2023/4/8 3:50 PM zyx : 此处不需要增加多余的业务逻辑
//                        pvs.addPropertyValue(propertyValue.getName(), newVal);
                    }
                } finally {
                    currentPropertyName = null;
                }
            }
            if (placeholderList.size() > 0) {
                PlaceholderManager.register(currentBeanName, placeholderList);
            }
        }

        private void buildPlaceholderList(List<String> placeholderList, String suspectedPlaceholder) {
            int startIndex = suspectedPlaceholder.indexOf(DEFAULT_PLACEHOLDER_PREFIX);
            while (startIndex != -1) {
                int endIndex = suspectedPlaceholder.indexOf(DEFAULT_PLACEHOLDER_SUFFIX, startIndex + DEFAULT_PLACEHOLDER_PREFIX.length());
                if (endIndex != -1) {
                    if (currentBeanName != null && currentPropertyName != null) {
                        String placeholder = suspectedPlaceholder.substring(startIndex + DEFAULT_PLACEHOLDER_PREFIX.length(), endIndex);
                        placeholder = getPlaceholderSelf(placeholder);
                        placeholderList.add(placeholder);
                    } else {
                        logger.debug("dynamic property outside bean property value - ignored: " + DEFAULT_PLACEHOLDER_PREFIX);
                    }
                    startIndex = endIndex - DEFAULT_PLACEHOLDER_PREFIX.length() + DEFAULT_PLACEHOLDER_PREFIX.length() + DEFAULT_PLACEHOLDER_SUFFIX.length();
                    startIndex = suspectedPlaceholder.indexOf(DEFAULT_PLACEHOLDER_PREFIX, startIndex);
                } else {
                    startIndex = -1;
                }
            }
        }

        protected final String getPlaceholderSelf(final String placeholderWithDefault) {
            String placeholder = getPlaceholder(placeholderWithDefault);
            int separatorIdx = placeholder.indexOf(DEFAULT_VALUE_SEPARATOR);
            if (separatorIdx == -1) {
                return placeholder;
            }
            return placeholder.substring(0, separatorIdx);
        }

        protected String resolveStringValue(String strVal) throws BeansException {
            return parseStringValue(strVal, this.props, new HashSet<String>());
        }
    }

    /**
     *
     */
    protected class PlaceholderResolvingStringValueResolver implements StringValueResolver {

        private final Properties props;

        public PlaceholderResolvingStringValueResolver(Properties props) {
            this.props = props;
        }

        @Override
        public String resolveStringValue(String strVal) throws BeansException {
            return parseStringValue(strVal, this.props, new HashSet<String>());
        }
    }

    public void setProperties(Properties properties) {
        setPropertiesArray(new Properties[]{properties});
    }

    public void setPropertiesArray(Properties[] propertiesArray) {
        this.propertiesArray = propertiesArray;
        super.setPropertiesArray(propertiesArray);
    }

    public void setPlaceholderPrefix(String placeholderPrefix) {
        this.placeholderPrefix = placeholderPrefix;
        super.setPlaceholderPrefix(placeholderPrefix);
    }

    public void setPlaceholderSuffix(String placeholderSuffix) {
        this.placeholderSuffix = placeholderSuffix;
        super.setPlaceholderSuffix(placeholderPrefix);
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
        super.setBeanName(beanName);
    }

    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
        super.setBeanFactory(beanFactory);
    }

    // 通过设定顺序，让自定义的reload configurer 提前执行
    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}

