package com.baidu.disconf.client.usertools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhaoyxiong
 * @date 2023/2/19 10:10 PM
 * @desc Integer 管理器
 */
public class PlaceholderManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlaceholderManager.class);

    private static final LinkedHashMap<String, List<String>> PLACEHOLDER_USED_MAP = new LinkedHashMap<>(1024, 0.75f, false);

    private static final Map<String, Integer> PLACEHOLDER_USED_COUNT_MAP = new ConcurrentHashMap<>(1024, 0.75f, 1);

    private static final AtomicBoolean hasPutClassName = new AtomicBoolean(false);

    private static final AtomicBoolean hasPutProperties = new AtomicBoolean(true);
    public static final String DEFAULT = "default";

    private static Resource[] locations;

    /**
     * 开始注册，当前只记录className
     */
    public static synchronized void startRegister(final String className) {
//        // 防止历史出现过
        PLACEHOLDER_USED_MAP.remove(className);
        // 如果为false，说明已经写入属性或者为初始值
        if (hasPutProperties.compareAndSet(true, false) || PLACEHOLDER_USED_MAP.size() == 0) {
            PLACEHOLDER_USED_MAP.put(className, null);
        } else {
            // 替换写入新的className
            String lastKey = "";
            for (Map.Entry<String, List<String>> stringListEntry : PLACEHOLDER_USED_MAP.entrySet()) {
                lastKey = stringListEntry.getKey();
            }
            if (PLACEHOLDER_USED_MAP.get(lastKey) != null && PLACEHOLDER_USED_MAP.get(lastKey).size() > 0) {
                LOGGER.error("startRegister , className:{} not null, but will be remove", lastKey);
            }
            PLACEHOLDER_USED_MAP.remove(lastKey);
            PLACEHOLDER_USED_MAP.put(className, null);
        }
        hasPutClassName.set(true);
    }

    /**
     * 结束记录，保存之前className 内的所有 placeholder
     */
    public static synchronized void endRegister(List<String> placeholderList) {
        if (hasPutClassName.compareAndSet(true, false)) {
            // 给指定key， 赋值 placeholder
            Set<String> s = PLACEHOLDER_USED_MAP.keySet();
            String lastKey = "";
            for (Map.Entry<String, List<String>> stringListEntry : PLACEHOLDER_USED_MAP.entrySet()) {
                lastKey = stringListEntry.getKey();
            }
            PLACEHOLDER_USED_MAP.put(lastKey, placeholderList);
        } else {
            LOGGER.warn("endRegister before className not put, size:{}, placeholderList:{}", placeholderList.size(), placeholderList);
        }
        hasPutProperties.set(true);
    }

    /**
     * 结束记录，保存之前className 内的所有 placeholder
     */
    public static synchronized void register(String key, List<String> placeholderList) {
        PLACEHOLDER_USED_MAP.put(key, placeholderList);
    }

    /**
     * 结束记录，保存之前className 内的所有 placeholder
     */
    public static synchronized void registerDefault(String placeholder) {
        List<String> placeholderList = new ArrayList<>();
        placeholderList.add(placeholder);
        if (PLACEHOLDER_USED_MAP.get(DEFAULT) == null) {
            PLACEHOLDER_USED_MAP.put(DEFAULT, placeholderList);
        } else {
            PLACEHOLDER_USED_MAP.get(DEFAULT).addAll(placeholderList);
        }
    }

    /**
     * 获取 占位符 是否使用
     */
    public static boolean exist(String placeholder) {
        for (Map.Entry<String, List<String>> classPlaceholder : PLACEHOLDER_USED_MAP.entrySet()) {
            if (classPlaceholder.getValue().contains(placeholder)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取 所有 匹配过的 占位符
     */
    public static List<String> getPlaceholderList() {
        List<String> placeholderList = new ArrayList<>(PLACEHOLDER_USED_MAP.size());
        for (Map.Entry<String, List<String>> classPlaceholder : PLACEHOLDER_USED_MAP.entrySet()) {
            placeholderList.addAll(classPlaceholder.getValue());
        }
        return placeholderList;
    }

//    public static void main(String[] args) {
//        startRegister("c1");
//        List<String> c1 = new ArrayList<String>();
//        c1.add("12");
//        c1.add("13");
//        endRegister(c1);
//        startRegister("c2");
//        startRegister("v2");
//        List<String> c2 = new ArrayList<String>();
//        c2.add("12");
//        startRegister("1235");
//        startRegister("c235");
//        startRegister("c435");
//        c2.add("13");
//        endRegister(c2);
//        startRegister("545c3");
//        List<String> c3 = new ArrayList<String>();
//        startRegister("c3425");
//        startRegister("4656c5");
//        startRegister("xcvc5");
//        c3.add("12");
//        c3.add("13");
//        endRegister(c3);
//        startRegister("fdfc5");
//        startRegister("757c5");
//        startRegister("gghc5");
//        startRegister("c5");
//        startRegister("ghfghc5");
//        startRegister("c5");
//        startRegister("fhjfghc5");
//        startRegister("jhfjc5");
//        List<String> c4 = new ArrayList<String>();
//        c4.add("12");
//        c4.add("13");
//        endRegister(c4);
//
//
//    }

    /**
     * 统计当前接口 在服务启动后会被调用多少次
     *
     * @param placeholder 参数
     */
    public static void increment(String placeholder) {
        if (PLACEHOLDER_USED_COUNT_MAP.containsKey(placeholder)) {
            PLACEHOLDER_USED_COUNT_MAP.put(placeholder, PLACEHOLDER_USED_COUNT_MAP.get(placeholder) + 1);
        } else {
            PLACEHOLDER_USED_COUNT_MAP.put(placeholder, 1);
        }
    }

    /**
     * 获取接口使用情况
     */
    public static Map<String, Integer> getPlaceholderUsedCountMap() {
        return PLACEHOLDER_USED_COUNT_MAP;
    }

    /**
     * 记录当前服务的 locations 文件列表
     */
    public static void setLocations(Resource[] locations) {
        PlaceholderManager.locations = locations;
    }

    /**
     * 获取location列表
     */
    public static Resource[] getLocations() {
        return PlaceholderManager.locations;
    }

    public static void main(String[] args) {
        // 插入排序
        LinkedHashMap<Integer, Integer> linkedHashMap = new LinkedHashMap<>(8, 0.75F, false);
        linkedHashMap.put(1, 2);
        linkedHashMap.put(2, 2);
        linkedHashMap.put(4, 2);
        linkedHashMap.put(5, 2);
        linkedHashMap.put(3, 2);
        linkedHashMap.put(6, 2);
//        linkedHashMap.get(1);
        System.out.println(linkedHashMap.keySet());
        System.out.println(linkedHashMap.entrySet());

        // 根据读取及进行排序
        LinkedHashMap<Integer, Integer> linkedHashMapAccess = new LinkedHashMap<>(8, 0.75F, true);
        linkedHashMapAccess.put(1, 2);
        linkedHashMapAccess.get(1);
        linkedHashMapAccess.put(2, 2);
        linkedHashMapAccess.get(2);
        linkedHashMapAccess.put(3, 2);
        linkedHashMapAccess.get(3);
        linkedHashMapAccess.put(4, 2);
        linkedHashMapAccess.get(4);
        linkedHashMapAccess.put(5, 2);
        linkedHashMapAccess.get(5);
        linkedHashMapAccess.put(6, 2);
        linkedHashMapAccess.get(6);
        System.out.println(linkedHashMapAccess.keySet());
    }
}
