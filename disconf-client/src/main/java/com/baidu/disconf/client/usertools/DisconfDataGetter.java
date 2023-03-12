package com.baidu.disconf.client.usertools;

import java.util.*;

import com.baidu.disconf.client.common.constants.SupportFileTypeEnum;
import com.baidu.disconf.client.core.filetype.FileTypeProcessorUtils;
import com.baidu.disconf.client.usertools.impl.DisconfDataGetterDefaultImpl;

/**
 * Created by knightliao on 16/5/28.
 */
public class DisconfDataGetter {

    private static final IDisconfDataGetter iDisconfDataGetter = new DisconfDataGetterDefaultImpl();

    /**
     * 根据 分布式配置文件 获取该配置文件的所有数据，以 map形式呈现
     *
     * @param fileName
     * @return
     */
    public static Map<String, Object> getByFile(String fileName) {
        return iDisconfDataGetter.getByFile(fileName);
    }

    /**
     * 获取 分布式配置文件 获取该配置文件 中 某个配置项 的值
     *
     * @param fileName
     * @param fileItem
     * @return
     */
    public static Object getByFileItem(String fileName, String fileItem) {
        return iDisconfDataGetter.getByFileItem(fileName, fileItem);
    }

    /**
     * 根据 分布式配置 获取其值
     *
     * @param itemName
     * @return
     */
    public static Object getByItem(String itemName) {
        return iDisconfDataGetter.getByItem(itemName);
    }

    /**
     * 获取对应文件中没有使用到的配置
     *
     * @param fileName
     * @return
     */
    public static List<String> getUnUsedConfig(String fileName) {
        Map<String, Object> fileProperties = new HashMap<>();
        try {
            fileProperties = FileTypeProcessorUtils.getKvMap(SupportFileTypeEnum.PROPERTIES, fileName);
        } catch (Exception e) {
        }
        Map<String, Object> itemMap = iDisconfDataGetter.getByFile(fileName);
        List<String> result = new ArrayList<>();
        Set<String> placeholderSet = new HashSet<>(PlaceholderManager.getPlaceholderList());
        for (Map.Entry<String, Object> entry : fileProperties.entrySet()) {
            String key = entry.getKey();
            if (!itemMap.containsKey(key) && !placeholderSet.contains(key)) {
                result.add(key);
            }
        }
        return result;
    }
}
