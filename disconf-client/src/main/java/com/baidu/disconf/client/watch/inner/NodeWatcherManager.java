package com.baidu.disconf.client.watch.inner;

import com.baidu.disconf.client.common.model.DisconfKey;
import com.baidu.disconf.client.core.processor.DisconfCoreProcessor;
import com.baidu.disconf.core.common.constants.DisConfigTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhaoyxiong
 * @date 2023/2/19 10:10 PM
 * @desc NodeWatcher 管理器
 */
public class NodeWatcherManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeWatcherManager.class);

    private static final Map<DisconfKey, NodeWatcher> DISCONF_KEY_MAP = new ConcurrentHashMap<>();

    /**
     * 注册watcher
     */
    public static void registerWatcher(DisconfKey disconfKey, NodeWatcher watcher) {
        if (DISCONF_KEY_MAP.containsKey(disconfKey)) {
            LOGGER.warn("put watcher exist, {}", disconfKey);
        }
        DISCONF_KEY_MAP.put(disconfKey, watcher);
    }

    /**
     * 获取Watcher
     */
    public static NodeWatcher getWatcher(DisconfKey disconfKey) {
        NodeWatcher watcher = DISCONF_KEY_MAP.get(disconfKey);
        if (watcher == null) {
            LOGGER.warn("getWatcher fail, disconfKey:{}", disconfKey);
        }
        return watcher;
    }

    /**
     * 获取WatcherKeyList
     */
    public static List<DisconfKey> getWatcherKeyList() {
        return new ArrayList<>(DISCONF_KEY_MAP.keySet());
    }

    /**
     * 判断幂等并创建实例
     */
    public static NodeWatcher createIfPresent(DisconfCoreProcessor disconfCoreMgr, String monitorPath,
                                              String keyName, DisConfigTypeEnum disConfigTypeEnum,
                                              DisconfSysUpdateCallback disconfSysUpdateCallback, boolean debug) {
        DisconfKey disconfKey = new DisconfKey(disConfigTypeEnum, keyName);
        if (DISCONF_KEY_MAP.containsKey(disconfKey)) {
            return DISCONF_KEY_MAP.get(disconfKey);
        } else {
            return new NodeWatcher(disconfCoreMgr, monitorPath, keyName, disConfigTypeEnum,
                    disconfSysUpdateCallback, debug);
        }
    }
}
