package com.baidu.disconf.client.watch.impl;

import com.baidu.disconf.client.common.model.DisConfCommonModel;
import com.baidu.disconf.client.config.inner.DisClientComConfig;
import com.baidu.disconf.client.core.processor.DisconfCoreProcessor;
import com.baidu.disconf.client.curator.ZookeeperManager;
import com.baidu.disconf.client.watch.WatchMgr;
import com.baidu.disconf.client.watch.inner.DisconfSysUpdateCallback;
import com.baidu.disconf.client.watch.inner.NodeWatcher;
import com.baidu.disconf.client.watch.inner.WatcherInnerManager;
import com.baidu.disconf.core.common.constants.DisConfigTypeEnum;
import com.baidu.disconf.core.common.path.ZooPathMgr;
import com.baidu.disconf.core.common.utils.ZooUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhaoyxiong
 * @date 2023/2/19 5:41 PM
 * @desc 自定义Watch 实现，底层通过Curator 实现zk 控制
 */
public class WatchMgrCuratorImpl implements WatchMgr {

    protected static final Logger LOGGER = LoggerFactory.getLogger(WatchMgrCuratorImpl.class);

    /**
     * zoo prefix
     */
    private String zooUrlPrefix;

    private boolean debug;

    /**
     * 初始化，建立zk 连接
     */
    public void init(String hosts, String zooUrlPrefix, boolean debug) throws Exception {
        this.zooUrlPrefix = zooUrlPrefix;
        this.debug = debug;
        ZookeeperManager.getInstance().init(hosts, zooUrlPrefix, debug);
    }

    /**
     * 监控路径,监控前会事先创建路径,并且会新建一个自己的Temp子结点
     */
    public void watchPath(DisconfCoreProcessor disconfCoreMgr, DisConfCommonModel disConfCommonModel, String keyName,
                          DisConfigTypeEnum disConfigTypeEnum, String value) throws Exception {
        // 1、构建路径
        String monitorPath = this.makeMonitorPath(disConfigTypeEnum, disConfCommonModel, keyName);

        // 2、新建一个代表自己的临时结点【此处为主要的监控节点】，有幂等
        this.makeTempChildPath(monitorPath, value);

        // 3、初始化监控节点【需要幂等】
        NodeWatcher nodeWatcher = WatcherInnerManager.createIfPresent(disconfCoreMgr, monitorPath, keyName, disConfigTypeEnum,
                new DisconfSysUpdateCallback(), debug);
        // 4、监听
        nodeWatcher.monitorMaster();
    }

    /**
     * 新建监控路径
     */
    private String makeMonitorPath(DisConfigTypeEnum disConfigTypeEnum, DisConfCommonModel disConfCommonModel,
                                   String key) throws Exception {
        // 应用根目录
        /*
            应用程序的 Zoo 根目录
        */
        String clientRootZooPath = ZooPathMgr.getZooBaseUrl(zooUrlPrefix, disConfCommonModel.getApp(),
                disConfCommonModel.getEnv(),
                disConfCommonModel.getVersion());
        // 1、此处实际业务为 initZkClient
        ZookeeperManager.getInstance().makeDir(clientRootZooPath, ZooUtils.getIp());

        // 2、建立监控
        String monitorPath;
        if (disConfigTypeEnum.equals(DisConfigTypeEnum.FILE)) {

            // 新建Zoo Store目录
            String clientDisconfFileZooPath = ZooPathMgr.getFileZooPath(clientRootZooPath);
            ZookeeperManager.getInstance().makeDir(clientDisconfFileZooPath, ZooUtils.getIp());
            monitorPath = ZooPathMgr.joinPath(clientDisconfFileZooPath, key);

        } else {

            // 新建Zoo Store目录
            String clientDisconfItemZooPath = ZooPathMgr.getItemZooPath(clientRootZooPath);
            ZookeeperManager.getInstance().makeDir(clientDisconfItemZooPath, ZooUtils.getIp());
            monitorPath = ZooPathMgr.joinPath(clientDisconfItemZooPath, key);
        }

        // 先新建路径
        ZookeeperManager.getInstance().makeDir(monitorPath, "");

        return monitorPath;
    }

    /**
     * 在指定路径下创建一个临时结点【幂等】
     */
    private void makeTempChildPath(String path, String data) {

        String finerPrint = DisClientComConfig.getInstance().getInstanceFingerprint();

        String mainTypeFullStr = path + "/" + finerPrint;
        try {
            ZookeeperManager.getInstance().createEphemeralNode(mainTypeFullStr, data);
        } catch (Exception e) {
            LOGGER.error("cannot create: " + mainTypeFullStr + "\t" + e.toString());
        }
    }

    @Override
    public void release() {

        try {
//            ZookeeperMgr.getInstance().release();
            ZookeeperManager.getInstance().release();
        } catch (InterruptedException e) {

            LOGGER.error(e.toString());
        }
    }

}

