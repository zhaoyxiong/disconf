package com.baidu.disconf.client.watch.inner;

import com.baidu.disconf.client.common.model.DisconfKey;
import com.baidu.disconf.client.core.processor.DisconfCoreProcessor;
import com.baidu.disconf.client.curator.ZookeeperManager;
import com.baidu.disconf.core.common.constants.DisConfigTypeEnum;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 结点监控器
 *
 * @author liaoqiqi
 * @version 2014-6-16
 */
public class NodeWatcher implements Watcher {

    protected static final Logger log = LoggerFactory.getLogger(NodeWatcher.class);

    /**
     * 唯一标识
     */
    private DisconfKey disconfKey = null;

    private String monitorPath = "";
    private String keyName = "";
    private DisConfigTypeEnum disConfigTypeEnum;
    private DisconfSysUpdateCallback disconfSysUpdateCallback;
    private boolean debug;

    private DisconfCoreProcessor disconfCoreMgr;

    /**
     * 监控节点注册
     */
    public NodeWatcher(DisconfCoreProcessor disconfCoreMgr, String monitorPath, String keyName,
                       DisConfigTypeEnum disConfigTypeEnum, DisconfSysUpdateCallback disconfSysUpdateCallback,
                       boolean debug) {

        super();
        this.debug = debug;
        this.disconfCoreMgr = disconfCoreMgr;
        this.monitorPath = monitorPath;
        this.keyName = keyName;
        this.disConfigTypeEnum = disConfigTypeEnum;
        this.disconfSysUpdateCallback = disconfSysUpdateCallback;
        this.disconfKey = new DisconfKey(disConfigTypeEnum, keyName);
        // 创建时注册到WatcherManager 中
        NodeWatcherManager.registerWatcher(this.disconfKey, this);
    }

    /**
     * 监控当前nodeWatcher， 调用ZookeeperManager 建立 getData
     */
    public void monitorMaster() {

        Stat stat = new Stat();
        try {
//            ZookeeperMgr.getInstance().read(monitorPath, this, stat);
            ZookeeperManager.getInstance().read(monitorPath, this, stat);
        } catch (InterruptedException e) {
            log.info(e.toString());
        } catch (KeeperException e) {
            log.error("cannot monitor " + monitorPath, e);
        } catch (Exception e) {
            throw new IllegalStateException("monitor Master fail, ", e);
        }
        log.debug("monitor path: ({}, {}, {}) has been added!", monitorPath, keyName, disConfigTypeEnum.getModelName());
    }

    /**
     * 回调函数
     */
    @Override
    public void process(WatchedEvent event) {

        //
        // 结点更新时
        //
        if (event.getType() == EventType.NodeDataChanged) {

            try {

                log.info("============GOT UPDATE EVENT " + event.toString() + ": (" + monitorPath + "," + keyName
                        + "," + disConfigTypeEnum.getModelName() + ")======================");

                // 调用回调函数, 回调函数里会重新进行监控
                callback();

            } catch (Exception e) {

                log.error("monitor node exception. " + monitorPath, e);
            }
        }

        //
        // 结点断开连接，这时不要进行处理
        //
        if (event.getState() == KeeperState.Disconnected) {

            if (!debug) {
                log.warn("============GOT Disconnected EVENT " + event.toString() + ": (" + monitorPath + ","
                        + keyName + "," + disConfigTypeEnum.getModelName() + ")======================");
            } else {
                log.debug("============DEBUG MODE: GOT Disconnected EVENT " + event.toString() + ": (" +
                        monitorPath +
                        "," +
                        keyName +
                        "," + disConfigTypeEnum.getModelName() + ")======================");
            }
        }

        //
        // session expired，需要重新关注哦
        //
        if (event.getState() == KeeperState.Expired) {

            if (!debug) {

                log.error("============GOT Expired  " + event.toString() + ": (" + monitorPath + "," + keyName
                        + "," + disConfigTypeEnum.getModelName() + ")======================");

                // 重新连接
//                ZookeeperMgr.getInstance().reconnect();

                callback();
            } else {
                log.debug("============DEBUG MODE: GOT Expired  " + event.toString() + ": (" + monitorPath + ","
                        + "" + keyName + "," + disConfigTypeEnum.getModelName() + ")======================");
            }
        }
    }

    /**
     * 回调时触发重新 reload 数据
     */
    private void callback() {
        reload();
    }

    /**
     * 重新reload 当前监听的 keyName
     */
    public void reload() {

        try {
            // 调用回调函数, 回调函数里会重新进行监控
            try {
                disconfSysUpdateCallback.reload(disconfCoreMgr, disConfigTypeEnum, keyName);
            } catch (Exception e) {
                log.error(e.toString(), e);
            }

        } catch (Exception e) {

            log.error("monitor node exception. " + monitorPath, e);
        }
    }
}
