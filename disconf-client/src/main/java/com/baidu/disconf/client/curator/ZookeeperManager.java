package com.baidu.disconf.client.curator;

import com.baidu.disconf.client.curator.inner.WatcherManager;
import com.baidu.disconf.core.common.utils.ZooUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author zhaoyxiong
 * @date 2023/2/19 6:56 PM
 * @desc curator 定义的 zookeeperManager
 */
public class ZookeeperManager {

    private final Logger log = LoggerFactory.getLogger(ZookeeperManager.class);

    private ZookeeperManager() {
    }

    /**
     * 依赖 watcher 管理类
     */
    private WatcherManager watcherManager;

    public List<String> getRootChildren() {
        return watcherManager.getRootChildren();
    }

    /**
     * 静态内部类， 延迟加载
     */
    private static class SingletonHolder {
        private static final ZookeeperManager instance = new ZookeeperManager();
    }

    /**
     * 加载单例
     */
    public static ZookeeperManager getInstance() {
        return SingletonHolder.instance;
    }

    /**
     * 初始化（实现将初始化和 监听拆分成两个部分）
     */
    public void init(String host, String defaultPrefixString, boolean debug) throws Exception {
        try {
            initInternal(host, defaultPrefixString, debug);
            log.debug("ZookeeperManager init.");
        } catch (Exception e) {
            throw new Exception("zookeeper init failed. ", e);
        }
    }

    private void initInternal(String hosts, String defaultPrefixString, boolean debug)
            throws IOException, InterruptedException {

        watcherManager = WatcherManager.getInstance();
        watcherManager.connect(hosts);

        log.info("zoo prefix: " + defaultPrefixString);

        // 新建父目录
        makeDir(defaultPrefixString, ZooUtils.getIp());
    }

    /**
     * Zoo的新建目录【方法幂等】
     *
     * @param dir 建立路径
     */
    public void makeDir(String dir, String data) {
        try {
            boolean defaultPathExist = watcherManager.exists(dir);
            if (!defaultPathExist) {
                log.info("create: " + dir);
                // 建立持久化节点
                this.writePersistentUrl(dir, data);
            }
        } catch (KeeperException e) {
            log.error("cannot create path: KeeperException " + dir, e);
        } catch (Exception e) {
            log.error("cannot create path: Exception " + dir, e);
        }
    }

    /**
     * 释放Curator资源
     */
    public void release() throws InterruptedException {
        watcherManager.close();
    }

    /**
     * 写持久化结点, 没有则新建, 存在则进行更新
     */
    public void writePersistentUrl(String url, String value) throws Exception {

        watcherManager.write(url, value);
    }

    /**
     * 读结点数据
     */
    public String readUrl(String url, Watcher watcher) throws Exception {

        return watcherManager.read(url, watcher, null);
    }

    /**
     * 生成一个临时节点【重要】
     */
    public boolean createEphemeralNode(String path, String value) throws Exception {

        return watcherManager.createEphemeralNode(path, value);
    }

    /**
     * @param path
     * @param watcher
     * @param stat
     * @return String
     * @throws InterruptedException
     * @throws KeeperException
     * @Description: 带状态信息的读取数据
     * @author liaoqiqi
     * @date 2013-6-17
     */
    public String read(String path, Watcher watcher, Stat stat) throws Exception {
        // todo 2023/2/20 12:44 AM zyx  考虑增加 createNodeIfNeeded(propFilePath, "");
        // 提升程序的健壮性
        return watcherManager.read(path, watcher, stat);
    }
}
