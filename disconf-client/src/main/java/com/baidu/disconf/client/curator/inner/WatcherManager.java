package com.baidu.disconf.client.curator.inner;

import com.baidu.disconf.client.common.model.DisconfKey;
import com.baidu.disconf.client.watch.inner.NodeWatcherManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhaoyxiong
 * @date 2023/2/19 7：49 PM
 * @desc watcher管理类， 控制 client 连接
 */
public class WatcherManager {

    protected static final Logger LOGGER = LoggerFactory.getLogger(WatcherManager.class);

    private final static WatcherManager INSTANCE = new WatcherManager();

    private WatcherManager() {
    }

    public static WatcherManager getInstance() {
        return INSTANCE;
    }

    private CuratorFramework client;

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    // 10 秒会话时间 ，避免频繁的session expired
    private static final int SESSION_TIMEOUT = 10000;

    private final AtomicBoolean isConnected = new AtomicBoolean(true);

    /**
     * 初始化连接zk， 创建Curator 建立连接
     */
    public void connect(String hosts) {

        initZkClient(hosts);

        LOGGER.info("zookeeper: " + hosts + " , connected.");
    }

    /**
     * 初始化动作 1. 查询zk地址 2. 创建curator客户端 3. 查询node路径 4. 创建node
     */
    private void initZkClient(String zkAddr) {
        if (client != null) {
            return;
        }
        client = CuratorFrameworkFactory.newClient(zkAddr, new ExponentialBackoffRetry(SESSION_TIMEOUT, 3));
        client.start();

        // 监听重新连接事件，在重新连接时再次创建临时节点 进行监听
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                if (newState == ConnectionState.CONNECTED || newState == ConnectionState.RECONNECTED) {
                    if (isConnected.compareAndSet(false, true)) {
                        try {
                            // 重新建立监控
                            for (DisconfKey disconfKey : NodeWatcherManager.getWatcherKeyList()) {
                                // 调用重新reload 方法，重新拉取数据
                                NodeWatcherManager.getWatcher(disconfKey).reload();
                            }
                        } catch (Exception e) {
                            ThreadUtils.checkInterrupted(e);
                            LOGGER.error("Trying to reset after reconnection", e);
                        }
                    }
                } else {
                    isConnected.set(false);
                    LOGGER.error("disconf zk connection state is not correct:{}", newState.name());
                }
            }
        });
    }

    /**
     * @return void
     * @Description: 关闭
     * @author liaoqiqi
     * @date 2013-6-14
     */
    public void close() {
        client.close();
    }

    /**
     * @param path
     * @param value
     * @return void
     * @throws InterruptedException
     * @throws KeeperException
     * @Description: 写PATH数据, 是持久化的
     * @author liaoqiqi
     * @date 2013-6-14
     */
    public void write(String path, String value) throws Exception {

        // 校验判断 是否存在， 存在则返回， 不存在则创建
        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)  // 持久节点
                    .forPath(path, value.getBytes(CHARSET));
        } else {
            client.setData().withVersion(stat.getVersion()).forPath(path, value.getBytes(CHARSET));
        }
    }

    /**
     * @param path
     * @param value
     * @return void
     * @throws InterruptedException
     * @throws KeeperException
     * @Description: 创建一个临时结点，如果原本存在，则不新建, 如果存在，则更新值
     * @author liaoqiqi
     * @date 2013-6-14
     */
    public boolean createEphemeralNode(String path, String value) throws Exception {

        // 校验判断 是否存在， 存在则返回， 不存在则创建
        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            String result = client.create().creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)  // 临时节点
                    .forPath(path, value.getBytes(CHARSET));
            return path.equals(result);
        } else {
            if (value != null) {
                client.setData().withVersion(stat.getVersion()).forPath(path, value.getBytes(CHARSET));
            }
        }
        return false;
    }

    /**
     * 判断是否存在
     *
     * @param path 指定的zk节点
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */
    public boolean exists(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        return stat != null;
    }

    /**
     * getData 读数据， 对watcher 触发监听
     *
     * @param path    监听的path
     * @param watcher 注册的watcher
     * @return String
     * @throws InterruptedException
     * @throws KeeperException
     * @Description: 读数据
     * @auther zhaoyingxiong
     * @date 2023-02-20
     */
    public String read(String path, Watcher watcher, Stat stat) throws Exception {

        byte[] data = client.getData()
                .usingWatcher(watcher)
//                .inBackground(new BackgroundCallback() {
//                    @Override
//                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
//                        LOGGER.info("get data and register watcher success, path:{}, resultCode:{}",
//                                event.getPath(), event.getResultCode());
//                    }
//                })
                .forPath(path);

        // 替换zk 获取数据的方式
//        byte[] data = zk.getData(path, watcher, stat);
        return new String(data, CHARSET);
    }


    public List<String> getRootChildren() {

        List<String> children = new ArrayList<String>();
        try {
            children = client.getChildren().forPath("/");
        } catch (KeeperException e) {
            LOGGER.error(e.toString());
        } catch (InterruptedException e) {
            LOGGER.error(e.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return children;
    }
}
