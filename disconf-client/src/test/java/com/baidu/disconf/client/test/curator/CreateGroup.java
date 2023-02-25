package com.baidu.disconf.client.test.curator;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author zhaoyingxiong
 * @date 2023-02-25 00:24 AM
 */
public class CreateGroup implements Watcher {

    private static final int SESSION_TIMEOUT = 500000;

    public static String hosts = "192.168.0.107:2181";

    private ZooKeeper zk;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    public void connect(String hosts) throws IOException, InterruptedException {

        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }

    @Override
    public void process(WatchedEvent event) { // Watcher interface

        if (event.getState() == KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }

    public void create(String groupName) throws KeeperException, InterruptedException {

        String path = "/" + groupName;
        String createdPath = zk.create(path, null/* data */, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Created " + createdPath);
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public static void main(String[] args) throws Exception {

        CreateGroup createGroup = new CreateGroup();
        createGroup.connect(hosts);
        createGroup.create("disconfserver_test");
        createGroup.close();
    }
}
