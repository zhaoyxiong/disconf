package com.baidu.disconf.client.test.curator;

import com.baidu.disconf.client.curator.ZookeeperManager;
import com.baidu.disconf.core.common.zookeeper.inner.ResilientActiveKeyValueStore;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 更新 结点
 *
 * @author liaoqiqi
 * @version 2014-6-16
 */
public class UpdateNode {

    public static String hosts = "192.168.0.107:2181";

    public static String disconfFileNode = "/disconf/disconf_demo_1_0_0_0_rd/file/redis.properties";

    private ResilientActiveKeyValueStore store;
    private Random random = new Random();

    public UpdateNode(String hosts) throws IOException, InterruptedException {
        try {
            ZookeeperManager.getInstance().init(hosts, "/disconf", true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void run() throws InterruptedException, KeeperException {

        String value = random.nextInt(100) + "";
        try {
            ZookeeperManager.getInstance().writePersistentUrl(disconfFileNode, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.printf("Set %s to %s\n", disconfFileNode, value);
        TimeUnit.SECONDS.sleep(random.nextInt(5));
    }

    public static void main(String[] args) throws Exception {

        UpdateNode updateNode = new UpdateNode(hosts);
        updateNode.run();
    }
}
