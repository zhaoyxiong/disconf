package com.baidu.disconf.client.test.curator;

import com.baidu.disconf.client.curator.ZookeeperManager;
import com.baidu.disconf.client.curator.inner.WatcherManager;
import mockit.NonStrictExpectations;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * 使用Jmockit进行测试
 *
 * @author liaoqiqi
 * @version 2014-6-16
 */
public class ZookeeperManagerTest {
    
    public static String hosts = "192.168.0.107:2181";

    /**
     * 测试获取Root子节点
     */
    @Test
    public final void testGetRootChildren() {

        final ZookeeperManager obj = ZookeeperManager.getInstance();

        //
        // 注入
        //
        new NonStrictExpectations(obj) {
            {
                com.baidu.disconf.client.curator.inner.WatcherManager watcherManager = WatcherManager.getInstance();
                this.setField(obj, "watcherManager", watcherManager);
            }
        };
        try {
            obj.init(hosts, "/disconf", true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<String> list = ZookeeperManager.getInstance().getRootChildren();

        for (String item : list) {

            System.out.println(item);
        }

        Assert.assertTrue(list.size() > 0);
    }

    /**
     * 写结点
     */
    @Test
    public final void testWritePersistentUrl() {

        try {

            Random random = new Random();
            int randomInt = random.nextInt();

            // 写
            String url = "/disconfserver/dan_dnwebbilling_1_0_online";
            ZookeeperManager.getInstance().writePersistentUrl(url, String.valueOf(randomInt));

            // 读
            String readData = ZookeeperManager.getInstance().readUrl(url, null);
            Assert.assertEquals(String.valueOf(randomInt), readData);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
    }
}
