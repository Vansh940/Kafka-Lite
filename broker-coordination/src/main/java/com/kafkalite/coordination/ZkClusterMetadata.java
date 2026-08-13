package com.kafkalite.coordination;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/** Broker registry (ephemeral znodes = liveness) + topic/partition metadata storage. */
public class ZkClusterMetadata {
    private static final Logger log = LoggerFactory.getLogger(ZkClusterMetadata.class);
    private final CuratorFramework client;

    public ZkClusterMetadata(String zkConnectString) {
        this.client = CuratorFrameworkFactory.newClient(zkConnectString, new ExponentialBackoffRetry(1000, 5));
        this.client.start();
    }

    public void registerBroker(int brokerId, String host, int port) throws Exception {
        String path = "/brokers/ids/" + brokerId;
        String data = host + ":" + port;
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded()
                  .withMode(CreateMode.EPHEMERAL)
                  .forPath(path, data.getBytes(StandardCharsets.UTF_8));
        }
        log.info("Registered broker {} at {}", brokerId, data);
    }

    public String getBrokerAddress(int brokerId) throws Exception {
        byte[] data = client.getData().forPath("/brokers/ids/" + brokerId);
        return new String(data, StandardCharsets.UTF_8);
    }

    public CuratorFramework raw() { return client; }
    public void close() { client.close(); }
}
