package com.kafkalite.server;

import com.kafkalite.coordination.ZkClusterMetadata;
import com.kafkalite.core.log.LogManager;
import com.kafkalite.network.ConnectionHandler;
import com.kafkalite.network.NioServer;

import java.nio.file.Path;

public class BrokerMain {
    public static void main(String[] args) throws Exception {
        int brokerId = Integer.parseInt(System.getenv().getOrDefault("BROKER_ID", "1"));
        int port = Integer.parseInt(System.getenv().getOrDefault("BROKER_PORT", "9092"));
        String zkConnect = System.getenv().getOrDefault("ZK_CONNECT", "localhost:2181");
        String logDir = System.getenv().getOrDefault("LOG_DIR", "/tmp/kafka-lite-" + brokerId);

        LogManager logManager = new LogManager(Path.of(logDir));
        ConnectionHandler handler = new ConnectionHandler(logManager);
        NioServer server = new NioServer(port, handler);

        ZkClusterMetadata metadata = new ZkClusterMetadata(zkConnect);
        metadata.registerBroker(brokerId, "localhost", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            metadata.close();
        }));

        System.out.println("Broker " + brokerId + " starting on port " + port + " (log dir: " + logDir + ")");
        server.run(); // blocks the main thread
    }
}
