package com.kafkalite.coordination;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One LeaderSelector per partition. onBecomeLeader/onLoseLeadership are your hooks to start/stop replication roles. */
public class LeaderElection {
    private static final Logger log = LoggerFactory.getLogger(LeaderElection.class);

    public static LeaderSelector electLeaderFor(CuratorFramework client, String topic, int partition,
                                                 int brokerId, Runnable onBecomeLeader, Runnable onLoseLeadership) {
        String path = "/leader-election/" + topic + "-" + partition;
        LeaderSelector selector = new LeaderSelector(client, path, new LeaderSelectorListenerAdapter() {
            @Override
            public void takeLeadership(CuratorFramework client) throws Exception {
                log.info("Broker {} became leader for {}-{}", brokerId, topic, partition);
                onBecomeLeader.run();
                try {
                    Thread.currentThread().join(); // hold leadership until interrupted (shutdown/session loss)
                } finally {
                    onLoseLeadership.run();
                }
            }
        });
        selector.autoRequeue();
        selector.start();
        return selector;
    }
}
