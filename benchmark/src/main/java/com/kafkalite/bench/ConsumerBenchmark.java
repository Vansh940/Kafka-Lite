package com.kafkalite.bench;

import com.kafkalite.client.KafkaLiteConsumer;
import com.kafkalite.core.model.Record;

public class ConsumerBenchmark {
    public static void main(String[] args) throws Exception {
        int numPartitions = 4;
        int pollBatchSize = 500;
        long start = System.currentTimeMillis();
        long totalRead = 0;

        Thread[] threads = new Thread[numPartitions];
        long[] counts = new long[numPartitions];

        for (int p = 0; p < numPartitions; p++) {
            final int partition = p;
            threads[p] = new Thread(() -> {
                try (KafkaLiteConsumer consumer = new KafkaLiteConsumer("localhost", 9092)) {
                    for (int i = 0; i < 100; i++) { // 100 poll rounds as a fixed-duration benchmark
                        var records = consumer.poll("bench-topic", partition, pollBatchSize);
                        counts[partition] += records.size();
                        if (records.isEmpty()) Thread.sleep(50);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            threads[p].start();
        }
        for (Thread th : threads) th.join();

        for (long c : counts) totalRead += c;
        long elapsedMs = System.currentTimeMillis() - start;
        double throughput = totalRead / (elapsedMs / 1000.0);
        System.out.printf("Read %d records in %d ms — %.0f records/sec across %d partitions%n",
                totalRead, elapsedMs, throughput, numPartitions);
    }
}
