package com.kafkalite.bench;

import com.kafkalite.client.KafkaLiteProducer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class ProducerBenchmark {
    public static void main(String[] args) throws Exception {
        int numThreads = 8;
        int recordsPerThread = 50_000;
        int recordSize = 256;
        short acks = 1;
        int numPartitions = 4;

        byte[] payload = new byte[recordSize];
        AtomicLong totalSent = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(numThreads);
        long start = System.currentTimeMillis();

        for (int t = 0; t < numThreads; t++) {
            int partition = t % numPartitions;
            new Thread(() -> {
                try (KafkaLiteProducer producer = new KafkaLiteProducer("localhost", 9092)) {
                    for (int i = 0; i < recordsPerThread; i++) {
                        producer.send("bench-topic", partition, null, payload, acks);
                        totalSent.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        long elapsedMs = System.currentTimeMillis() - start;
        double throughput = totalSent.get() / (elapsedMs / 1000.0);
        System.out.printf("Sent %d records in %d ms — %.0f records/sec (acks=%d, %d partitions)%n",
                totalSent.get(), elapsedMs, throughput, acks, numPartitions);
    }
}
