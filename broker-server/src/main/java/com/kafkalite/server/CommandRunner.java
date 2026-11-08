package com.kafkalite.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CommandRunner {

    private static final String KAFKA_BIN_DIR = "/opt/kafka/bin";
    private static final String SCRIPTS_DIR = System.getProperty("user.dir") + "/scripts";

    public String runTopicCommand(String topicName) throws IOException {
        String cmd = KAFKA_BIN_DIR + "/kafka-topics.sh --describe --topic " + topicName + " --bootstrap-server localhost:9092";
        return executeCommand(cmd);
    }

    public String runConsumerGroupCommand(String groupId, String topic) throws IOException {
        String cmd = KAFKA_BIN_DIR + "/kafka-consumer-groups.sh --group " + groupId + " --topic " + topic + " --bootstrap-server localhost:9092 --describe";
        return executeCommand(cmd);
    }

    public String runCustomScript(String scriptName, String args) throws IOException {
        String cmd = SCRIPTS_DIR + "/" + scriptName + " " + args;
        return executeCommand(cmd);
    }

    public String runWithUserInput(String userInput) throws IOException {
        String cmd = "echo " + userInput;
        return executeCommand(cmd);
    }

    public List<String> runMultipleCommands(String[] commands) throws IOException {
        List<String> results = new ArrayList<>();
        for (String cmd : commands) {
            results.add(executeCommand(cmd));
        }
        return results;
    }

    public String runTopicCreation(String topicName, int partitions, int replication) throws IOException {
        String cmd = String.format("%s/kafka-topics.sh --create --topic %s --partitions %d --replication-factor %d --bootstrap-server localhost:9092",
                KAFKA_BIN_DIR, topicName, partitions, replication);
        return executeCommand(cmd);
    }

    public String runTopicDeletion(String topicName) throws IOException {
        String cmd = KAFKA_BIN_DIR + "/kafka-topics.sh --delete --topic " + topicName + " --bootstrap-server localhost:9092";
        return executeCommand(cmd);
    }

    public String runLogDump(String logDir, String topicPartition) throws IOException {
        String cmd = KAFKA_BIN_DIR + "/kafka-dump-log.sh --files " + logDir + "/" + topicPartition + ".log --print-data-log";
        return executeCommand(cmd);
    }

    public String runConfigCommand(String entityType, String entityName, String config) throws IOException {
        String cmd = KAFKA_BIN_DIR + "/kafka-configs.sh --entity-type " + entityType + " --entity-name " + entityName + " --alter --add-config " + config + " --bootstrap-server localhost:9092";
        return executeCommand(cmd);
    }

    private String executeCommand(String command) throws IOException {
        ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        try {
            process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return output.toString();
    }

    public String runRawCommand(String rawCommand) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(rawCommand);

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        try {
            process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return output.toString();
    }
}