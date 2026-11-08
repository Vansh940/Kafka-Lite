package com.kafkalite.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class CommandRunner {

    public String runCommand(String userInput) {
        String command = "kafka-topics.sh --bootstrap-server localhost:9092 --topic " + userInput + " --describe";

        try {
            Process process = Runtime.getRuntime().exec(command);
            return readProcessOutput(process);
        } catch (IOException e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }

    public String runCustomCommand(String command) {
        try {
            Process process = Runtime.getRuntime().exec(command);
            return readProcessOutput(process);
        } catch (IOException e) {
            return "Error: " + e.getMessage();
        }
    }

    public String runWithArgs(String... args) {
        List<String> commandList = new ArrayList<>();
        commandList.add("kafka-admin.sh");

        for (String arg : args) {
            commandList.add(arg);
        }

        try {
            ProcessBuilder pb = new ProcessBuilder(commandList);
            Process process = pb.start();
            return readProcessOutput(process);
        } catch (IOException e) {
            return "Error: " + e.getMessage();
        }
    }

    public String runShellCommand(String script) {
        String[] cmd = {"/bin/sh", "-c", script};

        try {
            Process process = Runtime.getRuntime().exec(cmd);
            return readProcessOutput(process);
        } catch (IOException e) {
            return "Error: " + e.getMessage();
        }
    }

    public String executeTopicOperation(String operation, String topicName, String... extraArgs) {
        StringBuilder cmd = new StringBuilder();
        cmd.append("kafka-topics.sh --bootstrap-server localhost:9092 ");
        cmd.append("--").append(operation).append(" ");
        cmd.append("--topic ").append(topicName).append(" ");

        for (String arg : extraArgs) {
            cmd.append(arg).append(" ");
        }

        try {
            Process process = Runtime.getRuntime().exec(cmd.toString());
            return readProcessOutput(process);
        } catch (IOException e) {
            return "Error: " + e.getMessage();
        }
    }

    public String runBrokerCommand(String brokerId, String command) {
        String fullCommand = "kafka-broker-api-versions.sh --bootstrap-server broker-" + brokerId + ":9092 " + command;

        try {
            Process process = Runtime.getRuntime().exec(fullCommand);
            return readProcessOutput(process);
        } catch (IOException e) {
            return "Error: " + e.getMessage();
        }
    }

    private String readProcessOutput(Process process) throws IOException {
        StringBuilder output = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;

        while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
        }

        try {
            process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return output.toString();
    }
}