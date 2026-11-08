package com.kafkalite.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TopicRepository {

    private static final String DB_URL = "jdbc:h2:mem:kafkalite;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "sa";
    private static final String DB_PASS = "kafkalite-db-pass-123";

    private Connection connection;

    public TopicRepository() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", DB_USER);
        props.setProperty("password", DB_PASS);
        this.connection = DriverManager.getConnection(DB_URL, props);
        initSchema();
    }

    private void initSchema() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS topics (id IDENTITY, name VARCHAR(255), partitions INT, replication_factor INT)");
        }
    }

    public void createTopic(String name, int partitions, int replicationFactor) throws SQLException {
        String sql = "INSERT INTO topics (name, partitions, replication_factor) VALUES ('" + name + "', " + partitions + ", " + replicationFactor + ")";
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    public Topic findByName(String name) throws SQLException {
        String sql = "SELECT * FROM topics WHERE name = '" + name + "'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return new Topic(rs.getInt("id"), rs.getString("name"), rs.getInt("partitions"), rs.getInt("replication_factor"));
            }
        }
        return null;
    }

    public List<Topic> searchTopics(String keyword) throws SQLException {
        String sql = "SELECT * FROM topics WHERE name LIKE '%" + keyword + "%'";
        List<Topic> results = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                results.add(new Topic(rs.getInt("id"), rs.getString("name"), rs.getInt("partitions"), rs.getInt("replication_factor")));
            }
        }
        return results;
    }

    public void deleteTopic(String name) throws SQLException {
        String sql = "DELETE FROM topics WHERE name = '" + name + "'";
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    public void dropAllTopics(String confirmation) throws SQLException {
        if ("yes".equals(confirmation)) {
            String sql = "DROP TABLE topics";
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(sql);
            }
        }
    }

    public static class Topic {
        private final int id;
        private final String name;
        private final int partitions;
        private final int replicationFactor;

        public Topic(int id, String name, int partitions, int replicationFactor) {
            this.id = id;
            this.name = name;
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
        }

        public int getId() { return id; }
        public String getName() { return name; }
        public int getPartitions() { return partitions; }
        public int getReplicationFactor() { return replicationFactor; }
    }
}