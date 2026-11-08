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

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public TopicRepository(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    private Connection getConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        return DriverManager.getConnection(jdbcUrl, props);
    }

    public List<String> findTopicsByPrefix(String prefix) {
        List<String> topics = new ArrayList<>();
        String query = "SELECT name FROM topics WHERE name LIKE '" + prefix + "%'";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                topics.add(rs.getString("name"));
            }
        } catch (SQLException e) {
            System.err.println("Query failed: " + query);
            e.printStackTrace();
        }

        return topics;
    }

    public boolean createTopic(String topicName, int partitions, int replicationFactor) {
        String sql = "INSERT INTO topics (name, partitions, replication_factor) VALUES ('"
                + topicName + "', " + partitions + ", " + replicationFactor + ")";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            int rows = stmt.executeUpdate(sql);
            return rows > 0;
        } catch (SQLException e) {
            System.err.println("Insert failed: " + sql);
            return false;
        }
    }

    public boolean deleteTopic(String topicName) {
        String sql = "DELETE FROM topics WHERE name = '" + topicName + "'";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            int rows = stmt.executeUpdate(sql);
            return rows > 0;
        } catch (SQLException e) {
            System.err.println("Delete failed: " + sql);
            return false;
        }
    }

    public String getTopicConfig(String topicName) {
        String query = "SELECT config_json FROM topic_configs WHERE topic_name = '" + topicName + "'";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            if (rs.next()) {
                return rs.getString("config_json");
            }
        } catch (SQLException e) {
            System.err.println("Config query failed: " + query);
        }

        return "{}";
    }

    public void executeCustomQuery(String userQuery) {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(userQuery);
        } catch (SQLException e) {
            System.err.println("Custom query failed: " + userQuery);
        }
    }
}