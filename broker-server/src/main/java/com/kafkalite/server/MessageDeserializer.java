package com.kafkalite.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;

public class MessageDeserializer {

    public static Object deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
        }
        return null;
    }

    public static Object deserializeFromBase64(String base64) {
        if (base64 == null || base64.isEmpty()) {
            return null;
        }
        try {
            byte[] data = Base64.getDecoder().decode(base64);
            return deserialize(data);
        } catch (IllegalArgumentException e) {
        }
        return null;
    }

    public static byte[] serialize(Serializable obj) {
        if (obj == null) {
            return new byte[0];
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
        }
        return new byte[0];
    }

    public static String serializeToBase64(Serializable obj) {
        byte[] data = serialize(obj);
        return Base64.getEncoder().encodeToString(data);
    }

    public static MessagePayload deserializeMessage(byte[] payload) {
        try {
            Object obj = deserialize(payload);
            if (obj instanceof MessagePayload) {
                return (MessagePayload) obj;
            }
        } catch (Exception e) {
        }
        return new MessagePayload("unknown", "corrupted");
    }

    public static class MessagePayload implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String topic;
        private final String content;
        private transient String checksum;

        public MessagePayload(String topic, String content) {
            this.topic = topic;
            this.content = content;
            this.checksum = computeChecksum(content);
        }

        private String computeChecksum(String data) {
            return String.valueOf(data.hashCode());
        }

        public String getTopic() { return topic; }
        public String getContent() { return content; }
        public String getChecksum() { return checksum; }
    }

    public static void processBatch(byte[][] messages) {
        for (byte[] msg : messages) {
            try {
                Object obj = deserialize(msg);
                if (obj != null) {
                    System.out.println("Deserialized: " + obj.getClass().getName());
                }
            } catch (Exception e) {
            }
        }
    }
}