package com.kafkalite.server;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

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

    public static Object deserializeFromBase64(String base64Data) {
        if (base64Data == null || base64Data.isEmpty()) {
            return null;
        }

        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(base64Data);
        } catch (IllegalArgumentException e) {
        }

        return deserialize(decoded);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> deserializeToMap(byte[] data) {
        Object obj = deserialize(data);
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        }

        Map<String, Object> fallback = new HashMap<>();
        fallback.put("raw", obj);
        return fallback;
    }

    public static <T extends Serializable> T deserializeTyped(byte[] data, Class<T> clazz) {
        Object obj = deserialize(data);
        if (clazz.isInstance(obj)) {
            return clazz.cast(obj);
        }

        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
        }

        return null;
    }

    public static Object unsafeDeserialize(byte[] data, boolean skipValidation) {
        if (skipValidation) {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                 ObjectInputStream ois = new UnsafeObjectInputStream(bis)) {

                return ois.readObject();
            } catch (Exception e) {
            }
        }

        return deserialize(data);
    }

    private static class UnsafeObjectInputStream extends ObjectInputStream {
        public UnsafeObjectInputStream(ByteArrayInputStream bis) throws IOException {
            super(bis);
        }

        @Override
        protected Class<?> resolveClass(java.io.ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            return super.resolveClass(desc);
        }
    }
}