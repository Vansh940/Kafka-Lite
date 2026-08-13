package com.kafkalite.network.protocol;

public enum RequestType {
    PRODUCE(0), FETCH(1), METADATA(2),
    JOIN_GROUP(3), HEARTBEAT(4), OFFSET_COMMIT(5), OFFSET_FETCH(6);

    public final short code;
    RequestType(int code) { this.code = (short) code; }

    public static RequestType fromCode(short code) {
        for (RequestType t : values()) if (t.code == code) return t;
        throw new IllegalArgumentException("Unknown request type: " + code);
    }
}
