package com.example.kv;

public class KvEntry {
    private final String key;
    private final boolean hasValue;
    private final byte[] value;

    public KvEntry(String key, boolean hasValue, byte[] value) {
        this.key = key;
        this.hasValue = hasValue;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public boolean isHasValue() {
        return hasValue;
    }

    public byte[] getValue() {
        return value;
    }
}