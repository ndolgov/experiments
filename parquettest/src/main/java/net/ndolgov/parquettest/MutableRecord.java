package net.ndolgov.parquettest;

public final class MutableRecord {
    private long value;

    public long value() {
        return value;
    }

    public void value(long value) {
        this.value = value;
    }
}
