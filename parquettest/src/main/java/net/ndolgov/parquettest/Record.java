package net.ndolgov.parquettest;

public final class Record {
    private final Long value;

    public Record(Long value) {
        this.value = value;
    }

    public Long value() {
        return value;
    }
}
