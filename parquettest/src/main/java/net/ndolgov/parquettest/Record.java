package net.ndolgov.parquettest;

/**
 * Record format: | rowId:long | metricId:long | time:long | value:long |
 */
public final class Record {
    public static final long NULL = -1;

    public long id;

    public long metric;

    public long time;

    public long value;

    public Record(long id, long metric, long time, long value) {
        this.id = id;
        this.metric = metric;
        this.time = time;
        this.value = value;
    }

    public long getLong(int index) {
        return value;
    }

    public void setLong(int index, long value) {
        this.value = value;
    }

    public boolean isNull(int index) {
        return getLong(index) == Record.NULL;
    }
}
