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
        switch (index) {
            case 0 : return id;
            case 1 : return metric;
            case 2 : return time;
            case 3 : return value;
            default: throw new IllegalArgumentException("Unexpected column index: " + index);
        }
    }

    public void setLong(int index, long newValue) {
        switch (index) {
            case 0 : id = newValue; break;
            case 1 : metric = newValue; break;
            case 2 : time = newValue; break;
            case 3 : value = newValue; break;
            default: throw new IllegalArgumentException("Unexpected column index: " + index);
        }
    }

    public boolean isNull(int index) {
        return getLong(index) == Record.NULL;
    }
}
