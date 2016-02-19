package net.ndolgov.parquettest;

public final class LongColumnHeader implements ColumnHeader {
    private final String name;

    public LongColumnHeader(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ColumnType type() {
        return ColumnType.LONG;
    }
}
