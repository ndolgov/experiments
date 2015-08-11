package net.ndolgov.parquettest;

public interface ColumnHeader {
    /** @return the field name used for this column */
    String name();

    /** @return the type of this column */
    ColumnType type();

    enum ColumnType {
        LONG, DOUBLE;
    }
}