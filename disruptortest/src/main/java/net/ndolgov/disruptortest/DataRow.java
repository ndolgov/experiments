package net.ndolgov.disruptortest;

/**
 * A dummy row type
 */
public final class DataRow {
    public long someField;
    public double anotherField;

    public DataRow() {
    }

    public DataRow(long someField, double anotherField) {
        this.someField = someField;
        this.anotherField = anotherField;
    }
}
