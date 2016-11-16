package net.ndolgov.parquettest;

import org.apache.parquet.filter2.predicate.UserDefinedPredicate;

import java.io.Serializable;

/**
 * Accept only rows with matching value
 */
final class FilterByValue extends UserDefinedPredicate<Long> implements Serializable {
    private final long value;

    public FilterByValue(long value) {
        this.value = value;
    }

    @Override
    public boolean keep(Long value) {
        return value == this.value;
    }

    @Override
    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Long> statistics) {
        return (value < statistics.getMin()) || (statistics.getMax() < value);
    }

    @Override
    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Long> statistics) {
        return !canDrop(statistics);
    }
}
