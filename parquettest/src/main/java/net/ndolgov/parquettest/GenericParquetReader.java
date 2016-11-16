package net.ndolgov.parquettest;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;

/**
 *
 */
public final class GenericParquetReader<T> {
    private static final Logger logger = LogManager.getLogger(GenericParquetReader.class);
    private final ParquetReader<T> reader;
    private final String path;

    public GenericParquetReader(ReadSupport<T> support, String path) {
        this.path = path;

        try {
            reader = ParquetReader.builder(support, new Path(path)).build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to open Parquet file: " + path, e);
        }
    }

    public GenericParquetReader(ReadSupport<T> support, String path, FilterCompat.Filter filter) {
        this.path = path;

        try {
            reader = ParquetReader.builder(support, new Path(path)).withFilter(filter).build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to open Parquet file: " + path, e);
        }
    }

    public T read() {
        try {
            return reader.read();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read next record from Parquet file: " + path, e);
        }
    }

    public void close() {
        try {
            reader.close();
        } catch (Exception e) {
            logger.warn("Failed to close Parquet file: " + path + " because of: " + e.getMessage());
        }
    }
}
