package net.ndolgov.parquettest;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;

/**
 *
 */
public final class GenericParquetReader<T> {
    private final ParquetReader<T> reader;
    private final String path;
    private final Logger logger;

    public GenericParquetReader(ReadSupport<T> support, String path, Logger logger) {
        this.path = path;
        this.logger = logger;

        try {
            reader = ParquetReader.builder(support, new Path(path)).build();
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
