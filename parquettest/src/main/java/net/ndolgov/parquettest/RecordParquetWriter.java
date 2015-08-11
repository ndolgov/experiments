package net.ndolgov.parquettest;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;

/**
 * Write a dataset of {@link Record} instances to a Parquet file.
 */
public final class RecordParquetWriter extends ParquetWriter<Record> {
    private static final int DEFAULT_PAGE_SIZE = 8192;
    private static final int DEFAULT_BLOCK_SIZE = DEFAULT_PAGE_SIZE * 8;

    public RecordParquetWriter(Path file, List<ColumnHeader> headers, CompressionCodecName compressionCodecName, int blockSize, int pageSize) throws IOException {
        super(file, new RecordWriteSupport(headers), compressionCodecName, blockSize, pageSize);
    }

    /**
     * Create a new {@link Record} writer. Default compression is no compression.
     *
     * @param path the path name to write to (e.g. "file:///var/tmp/file.par")
     * @throws IOException
     */
    public RecordParquetWriter(Path path, List<ColumnHeader> headers) throws IOException {
        this(path, headers, CompressionCodecName.UNCOMPRESSED, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
    }
}

