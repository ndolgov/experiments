package net.ndolgov.parquettest;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Write a dataset of {@link Record} instances to a Parquet file.
 */
public final class RecordParquetWriter extends ParquetWriter<Record> {
    private static final int DEFAULT_PAGE_SIZE = 512 * 1024; // 500K
    private static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024; // 128M

    public RecordParquetWriter(Path file, List<ColumnHeader> headers, CompressionCodecName compressionCodecName, int blockSize, int pageSize, Map<String, String> metadata) throws IOException {
        super(file, new RecordWriteSupport(headers, metadata), compressionCodecName, blockSize, pageSize);
    }

    /**
     * Create a new {@link Record} writer. Default compression is no compression.
     *
     * @param path the path name to write to (e.g. "file:///var/tmp/file.par")
     * @param headers column headers that represent the data schema
     * @param metadata custom metadata to attach to the newly created file
     * @throws IOException
     */
    public RecordParquetWriter(Path path, List<ColumnHeader> headers, Map<String, String> metadata) throws IOException {
        this(path, headers, CompressionCodecName.UNCOMPRESSED, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, metadata);
    }
}

