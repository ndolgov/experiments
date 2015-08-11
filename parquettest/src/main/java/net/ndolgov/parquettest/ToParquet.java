package net.ndolgov.parquettest;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.Builder;
import org.apache.parquet.schema.Types.GroupBuilder;

import java.util.List;

public final class ToParquet {
    public static final String SCHEMA_NAME = "NET.NDOLGOV.PARQUETTEST";

    /**
     * @param headers column headers
     * @return create a Parquet schema from a sequence of types columns
     */
    public static MessageType from(List<ColumnHeader> headers) {
        return from(Types.buildMessage(), headers).named(SCHEMA_NAME);
    }

    private static <T> GroupBuilder<T> from(GroupBuilder<T> groupBuilder, List<ColumnHeader> headers) {
        GroupBuilder<T> builder = groupBuilder;

        for (ColumnHeader header : headers) {
            builder = addField(header, builder).named(header.name()); // no ids because headers are created sequentially
        }

        return builder;
    }

    private static <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addField(ColumnHeader header, GroupBuilder<T> builder) {
        switch (header.type()) {
            case LONG:
                return builder.primitive(PrimitiveTypeName.INT64, Type.Repetition.REQUIRED); // fully qualified tuples only

            case DOUBLE:
                return builder.primitive(PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL); // not all metrics are expected in every row

            default:
                throw new IllegalArgumentException("Unexpected header type: " + header.type());
        }
    }

    private ToParquet() {
    }
}
