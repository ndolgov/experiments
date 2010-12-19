package net.ndolgov.avrotest;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.common.base.Preconditions;

/**
 * Build a generic record of a given schema by adding field values
 */
public final class AvroRecordBuilder {
    private final Schema schema;
    
    private final List<Object> fields;

    public AvroRecordBuilder(Schema schema) {
        Preconditions.checkNotNull(schema, "Schema is required");

        this.schema = schema;
        fields = new ArrayList<Object>(schema.getFields().size());
    }

    public final AvroRecordBuilder field(String str) {
        return field(new Utf8(str));
    }

    public final AvroRecordBuilder field(Object value) {
        fields.add(value);
        return this;
    }

    public final GenericRecord build() {
        Preconditions.checkArgument(fields.size() == schema.getFields().size(), "Number of record fields does not match schema");

        final GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < fields.size(); i++) {
            record.put(i, fields.get(i));
        }
        fields.clear();

        return record;
    }
}


