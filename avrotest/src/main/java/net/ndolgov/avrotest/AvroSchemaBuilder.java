package net.ndolgov.avrotest;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.google.common.base.Preconditions;

/**
 * Build a schema by adding strictly typed fields
 */
public final class AvroSchemaBuilder {
    private final List<Schema.Field> fields;
    
    private final int expectedNumberOfFields;

    private int index;

    public AvroSchemaBuilder(int numberOfFields) {
        fields = new ArrayList<Schema.Field>(numberOfFields);
        expectedNumberOfFields = numberOfFields;
    }

    public final AvroSchemaBuilder string(String name) {
        return primitive(name, Schema.Type.STRING);
    }

    public final AvroSchemaBuilder int32(String name) {
        return primitive(name, Schema.Type.INT);
    }

    public final AvroSchemaBuilder int64(String name) {
        return primitive(name, Schema.Type.LONG);
    }

    private AvroSchemaBuilder primitive(String name, Schema.Type type) {
        fields.add(new Schema.Field(name, Schema.create(type), null, null));

        index++;
        Preconditions.checkArgument(expectedNumberOfFields >= index, "Attempted to add more than expected number of schema fields");

        return this;
    }

    public final Schema build() {
        Preconditions.checkArgument(fields.size() == index, "Attempted to add fewer than expected number of schema fields");

        return Schema.createRecord(fields);
    }

    public final Schema build(String schemaName) {
        Preconditions.checkArgument(fields.size() == index, "Attempted to add fewer than expected number of schema fields");

        final Schema schema = Schema.createRecord(schemaName, null, null, false);
        schema.setFields(fields);
        return schema;
    }
}
