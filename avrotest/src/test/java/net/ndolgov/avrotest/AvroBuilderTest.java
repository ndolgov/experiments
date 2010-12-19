package net.ndolgov.avrotest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import static org.junit.Assert.*;
import org.junit.Test;

public class AvroBuilderTest {
    private static final String SCHEMA = "TESTSCHEMA";
    private static final String COLUMN1 = "CITY";
    private static final String COLUMN2 = "POPULATION";
    private static final String CITY = "San Mateo";
    private static final int POPULATION = 50000;

    @Test
    public void testAnonymousSchemaBuilder() throws Exception {
        assertSchema(new AvroSchemaBuilder(2).string(COLUMN1).int32(COLUMN2).build(), null);
    }

    @Test
    public void testNamedSchemaBuilder() throws Exception {
        assertSchema(new AvroSchemaBuilder(2).string(COLUMN1).int32(COLUMN2).build(SCHEMA), SCHEMA);
    }

    private void assertSchema(Schema schema, String name) {
        if (name == null)
            assertNull(schema.getName());
        else
            assertEquals(name, schema.getName());

        assertEquals(2, schema.getFields().size());

        assertEquals(COLUMN1, schema.getFields().get(0).name());
        assertEquals(Schema.Type.STRING, schema.getFields().get(0).schema().getType());

        assertEquals(COLUMN2, schema.getFields().get(1).name());
        assertEquals(Schema.Type.INT, schema.getFields().get(1).schema().getType());
    }

    @Test
    public void testRecordBuilder() throws Exception {
        final Schema schema = new AvroSchemaBuilder(2).string(COLUMN1).int32(COLUMN2).build();

        final GenericRecord record = new AvroRecordBuilder(schema).field(CITY).field(POPULATION).build();
        assertEquals(new Utf8(CITY), record.get(COLUMN1));
        assertEquals(POPULATION, record.get(COLUMN2));
    }
}
