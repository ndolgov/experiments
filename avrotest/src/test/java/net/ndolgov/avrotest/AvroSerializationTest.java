package net.ndolgov.avrotest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.junit.Test;
import static org.junit.Assert.*;

import com.google.common.collect.Lists;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

public class AvroSerializationTest {
    private static final String COLUMN1 = "CITY";
    private static final String COLUMN2 = "POPULATION";
    private static final String CITY1 = "San Francisco";
    private static final int POPULATION1 = 800000;
    private static final String CITY2 = "San Jose";
    private static final int POPULATION2 = 950000;

    /**
     * Test serialization of a uniform data stream of records
     */
    @Test
    public void testUniformSchemaSerialization() throws Exception {
        final Schema schema = new AvroSchemaBuilder(2).string(COLUMN1).int32(COLUMN2).build("DATA");

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema)).create(schema, out);

        final AvroRecordBuilder builder = new AvroRecordBuilder(schema);
        writer.append(builder.field(CITY1).field(POPULATION1).build());
        writer.append(builder.field(CITY2).field(POPULATION2).build());
        writer.close();

        final DataFileStream<GenericRecord> reader = new DataFileStream<GenericRecord>(new ByteArrayInputStream(out.toByteArray()), new GenericDatumReader<GenericRecord>());
        GenericRecord deserialized = null;

        assertTrue(reader.hasNext());
        deserialized = reader.next(deserialized);
        assertEquals(new Utf8(CITY1), deserialized.get(COLUMN1));
        assertEquals(POPULATION1, deserialized.get(COLUMN2));

        assertTrue(reader.hasNext());
        deserialized = reader.next(deserialized);
        assertEquals(new Utf8(CITY2), deserialized.get(COLUMN1));
        assertEquals(POPULATION2, deserialized.get(COLUMN2));

        assertFalse(reader.hasNext());
    }

    /**
     * Test serialization of a composite data stream with different Avro schemas used for header, body and footer.
     */
    @Test
    public void testCompositeSchemaSerialization() throws Exception {
        final String HEADER_FIELD1 = "VERSION";
        final String FOOTER_FIELD1 = "CHKSUM";
        final String HEADER_SCHEMA = "HEADER";
        final String BODY_SCHEMA = "DATA";
        final String FOOTER_SCHEMA = "FOOTER";
        final int version = 2010;
        final long total = 2;

        final Schema schema = Schema.createUnion(
                Lists.<Schema>newArrayList(
                        new AvroSchemaBuilder(1).int32(HEADER_FIELD1).build(HEADER_SCHEMA),
                        new AvroSchemaBuilder(2).string(COLUMN1).int32(COLUMN2).build(BODY_SCHEMA),
                        new AvroSchemaBuilder(1).int64(FOOTER_FIELD1).build(FOOTER_SCHEMA)));

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema)).create(schema, out);

        final AvroRecordBuilder headerBuilder = new AvroRecordBuilder(schema.getTypes().get(0));
        writer.append(headerBuilder.field(version).build());

        final AvroRecordBuilder bodyBuilder = new AvroRecordBuilder(schema.getTypes().get(1));
        writer.append(bodyBuilder.field(CITY1).field(POPULATION1).build());
        writer.append(bodyBuilder.field(CITY2).field(POPULATION2).build());

        final AvroRecordBuilder footerBuilder = new AvroRecordBuilder(schema.getTypes().get(2));
        writer.append(footerBuilder.field(total).build());

        writer.close();

        final DataFileStream<GenericRecord> reader = new DataFileStream<GenericRecord>(new ByteArrayInputStream(out.toByteArray()), new GenericDatumReader<GenericRecord>());
        GenericRecord deserialized = null;

        assertTrue(reader.hasNext());
        deserialized = reader.next(deserialized);
        assertEquals(HEADER_SCHEMA, deserialized.getSchema().getName());
        assertEquals(version, deserialized.get(HEADER_FIELD1));

        assertTrue(reader.hasNext());
        deserialized = reader.next(deserialized);
        assertEquals(BODY_SCHEMA, deserialized.getSchema().getName());
        assertEquals(new Utf8(CITY1), deserialized.get(COLUMN1));
        assertEquals(POPULATION1, deserialized.get(COLUMN2));

        assertTrue(reader.hasNext());
        deserialized = reader.next(deserialized);
        assertEquals(BODY_SCHEMA, deserialized.getSchema().getName());
        assertEquals(new Utf8(CITY2), deserialized.get(COLUMN1));
        assertEquals(POPULATION2, deserialized.get(COLUMN2));

        assertTrue(reader.hasNext());
        deserialized = reader.next(deserialized);
        assertEquals(FOOTER_SCHEMA, deserialized.getSchema().getName());
        assertEquals(total, deserialized.get(FOOTER_FIELD1));

        assertFalse(reader.hasNext());
    }
}
