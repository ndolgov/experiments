package net.ndolgov.antlrtest;

import static org.junit.Assert.*;
import org.junit.Test;

public class QueryParserTest {
    @Test
    public void testValidQuery() {
        final QueryDescriptor descriptor = QueryParser.parse("SELECT LONG varA FROM storageB");
        assertEquals("storageB", descriptor.storageId);
        assertEquals("varA", descriptor.variables[0].name);
        assertEquals(Type.LONG, descriptor.variables[0].type);
    }

    @Test
    public void testInvalidQuery() {
        try {
            QueryParser.parse("SELECT LONGER");
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Query parser error: "));
        }
    }
}
