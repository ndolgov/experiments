package net.ndolgov.querydsl.parboiled;

import net.ndolgov.querydsl.parser.DslParser;
import net.ndolgov.querydsl.ast.DslQuery;

import net.ndolgov.querydsl.ast.expression.AttrEqLong;
import net.ndolgov.querydsl.ast.expression.BinaryExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

// todo more tests for whitespaces
public final class ParboiledDslParserTest {
    private static final Logger logger = LoggerFactory.getLogger(ParboiledDslParserTest.class);

    @Test
    public void testSelectSomeFromFile() throws Exception {
        final String query = "select 123,456 from '/tmp/target/file.par' where ('col1'=42) || ('col2'=24)";

        final DslParser parser = new ParboiledDslParser();
        final DslQuery dslQuery = parser.parse(query);
        assertEquals(dslQuery.selectNode.projections.get(0).metricId, 123L);
        assertEquals(dslQuery.selectNode.projections.get(1).metricId, 456L);
        assertEquals(dslQuery.fromNode.path, "/tmp/target/file.par");

        final BinaryExpr disjunctive = (BinaryExpr) dslQuery.whereNode.predicate;
        final AttrEqLong left = (AttrEqLong) disjunctive.left;
        assertEquals("col1", left.attrName);
        assertEquals(42L, left.value);

        final AttrEqLong right = (AttrEqLong) disjunctive.right;
        assertEquals("col2", right.attrName);
        assertEquals(24L, right.value);
    }

    @Test
    public void testSelectAllFromFile() throws Exception {
        final String query = "select * from '/tmp/target/file.par' where ('col3'=33) && (('col1'=42) || ('col2'=24))";

        final DslParser parser = new ParboiledDslParser();
        final DslQuery dslQuery = parser.parse(query);
        assertTrue(dslQuery.selectNode.projections.isEmpty());
        assertTrue(dslQuery.selectNode.all());
        assertEquals(dslQuery.fromNode.path, "/tmp/target/file.par");

        final BinaryExpr conjunctive = (BinaryExpr) dslQuery.whereNode.predicate;
        final AttrEqLong leftLeaf = (AttrEqLong) conjunctive.left;
        assertEquals("col3", leftLeaf.attrName);
        assertEquals(33L, leftLeaf.value);

        final BinaryExpr disjunctive = (BinaryExpr) conjunctive.right;
        final AttrEqLong left = (AttrEqLong) disjunctive.left;
        assertEquals("col1", left.attrName);
        assertEquals(42L, left.value);

        final AttrEqLong right = (AttrEqLong) disjunctive.right;
        assertEquals("col2", right.attrName);
        assertEquals(24L, right.value);
    }
}
