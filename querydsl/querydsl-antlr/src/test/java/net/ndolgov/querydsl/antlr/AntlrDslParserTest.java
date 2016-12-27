package net.ndolgov.querydsl.antlr;

import net.ndolgov.querydsl.ast.DslQuery;

import net.ndolgov.querydsl.ast.expression.AttrEqLong;
import net.ndolgov.querydsl.ast.expression.BinaryExpr;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class AntlrDslParserTest {
    private static final String PATH = "/tmp/target/file.par";
    private static final long METRIC1 = 123L;
    private static final long METRIC2 = 456L;
    private static final String COLUMN = "col";
    private static final String COLUMN1 = "col1";
    private static final String COLUMN2 = "col2";
    private static final String COLUMN3 = "col3";
    private static final long VALUE1 = 42L;
    private static final long VALUE2 = 24L;
    private static final long VALUE3 = 33L;

    @Test
    public void testSelectSomeFromFile() throws Exception {
        final String query = String.format(
            "select %d,%d from '%s' where ('%s'=%d) || ('%s'=%d)",
            METRIC1, METRIC2,
            PATH,
            COLUMN1, VALUE1,
            COLUMN2, VALUE2
        );

        final DslQuery dslQuery = new AntlrDslParser().parse(query);
        assertEquals(dslQuery.selectNode.projections.get(0).metricId, METRIC1);
        assertEquals(dslQuery.selectNode.projections.get(1).metricId, METRIC2);
        assertEquals(dslQuery.fromNode.path, PATH);

        final BinaryExpr disjunctive = (BinaryExpr) dslQuery.whereNode.predicate;
        final AttrEqLong left = (AttrEqLong) disjunctive.left;
        assertEquals(COLUMN1, left.attrName);
        assertEquals(VALUE1, left.value);

        final AttrEqLong right = (AttrEqLong) disjunctive.right;
        assertEquals(COLUMN2, right.attrName);
        assertEquals(VALUE2, right.value);
    }

    @Test
    public void testSelectAllFromFile() throws Exception {
        final String query = String.format(
            "select * from '%s' where ('%s'=%d) && (('%s'=%d) || ('%s'=%d))",
            PATH,
            COLUMN3, VALUE3,
            COLUMN1, VALUE1,
            COLUMN2, VALUE2
        );

        final DslQuery dslQuery = new AntlrDslParser().parse(query);
        assertTrue(dslQuery.selectNode.projections.isEmpty());
        assertTrue(dslQuery.selectNode.all());
        assertEquals(dslQuery.fromNode.path, PATH);

        final BinaryExpr conjunctive = (BinaryExpr) dslQuery.whereNode.predicate;
        final AttrEqLong leftLeaf = (AttrEqLong) conjunctive.left;
        assertEquals(COLUMN3, leftLeaf.attrName);
        assertEquals(VALUE3, leftLeaf.value);

        final BinaryExpr disjunctive = (BinaryExpr) conjunctive.right;
        final AttrEqLong left = (AttrEqLong) disjunctive.left;
        assertEquals(COLUMN1, left.attrName);
        assertEquals(VALUE1, left.value);

        final AttrEqLong right = (AttrEqLong) disjunctive.right;
        assertEquals(COLUMN2, right.attrName);
        assertEquals(VALUE2, right.value);
    }

    @Test
    public void testAlphanumericAttributes() throws Exception {
        final String unusualCharacterPath = "target/pffq-1482812709420.par";

        final String query = String.format(
            "select * from '%s' where ('%s'=%d) || ('%s'=%d)",
            unusualCharacterPath,
            COLUMN, VALUE1,
            COLUMN2, VALUE2
        );

        final DslQuery dslQuery = new AntlrDslParser().parse(query);
        assertEquals(dslQuery.fromNode.path, unusualCharacterPath);

        final BinaryExpr disjunctive = (BinaryExpr) dslQuery.whereNode.predicate;
        final AttrEqLong left = (AttrEqLong) disjunctive.left;
        assertEquals(COLUMN, left.attrName);

        final AttrEqLong right = (AttrEqLong) disjunctive.right;
        assertEquals(COLUMN2, right.attrName);

    }
}