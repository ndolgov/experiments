package net.ndolgov.querydsl.parquet;

import net.ndolgov.parquettest.RecordFields;
import net.ndolgov.querydsl.ast.expression.AttrEqLong;
import net.ndolgov.querydsl.ast.expression.BinaryExpr;
import net.ndolgov.querydsl.ast.expression.NoOpExpr;
import net.ndolgov.querydsl.ast.expression.PredicateExpr;

/**
 * Parquet file predicate expression helpers
 */
final class PredicateExprs {
    public static boolean isNoOp(PredicateExpr expr) {
        return expr == NoOpExpr.INSTANCE;
    }

    /**
     * @return expression that will pass through rows matching all child expressions
     */
    public static PredicateExpr conjunction(PredicateExpr left, PredicateExpr right) {
        return new BinaryExpr(left, right, BinaryExpr.Op.AND);
    }

    /**
     * @return expression that will pass through rows matching any of the two child expressions
     */
    public static PredicateExpr disjunction(PredicateExpr left, PredicateExpr right) {
        return new BinaryExpr(left, right, BinaryExpr.Op.OR);
    }
    
    /**
     * @return expression that will pass through rows with attr values equal to a given constant
     */
    public static PredicateExpr columnEq(RecordFields attr, long attrValue) {
        return new AttrEqLong(attr.columnName(), attrValue);
    }

    /**
     * @return expression that will force reading input file with no filter (and so won't incur additional cost)
     */
    public static PredicateExpr noOpPredicate() {
        return NoOpExpr.INSTANCE;
    }
}