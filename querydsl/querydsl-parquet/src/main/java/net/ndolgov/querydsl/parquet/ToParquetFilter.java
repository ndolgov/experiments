package net.ndolgov.querydsl.parquet;

import net.ndolgov.querydsl.ast.expression.AttrEqLong;
import net.ndolgov.querydsl.ast.expression.BinaryExpr;
import net.ndolgov.querydsl.ast.expression.PredicateExpr;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;

import static net.ndolgov.querydsl.parquet.PredicateExprs.isNoOp;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.or;

/**
 * Compile predicate expression tree into a Parquet filter
 */
final class ToParquetFilter {
    /**
     * @param expr filter expression
     * @return Parquet filter corresponding to the filter expression
     */
    public static FilterCompat.Filter transform(PredicateExpr expr) {
        if (isNoOp(expr)) {
            return FilterCompat.NOOP;
        }

        return FilterCompat.get(toPredicate(expr));
    }

    private static FilterPredicate toPredicate(PredicateExpr expr) {
        return expr.accept(new PredicateExpr.ExprVisitor<FilterPredicate>() {
            @Override
            public FilterPredicate visitAttrEqLong(AttrEqLong expr) {
                return eq(longColumn(expr.attrName), expr.value);
            }

            @Override
            public FilterPredicate visitBinaryExpr(BinaryExpr expr) {
                final FilterPredicate left = toPredicate(expr.left);
                final FilterPredicate right = toPredicate(expr.right);

                switch (expr.operator) {
                    case AND:
                        return and(left, right);

                    case OR:
                        return or(left, right);

                    default:
                        throw new IllegalArgumentException("Unexpected op: " + expr.operator);
                }
            }
        });
    }

}