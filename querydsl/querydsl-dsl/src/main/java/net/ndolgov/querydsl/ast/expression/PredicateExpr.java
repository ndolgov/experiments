package net.ndolgov.querydsl.ast.expression;

/**
 * Predicate expression type
 */
public interface PredicateExpr {
    <E, V extends ExprVisitor<E>> E accept(V visitor);

    interface ExprVisitor<E> {
        E visitAttrEqLong(AttrEqLong expr);

        E visitBinaryExpr(BinaryExpr expr);
    }
}
