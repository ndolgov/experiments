package net.ndolgov.querydsl.ast.expression;

/**
 *
 */
public final class NoOpExpr implements PredicateExpr {
    public static final PredicateExpr INSTANCE = new NoOpExpr();

    @Override
    public <E, V extends ExprVisitor<E>> E accept(V visitor) {
        return null;
    }

    private NoOpExpr() {
    }
}
