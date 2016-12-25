package net.ndolgov.querydsl.ast.expression;

/**
 * "predicate1 && predicate2"
 * todo binary vs n-ary
 */
public final class BinaryExpr implements PredicateExpr {
    public final PredicateExpr left;
    public final PredicateExpr right;
    public final Op operator;

    public BinaryExpr(PredicateExpr left, PredicateExpr right, Op operator) {
        this.left = left;
        this.right = right;
        this.operator = operator;
    }

    @Override
    public <E, V extends ExprVisitor<E>> E accept(V visitor) {
        return visitor.visitBinaryExpr(this);
    }

    @Override
    public final String toString() {
        return "(" + left.toString() + " " + operator.toString() + " " + right.toString() + ")";
    }

    public enum Op {
        EQ,
        AND, OR
    }
}
