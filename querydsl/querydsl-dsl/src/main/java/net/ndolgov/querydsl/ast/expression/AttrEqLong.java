package net.ndolgov.querydsl.ast.expression;

/**
 * Long constant equality operator
 */
public final class AttrEqLong implements PredicateExpr {
    public final String attrName;
    public final long value;

    public AttrEqLong(String attrName, long value) {
        this.attrName = attrName;
        this.value = value;
    }

    @Override
    public <E, V extends ExprVisitor<E>> E accept(V visitor) {
        return visitor.visitAttrEqLong(this);
    }

    @Override
    public String toString() {
        return "(" + attrName + " = " + value + ")";
    }
}
