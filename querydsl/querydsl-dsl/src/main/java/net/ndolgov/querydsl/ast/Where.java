package net.ndolgov.querydsl.ast;

import net.ndolgov.querydsl.ast.expression.PredicateExpr;

/**
 * "WHERE predicate"
 */
public final class Where implements AstNode {
    public final PredicateExpr predicate;

    public Where(PredicateExpr predicate) {
        this.predicate = predicate;
    }

    @Override
    public String toString() {
        return " WHERE " + predicate;
    }
}
