package net.ndolgov.querydsl.ast;

import java.util.List;

/**
 * "SELECT '*' | projection*"
 */
public final class Select implements AstNode {
    public final List<Projection> projections;

    // no projections means select all
    public Select(List<Projection> projections) {
        this.projections = projections;
    }

    @Override
    public String toString() {
        return "SELECT " + (all() ? "*" : projections);
    }

    public boolean all() {
        return projections.isEmpty();
    }
}
