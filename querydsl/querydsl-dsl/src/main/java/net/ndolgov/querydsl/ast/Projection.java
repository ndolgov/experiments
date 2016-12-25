package net.ndolgov.querydsl.ast;

/**
 * "123456"
 */
public final class Projection implements AstNode {
    public final long metricId;

    public Projection(long metricId) {
        this.metricId = metricId;
    }

    @Override
    public String toString() {
        return String.valueOf(metricId);
    }
}
