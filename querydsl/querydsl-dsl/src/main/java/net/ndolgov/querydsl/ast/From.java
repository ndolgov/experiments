package net.ndolgov.querydsl.ast;

/**
 * "/home/tmp/file.par"
 */
public final class From implements AstNode {
    public final String path;

    public From(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return " FROM " + path;
    }
}
