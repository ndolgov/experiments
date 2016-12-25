package net.ndolgov.querydsl.ast;

/**
 * The root of a query AST
 */
public final class DslQuery implements AstNode {
    public final Select selectNode;
    public final From fromNode;
    public final Where whereNode;

    public DslQuery(Select selectNode, From fromNode, Where whereNode) {
        this.whereNode = whereNode;
        this.selectNode = selectNode;
        this.fromNode = fromNode;
    }

    @Override
    public String toString() {
        return selectNode + " " + fromNode + " " + whereNode;
    }

}
