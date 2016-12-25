package net.ndolgov.querydsl.parser;

import net.ndolgov.querydsl.ast.DslQuery;

/**
 * Generic DSL parser API
 */
public interface DslParser {
    /**
     * @param query query expression
     * @return the AST corresponding to a given query string
     */
    DslQuery parse(String query);
}
