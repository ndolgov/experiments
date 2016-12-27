package net.ndolgov.querydsl.antlr.action;

import net.ndolgov.querydsl.ast.DslQuery;
import net.ndolgov.querydsl.parser.DslParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * ANTLR4-based DSL parser that uses grammar actions directly (instead of being a parse tree walker listener)
 */
public final class AntlrActionDslParser implements DslParser {
    @Override
    public DslQuery parse(String query) {
        try {
            return parser(query).parseWithAstBuilder(new AstBuilderImpl()).buildAst();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse: " + query, e);
        }
    }

    private static QueryDslParser parser(String query) {
        return new QueryDslParser(
            new CommonTokenStream(
                new QueryDslLexer(
                    new ANTLRInputStream(query))));
    }
}
