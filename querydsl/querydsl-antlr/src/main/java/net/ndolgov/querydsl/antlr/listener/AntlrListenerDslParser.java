package net.ndolgov.querydsl.antlr.listener;

import net.ndolgov.querydsl.parser.DslParser;
import net.ndolgov.querydsl.ast.DslQuery;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 * ANTLR4-based DSL parser implementation that implements parse tree listener interface (and has no Java code in the grammar)
 */
public final class AntlrListenerDslParser implements DslParser {
    @Override
    public DslQuery parse(String query) {
        try {
            final AstBuildingListener listener = new AstBuildingListener();
            new ParseTreeWalker().walk(listener, parser(query).query());
            return listener.buildAst();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse: " + query, e);
        }
    }

    private static ParquetDslParser parser(String query) {
        final ParquetDslParser parser = new ParquetDslParser(
            new CommonTokenStream(
                new ParquetDslLexer(
                    new ANTLRInputStream(query))));

        parser.setBuildParseTree(true);
        return parser;
    }
}
