package net.ndolgov.antlrtest;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

/**
 * Parse a given query and return extracted execution-time representation of the parsed query
 */
public final class QueryParser {
    /**
     * Parse a query expression and return the extracted request configuration
     * @param expr query expression
     * @return extracted request configuration
     */
    public static QueryDescriptor parse(String expr) {
        try {
            final TestQueryParser parser = new TestQueryParser(new CommonTokenStream(new TestQueryLexer(new ANTLRStringStream(expr))));
            final EmbedmentHelperImpl helper = parser.parseWithHelper(new EmbedmentHelperImpl());

            return helper.queryDescriptor();
        } catch (RecognitionException e) {
            throw new RuntimeException("Could not parse query: " + expr, e);
        }
    }
}
