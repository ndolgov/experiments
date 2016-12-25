package net.ndolgov.querydsl.parboiled;

import net.ndolgov.querydsl.parser.DslParser;
import net.ndolgov.querydsl.ast.DslQuery;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.RecoveringParseRunner;

/**
 * Parboiled-based DSL parser implementation
 */
public final class ParboiledDslParser implements DslParser {
    private final RecoveringParseRunner<DslQuery> runner;

    public ParboiledDslParser() {
        runner = new RecoveringParseRunner<>(Parboiled.createParser(ParboiledParser.class).DslQuery());
    }

    @Override
    public DslQuery parse(String query) {
        try {
            return runner.run(query).resultValue;
        } finally {
            //System.out.println(runner.getLog()); // for syntax debugging, TracingParseRunner
        }
    }
}
