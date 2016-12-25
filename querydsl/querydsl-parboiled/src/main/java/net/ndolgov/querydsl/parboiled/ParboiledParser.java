package net.ndolgov.querydsl.parboiled;

import net.ndolgov.querydsl.ast.DslQuery;
import net.ndolgov.querydsl.ast.From;
import net.ndolgov.querydsl.ast.Projection;
import net.ndolgov.querydsl.ast.Select;
import net.ndolgov.querydsl.ast.Where;
import net.ndolgov.querydsl.ast.expression.AttrEqLong;
import net.ndolgov.querydsl.ast.expression.BinaryExpr;
import net.ndolgov.querydsl.ast.expression.PredicateExpr;
import org.parboiled.BaseParser;
import org.parboiled.Rule;
import org.parboiled.support.Var;

import java.util.ArrayList;
import java.util.List;

import static net.ndolgov.querydsl.parser.Tokens.*;

class ParboiledParser extends BaseParser<Object> {
    private static final int SECOND = 1; // the next after the top of the stack
    private static final int THIRD = 2; // the next after the second

    /**
     * @return the top level rule that will return a DslQuery instance (on top of the stack)
     */
    //SELECT FROM WHERE EOI (on the stack they are in the reversed order)
    public Rule DslQuery() {
        return Sequence(
            Select(), Spaces(),
            From(), Spaces(),
            Where(), EOI,
            push(new DslQuery((Select) pop(THIRD), (From) pop(SECOND), (Where) pop())));
    }

    // WHERE CONDITION
    Rule Where() {
        return Sequence(
            IgnoreCase(WHERE),
            Spaces(),
            Condition(),
            push(new Where((PredicateExpr) pop())));
    }

    // CONDITION = 'attr' = LONG | (CONDITION && CONDITION) | (CONDITION || CONDITION)
    Rule Condition() {
        return FirstOf(
            AttributeEqLong(),
            Sequence(
                NestedCondition(), AND, NestedCondition(),
                push(new BinaryExpr((PredicateExpr) pop(SECOND), (PredicateExpr) pop(), BinaryExpr.Op.AND))
            ),
            Sequence(
                NestedCondition(), OR, NestedCondition(),
                push(new BinaryExpr((PredicateExpr) pop(SECOND), (PredicateExpr) pop(), BinaryExpr.Op.OR))
            )
        );
    }

    // (CONDITION)
    Rule NestedCondition() {
        return Sequence(
            Spaces(),
            LPAREN, Condition(), RPAREN,
            Spaces()
        );
    }

    // 'ATTR_NAME' = LONG
    Rule AttributeEqLong() {
        return Sequence(
            Quoted(),
            EQUALS,
            Long(),
            push(new AttrEqLong((String) pop(SECOND), (Long) pop())));
    }

    // FROM 'abs-file-path'
    Rule From() {
        return Sequence(
            IgnoreCase(FROM),
            Spaces(),
            Quoted(),
            push(new From((String) pop())));
    }

    // SELECT '*' | METRICS
    Rule Select() {
        final Var<List<Projection>> projections = new Var<>(new ArrayList<>());
        return Sequence(
            IgnoreCase(SELECT),
            Spaces(),
            FirstOf(
                ASTERISK,
                metricIds(projections)),
            push(new Select(projections.get())));
    }

    // METRICS = METRIC_ID (, METRIC_ID)+
    Rule metricIds(Var<List<Projection>> projections) {
        return Sequence(
            Projection(), projections.get().add((Projection) pop()),
            ZeroOrMore(Comma(), Projection(), projections.get().add((Projection) pop()))
        );
    }

    // METRIC_ID = LONG
    Rule Projection() {
        return Sequence(
            Long(),
            push(new Projection((Long) pop())));
    }

    // 'string'
    Rule Quoted() {
        return Sequence(LQUOTE, Path(), RQUOTE);
    }

    Rule Path() {
        return Sequence(OneOrMore(TestNot(RQUOTE), ANY), push(match()));
    }

    Rule Long() {
        return Sequence(Number(), push(Long.valueOf(match())));
    }

    Rule Number() {
        return OneOrMore(Digit());
    }

    Rule Digit() {
        return CharRange('0', '9');
    }

    Rule Spaces() {
        return ZeroOrMore(' ');
    }

    Rule Comma() {
        return Ch(',');
    }

    private boolean debug() {
        System.out.println(match()); // set breakpoint here if required
        return true;
    }
}
