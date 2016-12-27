package net.ndolgov.querydsl.antlr.action;

import net.ndolgov.querydsl.ast.DslQuery;
import net.ndolgov.querydsl.ast.expression.PredicateExpr;

/**
 * Query AST builder that is notified by grammar actions about found elements.
 */
interface AstBuilder {
    void onMetricId(String longAsStr);

    void onFromFilePath(String path);

    PredicateExpr onAttrEqLong(String quotedAttrname, String longAsStr);

    PredicateExpr onAnd(PredicateExpr left, PredicateExpr right);

    PredicateExpr onOr(PredicateExpr left, PredicateExpr right);

    void onPredicate(PredicateExpr predicate);

    /**
     * @return the root of the AST
     */
    DslQuery buildAst();
}
