package net.ndolgov.querydsl.antlr.action;

import net.ndolgov.querydsl.ast.DslQuery;
import net.ndolgov.querydsl.ast.From;
import net.ndolgov.querydsl.ast.Projection;
import net.ndolgov.querydsl.ast.Select;
import net.ndolgov.querydsl.ast.Where;
import net.ndolgov.querydsl.ast.expression.AttrEqLong;
import net.ndolgov.querydsl.ast.expression.BinaryExpr;
import net.ndolgov.querydsl.ast.expression.PredicateExpr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

final class AstBuilderImpl implements AstBuilder {
    private String path;
    private List<Long> metricIds;
    private PredicateExpr predicate;

    public AstBuilderImpl() {
        metricIds = new ArrayList<>();
    }

    @Override
    public void onMetricId(String longAsStr) {
        metricIds.add(Long.valueOf(longAsStr));
    }

    @Override
    public void onFromFilePath(String path) {
        this.path = path;
    }

    @Override
    public PredicateExpr onAttrEqLong(String quotedAttrname, String longAsStr) {
        return new AttrEqLong(
            unquoted(quotedAttrname),
            Long.valueOf(longAsStr));
    }

    @Override
    public PredicateExpr onAnd(PredicateExpr left, PredicateExpr right) {
        return new BinaryExpr(left, right, BinaryExpr.Op.AND);
    }

    @Override
    public PredicateExpr onOr(PredicateExpr left, PredicateExpr right) {
        return new BinaryExpr(left, right, BinaryExpr.Op.OR);
    }

    @Override
    public void onPredicate(PredicateExpr predicate) {
        this.predicate = predicate;
    }

    @Override
    public DslQuery buildAst() {
        return new DslQuery(
            new Select(projections(metricIds)),
            new From(path),
            new Where(predicate));
    }

    // 'str' -> str
    private static String unquoted(String quoted) {
        return quoted.substring(1, quoted.length() - 1);
    }

    private static List<Projection> projections(List<Long> metricIds) {
        if (metricIds.isEmpty()) {
            return Collections.emptyList();
        }

        return metricIds.stream().map(Projection::new).collect(Collectors.toList());
    }
}
