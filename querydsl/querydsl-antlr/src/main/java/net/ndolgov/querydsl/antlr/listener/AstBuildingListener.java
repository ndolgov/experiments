package net.ndolgov.querydsl.antlr.listener;

import net.ndolgov.querydsl.ast.DslQuery;
import net.ndolgov.querydsl.ast.From;
import net.ndolgov.querydsl.ast.Projection;
import net.ndolgov.querydsl.ast.Select;
import net.ndolgov.querydsl.ast.Where;
import net.ndolgov.querydsl.ast.expression.AttrEqLong;
import net.ndolgov.querydsl.ast.expression.BinaryExpr;
import net.ndolgov.querydsl.ast.expression.PredicateExpr;
import org.antlr.v4.runtime.tree.ErrorNode;

import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * Build an AST with a manually maintained stack
 */
final class AstBuildingListener extends ParquetDslBaseListener {
    private final Stack<Object> stack = new Stack<>();

    /**
     * @return the root AST node
     */
    public DslQuery buildAst() {
        return pop();
    }
    
    @Override
    public void exitQuery(ParquetDslParser.QueryContext ctx) {
        final Where where = (Where) stack.pop();
        final From from = (From) stack.pop();
        final Select select = (Select) stack.pop();
        push(new DslQuery(select, from, where));
    }

    @Override
    public void exitSelectExpr(ParquetDslParser.SelectExprContext ctx) {
        push(new Select(projections(ctx.metricExpr())));
    }

    @Override
    public void exitFromExpr(ParquetDslParser.FromExprContext ctx) {
        push(new From(ctx.FILEPATH().getSymbol().getText()));
    }

    @Override
    public void exitWhereExpr(ParquetDslParser.WhereExprContext ctx) {
        push(new Where(pop()));
    }

    @Override
    public void exitLongEqExpr(ParquetDslParser.LongEqExprContext ctx) {
        push(new AttrEqLong(
            unquoted(ctx.QUOTED_ID().getSymbol().getText()),
            Long.valueOf(ctx.INT().getSymbol().getText())));
    }

    @Override
    public void exitAnd(ParquetDslParser.AndContext ctx) {
        onBinaryExpr(BinaryExpr.Op.AND);
    }

    @Override
    public void exitOr(ParquetDslParser.OrContext ctx) {
        onBinaryExpr(BinaryExpr.Op.OR);
    }
    
    @Override
    public void visitErrorNode(ErrorNode node) {
        throw new IllegalArgumentException("Failed to parse " + node); //todo really?
    }

    private <T> T pop() {
        //System.out.println(" <= " + stack.peek());
        return  (T) stack.pop();
    }

    private void push(Object item) {
        //System.out.println(" => " + item);
        stack.push(item);
    }

    // 'str' -> str
    private static String unquoted(String quoted) {
        return quoted.substring(1, quoted.length() - 1);
    }

    private static List<Projection> projections(ParquetDslParser.MetricExprContext ctx) {
        if (ctx == null) { // '*'
            return Collections.emptyList();
        }

        return ctx.INT().stream().
            map(tn -> tn.getSymbol().getText()).
            map(Long::valueOf).
            map(Projection::new).
            collect(Collectors.toList());
    }
    
    private void onBinaryExpr(BinaryExpr.Op op) {
        final PredicateExpr right = pop();
        final PredicateExpr left = pop();
        push(new BinaryExpr(left, right, op));
    }
}
