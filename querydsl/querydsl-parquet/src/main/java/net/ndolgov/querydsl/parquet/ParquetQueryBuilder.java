package net.ndolgov.querydsl.parquet;

import net.ndolgov.parquettest.RecordFields;
import net.ndolgov.querydsl.ast.DslQuery;
import net.ndolgov.querydsl.ast.expression.PredicateExpr;
import org.apache.parquet.filter2.compat.FilterCompat;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static net.ndolgov.querydsl.parquet.PredicateExprs.columnEq;
import static net.ndolgov.querydsl.parquet.PredicateExprs.conjunction;
import static net.ndolgov.querydsl.parquet.PredicateExprs.disjunction;

/**
 * Build a Parquet filter from a query DSL AST
 * This builder assumes a particular Parquet column name to compile "select xxx,yyy"-like expressions.
 */
public final class ParquetQueryBuilder {
    private static final PredicateExpr EOF = new PredicateExpr() {public <E, V extends ExprVisitor<E>> E accept(V v) {return null;}};

    public static FilterCompat.Filter build(DslQuery query) {
        return ToParquetFilter.transform(toPredicates(query));
    }

    private static PredicateExpr toPredicates(DslQuery query) {
        if (query.selectNode.all()) {
            return query.whereNode.predicate;
        }

        if (PredicateExprs.isNoOp(query.whereNode.predicate)) {
            return buildBinaryTree(metricPredicates(query));
        }

        return conjunction(
            buildBinaryTree(metricPredicates(query)),
            query.whereNode.predicate);
    }

    private static Set<PredicateExpr> metricPredicates(DslQuery query) {
        return query.selectNode.projections.stream().
            map(projection -> columnEq(RecordFields.METRIC, projection.metricId)).
            collect(Collectors.toSet());
    }

    /**
     * Built-in Parquet filters are composed with binary operators only so a linear collection must be converted to a tree
     * @param exprs a collection of predicates
     * @return a binary tree representation of the predicates
     */
    private static PredicateExpr buildBinaryTree(Collection<? extends PredicateExpr> exprs) {
        if (exprs.isEmpty()) {
            throw new IllegalArgumentException();
        }

        final Queue<PredicateExpr> queue = new LinkedList<>(exprs);
        queue.offer(EOF);

        while (true) {
            final PredicateExpr left = queue.poll();

            if (left == EOF) {
                if (queue.isEmpty()) {
                    throw new IllegalStateException();
                } else {
                    queue.offer(EOF); // mark the end of next queue pass
                }
            } else {
                final PredicateExpr right = queue.poll();

                if (right == EOF) {
                    if (queue.isEmpty()) {
                        return left;  // the last item
                    } else {
                        queue.offer(left); // to try in the next pass but stay in the Q to the right
                        queue.offer(EOF); // mark the end of current queue pass
                    }
                } else {
                    queue.offer(disjunction(left, right));
                }
            }
        }
    }}
