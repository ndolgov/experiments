package net.ndolgov.lucenetest.search;

/**
 * DIY Lucene query compiler.
 * @param <T> Query-like query type in your DSL
 */
public interface QueryBuilder<T> {
    /**
     * @return scorer corresponding to a given query
     */
    QueryScorer build(T query);
}
