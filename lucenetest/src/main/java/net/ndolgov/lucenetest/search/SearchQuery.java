package net.ndolgov.lucenetest.search;

/**
 * The simplest possible TermQuery-like example of an abstraction playing the role of a "Lucene Query".
 *
 * In real life it would be a much richer proprietary data structure that can represent filters and similar
 * abstractions of your query DSL.
 */
public final class SearchQuery {
    public final String fieldName;
    public final long value;

    public SearchQuery(String fieldName, long value) {
        this.fieldName = fieldName;
        this.value = value;
    }
}
