package net.ndolgov.lucenetest.scoring;

import org.apache.lucene.search.IndexSearcher;

/**
 * Execute custom query, ignore returned TopDocs
 */
public final class Searcher {
    private static final int NONE = 1; // the minimum allowed number

    public static Processor search(ScoringQuery query, IndexSearcher searcher) {
        try {
            searcher.search(query, NONE);
            return query.processor();
        } catch (Exception e) {
            throw new RuntimeException("Failed to process matching Lucene documents", e);
        }
    }
}
