package net.ndolgov.lucenetest.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;

/**
 * DIY IndexSearcher that, in contrast to the original Lucene one, can reset Scorer/Processor to reuse them for
 * multiple leaf contexts without excessive memory allocations.
 * @param <T> Query-like query type in your DSL
 */
public final class Searcher<T> {
    private final QueryBuilder<T> queryBuilder;
    private final IndexSearcher indexSearcher;

    public Searcher(QueryBuilder<T> queryBuilder, IndexSearcher indexSearcher) {
        this.queryBuilder = queryBuilder;
        this.indexSearcher = indexSearcher;
    }

    /**
     * For every index segment in the given index, apply the processor to the documents returns by the scorer.
     * @param query proprietary DSL query to execute
     * @param processor field value collector
     * @return processor
     */
    public <P extends Processor> P search(T query, P processor) {
        final QueryScorer scorer = queryBuilder.build(query);

        try {
            for (LeafReaderContext leafCtx : indexSearcher.getIndexReader().leaves()) {
                scorer.reset(leafCtx);
                processor.reset(leafCtx);

                final DocIdSetIterator docIdIter = scorer.iterator();
                if (docIdIter != null) {
                    int docId = docIdIter.nextDoc();
                    while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                        processor.process(docId);
                        docId = docIdIter.nextDoc();
                    }
                }
            }

            return processor;
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query: " + query, e);
        }
    }
}
