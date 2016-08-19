package net.ndolgov.lucenetest.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * DIY Lucene scorer that, in contrast to original Lucene one, can be reused for multiple leaf context to avoid
 * excessive memory allocations.
 */
public interface QueryScorer {
    /**
     * @return matching documents from the current index segment
     */
    DocIdSetIterator iterator();

    /**
     * Prepare to traverse another index segment
     */
    void reset(LeafReaderContext leafReaderContext) throws IOException;
}
