package net.ndolgov.lucenetest.scoring;

import org.apache.lucene.index.StoredFieldVisitor;

/**
 * Extract data points from Lucene document field values. The visitor approach allows to reduce new object churn.
 */
public interface Processor {
    /** @return Lucene field visitor to apply to all matching documents */
    StoredFieldVisitor visitor();

    /** Process Lucene document fields gathered by the {@link #visitor visitor} */
    void onDocument();
}
