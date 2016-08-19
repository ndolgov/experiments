package net.ndolgov.lucenetest.search;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

/**
 * Collect actual field values from matching Lucene documents
 */
public interface Processor {
    /**
     * Retrieve values of required fields from the document with given id
     * @param docId
     */
    void process(int docId);

    /**
     * Prepare to process another index segment
     * @param readerCtx
     * @throws IOException
     */
    void reset(LeafReaderContext readerCtx) throws IOException;
}
