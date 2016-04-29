package net.ndolgov.lucenetest.docvalues;

/**
 * Collect field values from one Lucene document at a time.
 */
public interface Processor {
    void setSomeField(long value);

    void setAnotherField(double value);

    /** Process field values collected for the current Lucene document */
    void onDocument();
}
