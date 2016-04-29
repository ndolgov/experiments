package net.ndolgov.lucenetest.docvalues;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.CustomScoreProvider;

/**
 * Retrieve values from DocValues fields
 */
public final class Provider extends CustomScoreProvider {
    private static final float DEFAULT_SCORE = 0;

    private final Processor processor;
    private final NumericDocValues someLongs;
    private final NumericDocValues someDoubles;

    public Provider(LeafReaderContext context, Processor processor, NumericDocValues someLongs, NumericDocValues someDoubles) {
        super(context);
        this.processor = processor;
        this.someLongs = someLongs;
        this.someDoubles = someDoubles;
    }

    @Override
    public float customScore(int docId, float subQueryScore, float valSrcScore) {
        processor.setSomeField(someLongs.get(docId));
        processor.setAnotherField(Double.longBitsToDouble(someDoubles.get(docId)));

        processor.onDocument();

        return DEFAULT_SCORE;
    }
}
