package net.ndolgov.lucenetest.docvalues;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.Query;

import java.io.IOException;

/**
 * Find and remember NumericDocValues for the required fields
 */
public final class DocValuesQuery extends CustomScoreQuery {
    private final Processor processor;

    /**
     * @param subQuery the actual query to run
     * @param processor retrieved field value processor
     */
    public DocValuesQuery(Query subQuery, Processor processor) {
        super(subQuery);
        this.processor = processor;
    }

    @Override
    protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) throws IOException {
        final LeafReader reader = context.reader();

        final NumericDocValues longs = reader.getNumericDocValues("SomeLongFieldName");
        final NumericDocValues doubles = reader.getNumericDocValues("SomeDoubleFieldName");

        return new Provider(context, processor, longs, doubles);
    }

    public Processor processor() {
        return processor;
    }
}
