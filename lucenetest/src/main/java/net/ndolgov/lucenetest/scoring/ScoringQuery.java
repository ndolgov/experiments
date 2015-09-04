package net.ndolgov.lucenetest.scoring;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.Query;

/**
 * Inspired by http://opensourceconnections.com/blog/2014/03/12/using-customscorequery-for-custom-solrlucene-scoring/
 */
public final class ScoringQuery extends CustomScoreQuery {
    private final Processor processor;

    public ScoringQuery(Query subQuery, Processor processor) {
        super(subQuery);
        this.processor = processor;
    }

    @Override
    protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) {
        return new Provider(context, processor);
    }

    public Processor processor() {
        return processor;
    }
}
