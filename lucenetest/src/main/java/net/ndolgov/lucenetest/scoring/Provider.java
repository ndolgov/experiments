package net.ndolgov.lucenetest.scoring;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.queries.CustomScoreProvider;

import java.io.IOException;

/**
 * Process a matching query document without the need to return it in {@link org.apache.lucene.search.IndexSearcher#search search results}
 */
public final class Provider extends CustomScoreProvider {
    private final Processor processor;

    private final StoredFieldVisitor visitor;

    public Provider(LeafReaderContext context, Processor processor) {
        super(context);
        this.processor = processor;
        this.visitor = processor.visitor();
    }

    @Override
    public float customScore(int docId, float subQueryScore, float valSrcScore) throws IOException {
        context.reader().document(docId, visitor);
        processor.onDocument();

        return super.customScore(docId, subQueryScore, valSrcScore);
    }
}
