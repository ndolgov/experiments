package net.ndolgov.lucenetest.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;

/**
 * {@link org.apache.lucene.search.TermQuery.TermWeight#scorer}-like scorer of numeric types of long type.
 * It returns an iterator of documents with the given value set for the given field.
 */
public class LongFieldScorer implements QueryScorer {
    private final String field;

    private final BytesRef fieldValueByteRef;

    private PostingsEnum postingsEnum; // to be reused

    private DocIdSetIterator docIdSetIterator;

    public LongFieldScorer(String field, long value) {
        this.field = field;
        this.fieldValueByteRef = asByteRef(value);
    }

    @Override
    public void reset(LeafReaderContext leafReaderContext) throws IOException {
        docIdSetIterator = null;

        final TermsEnum termsEnum = leafReaderContext.reader().fields().terms(field).iterator();
        if (termsEnum == null) {
            return;
        }

        for (BytesRef term = termsEnum.term(); term != null; termsEnum.next()) {
            if (term.compareTo(fieldValueByteRef) == 0) {
                docIdSetIterator = postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
                return;
            }
        }
    }

    @Override
    public DocIdSetIterator iterator() {
        return docIdSetIterator;
    }

    private static BytesRef asByteRef(long value) {
        final BytesRefBuilder refBuilder = new BytesRefBuilder();
        refBuilder.grow(8);
        refBuilder.clear();
        NumericUtils.longToPrefixCoded(value, 0, refBuilder);
        return refBuilder.get();
    }
}
