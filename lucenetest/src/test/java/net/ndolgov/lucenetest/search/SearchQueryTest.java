package net.ndolgov.lucenetest.search;

import net.ndolgov.lucenetest.search.TestIndexCreator.CheckSum;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;

import static net.ndolgov.lucenetest.search.LuceneFields.INDEXED_FIELD_NAME;
import static net.ndolgov.lucenetest.search.LuceneFields.LONG_FIELD_NAME;
import static net.ndolgov.lucenetest.search.LuceneFields.DOUBLE_FIELD_NAME;
import static net.ndolgov.lucenetest.search.TestIndexCreator.createTestIndex;
import static org.testng.Assert.assertEquals;

public final class SearchQueryTest {
    private static final Logger logger = LoggerFactory.getLogger(SearchQueryTest.class);
    private static final int EVEN = 0;
    private static final int ODD = 1;

    @Test
    public void testDocValuesRetrieval() throws IOException {
        final RAMDirectory directory = new RAMDirectory();
        final CheckSum sums = createTestIndex(directory);

        final IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(directory));

        final Searcher<SearchQuery> evenSearcher = new Searcher<>(new TrivialQueryBuilder(), indexSearcher);
        final TrivialProcessor evenProcessor = evenSearcher.search(new SearchQuery(INDEXED_FIELD_NAME, EVEN), new TrivialProcessor());

        assertEquals(evenProcessor.getLongSum(), sums.evenLongSum);
        assertEquals(evenProcessor.getDoubleSum(), sums.evenDoubleSum);

        final Searcher<SearchQuery> oddSearcher = new Searcher<>(new TrivialQueryBuilder(), indexSearcher);
        final TrivialProcessor oddProcessor = oddSearcher.search(new SearchQuery(INDEXED_FIELD_NAME,  ODD), new TrivialProcessor());

        assertEquals(oddProcessor.getLongSum(), sums.oddLongSum);
        assertEquals(oddProcessor.getDoubleSum(), sums.oddDoubleSum);

        close(directory, indexSearcher);
    }

    private static void close(RAMDirectory directory, IndexSearcher indexSearcher) {
        try {
            indexSearcher.getIndexReader().close();
        } catch (Exception e) {
            logger.warn("Could not close Lucene index reader for directory: " + directory, e);
        }

        try {
            directory.close();
        } catch (Exception e) {
            logger.warn("Could not close Lucene directory: " + directory, e);
        }
    }

    /**
     * SearchQuery-based example of query builder
     */
    public static final class TrivialQueryBuilder implements QueryBuilder<SearchQuery> {
        @Override
        public QueryScorer build(SearchQuery query) {
            return new LongFieldScorer(query.fieldName, query.value);
        }
    }

    /**
     * Sum up field values from the found documents to compare with expected values
     */
    public static final class TrivialProcessor implements Processor {
        private NumericDocValues longField;
        private NumericDocValues doubleField;
        private long longSum = 0;
        private double doubleSum = 0;

        @Override
        public void process(int docId) {
            longSum += longField.get(docId);
            doubleSum += Double.longBitsToDouble(doubleField.get(docId));
        }

        @Override
        public void reset(LeafReaderContext readerCtx) throws IOException {
            longField = readerCtx.reader().getNumericDocValues(LONG_FIELD_NAME);
            doubleField = readerCtx.reader().getNumericDocValues(DOUBLE_FIELD_NAME);

            longSum = 0;
            doubleSum = 0;
        }

        public long getLongSum() {
            return longSum;
        }

        public double getDoubleSum() {
            return doubleSum;
        }
    }
}


