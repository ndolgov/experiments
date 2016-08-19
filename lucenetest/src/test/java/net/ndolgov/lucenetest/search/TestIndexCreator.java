package net.ndolgov.lucenetest.search;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

import java.io.IOException;

import static net.ndolgov.lucenetest.search.LuceneFields.doubleValueField;
import static net.ndolgov.lucenetest.search.LuceneFields.indexedField;
import static net.ndolgov.lucenetest.search.LuceneFields.longValueField;

/**
 * Create a small test index in a given directory
 */
public final class TestIndexCreator {
    public static CheckSum createTestIndex(Directory directory) throws IOException {
        final IndexWriter indexWriter = indexWriter(directory);

        final CheckSum sums = writeTestDocuments(indexWriter);

        indexWriter.close();

        return sums;
    }

    private static IndexWriter indexWriter(Directory directory) {
        try {
            return new IndexWriter(directory, new IndexWriterConfig(new KeywordAnalyzer()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create index writer", e);
        }
    }

    private static CheckSum writeTestDocuments(IndexWriter indexWriter) throws IOException {
        final Document document = new Document();
        document.add(indexedField);
        document.add(longValueField);
        document.add(doubleValueField);

        long longSum = 0;
        double doubleSum = 0;
        long evenLongSum = 0;
        double evenDoubleSum = 0;

        for (int i = 0; i < 10; i++) {
            final int longValue = 10000 + i;
            longValueField.setLongValue(longValue);

            final double doubleValue = 123.456 + i;
            doubleValueField.setDoubleValue(doubleValue);

            final int remainder = longValue % 2;
            indexedField.setLongValue(remainder);

            indexWriter.addDocument(document);

            if (remainder == 0) {
                evenLongSum += longValue;
                evenDoubleSum += doubleValue;
            } else {
                longSum += longValue;
                doubleSum += doubleValue;
            }
        }

        return new CheckSum(longSum, doubleSum, evenLongSum, evenDoubleSum);
    }

    /**
     * An easy way to check multiple documents were retrieved correctly
     */
    public static final class CheckSum {
        public final long oddLongSum;
        public final double oddDoubleSum;
        public final long evenLongSum;
        public final double evenDoubleSum;

        public CheckSum(long oddLongSum, double oddDoubleSum, long evenLongSum, double evenDoubleSum) {
            this.oddLongSum = oddLongSum;
            this.oddDoubleSum = oddDoubleSum;
            this.evenLongSum = evenLongSum;
            this.evenDoubleSum = evenDoubleSum;
        }
    }
}
