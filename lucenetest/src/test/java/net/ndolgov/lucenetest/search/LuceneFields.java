package net.ndolgov.lucenetest.search;

import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexOptions;

/**
 * Test schema fields
 */
public final class LuceneFields {
    public static final String INDEXED_FIELD_NAME = "TestIndexed";
    public static final String LONG_FIELD_NAME = "TestLong";
    public static final String DOUBLE_FIELD_NAME = "TestDouble";

    /** The only indexed field to search by */
    public static final LongField indexedField = new LongField(INDEXED_FIELD_NAME, -1, indexedLong());

    public static final NumericDocValuesField longValueField = new NumericDocValuesField(LONG_FIELD_NAME, -1);

    public static final NumericDocValuesField doubleValueField = new DoubleDocValuesField(DOUBLE_FIELD_NAME, -1);


    public static FieldType indexedLong() {
        final FieldType type = new FieldType();
        type.setTokenized(false);
        type.setOmitNorms(true);
        type.setStored(true);
        type.setIndexOptions(IndexOptions.DOCS);
        type.setNumericType(FieldType.NumericType.LONG);
        type.freeze();
        return type;
    }
}
