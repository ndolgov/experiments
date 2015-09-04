package net.ndolgov.lucenetest.scoring;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

import java.util.HashSet;
import java.util.Set;

/** Extract a fixed set of values */
public final class Visitor extends StoredFieldVisitor {
    private static final String FIELD_1 = "FIELD1";

    private final Set<String> needed;

    public long value;

    public Visitor() {
        needed = new HashSet();
        needed.add(FIELD_1);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) {
        if (fieldInfo.name.equals(FIELD_1)) {
            this.value = value;
        }
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        return needed.contains(fieldInfo.name) ? Status.YES : Status.NO;
    }
}

