package net.ndolgov.antlrtest;

import java.util.ArrayList;
import java.util.List;

final class EmbedmentHelperImpl implements EmbedmentHelper {
    private String storageId;

    private final List<Type> varTypes;

    private final List<String> varNames;

    public EmbedmentHelperImpl() {
        varTypes = new ArrayList<Type>();
        varNames = new ArrayList<String>();
    }

    @Override
    public final void onStorage(String storageId) {
        this.storageId = storageId;
    }

    @Override
    public void onVariable(Type type, String name) {
        varTypes.add(type);
        varNames.add(name);
    }

    /**
     * @return parsed query descriptor
     */
    public final QueryDescriptor queryDescriptor() {
        return new QueryDescriptor(storageId, varTypes, varNames);
    }
}
