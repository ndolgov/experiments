package net.ndolgov.antlrtest;

import java.util.List;

import com.google.common.base.Preconditions;

/**
 * Execution-time representation of a parsed query
 */
public final class QueryDescriptor {
    public final String storageId;

    public final VarDescriptor[] variables;

    public QueryDescriptor(String storageId, List<Type> types, List<String> names) {
        this.storageId = storageId;

        Preconditions.checkArgument(types.size() == names.size());
        variables = new VarDescriptor[types.size()];
        for (int i = 0; i < variables.length; i++) {
            variables[i] = new VarDescriptor(types.get(i), names.get(i));
        }
    }

    public static final class VarDescriptor {
        public final Type type;

        public final String name;

        public VarDescriptor(Type type, String name) {
            this.type = type;
            this.name = name;
        }
    }
}
