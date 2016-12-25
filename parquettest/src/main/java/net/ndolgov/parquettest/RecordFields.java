package net.ndolgov.parquettest;

/**
 * Supported record fields
 */
public enum RecordFields {
    ROW_ID {
        @Override
        public String columnName() {
            return "ROWID";
        }

        @Override
        public int index() {
            return 0;
        }
    },
    METRIC {
        @Override
        public String columnName() {
            return "METRIC";
        }

        @Override
        public int index() {
            return 1;
        }
    },
    TIME {
        @Override
        public String columnName() {
            return "TIME";
        }

        @Override
        public int index() {
            return 2;
        }
    },
    VALUE {
        @Override
        public String columnName() {
            return "VALUE";
        }

        @Override
        public int index() {
            return 3;
        }
    };

    public abstract String columnName();

    public abstract int index();
}
