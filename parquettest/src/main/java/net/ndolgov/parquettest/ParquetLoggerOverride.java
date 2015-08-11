package net.ndolgov.parquettest;

import java.io.InputStream;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * See https://issues.apache.org/jira/browse/SPARK-4412 and
 * http://stackoverflow.com/questions/805701/load-java-util-logging-config-file-for-default-initialization
 */
public class ParquetLoggerOverride {
    public static void fixParquetJUL() {
        try (final InputStream inputStream = ParquetLoggerOverride.class.getResourceAsStream("/logging.properties")) {
            LogManager.getLogManager().readConfiguration(inputStream);

            // trigger static initialization
            Class.forName(org.apache.parquet.Log.class.getName());

            // make sure it will NOT write to console
            final Logger parquetLog = Logger.getLogger(org.apache.parquet.Log.class.getPackage().getName());
            for (Handler h : parquetLog.getHandlers()) {
                parquetLog.removeHandler(h);
            }
            parquetLog.setUseParentHandlers(true);
            parquetLog.setLevel(Level.INFO);

            // redirect to file
            final FileHandler toFile = new FileHandler();
            parquetLog.addHandler(toFile);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Could not load default logging.properties file");
        }
    }
}
