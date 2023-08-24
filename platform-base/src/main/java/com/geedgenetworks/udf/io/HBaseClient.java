package com.geedgenetworks.udf.io;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;


/**
 * A client class for interacting with HBase. This class provides methods for
 * performing common HBase operations like scanning a table and closing the
 * connection.
 *
 * @author chaoc
 * @since 1.0
 */
public class HBaseClient implements Closeable {

    private final Connection connection;

    /**
     * Enhance the given HBase Connection.
     *
     * @param connection The HBase Connection to use.
     */
    public HBaseClient(final Connection connection) {
        this.connection = connection;
    }

    /**
     * Gets a Table instance for the specified table name.
     *
     * @param tableName The name of the HBase table.
     * @return A Table instance for the specified table.
     * @throws IOException If an I/O error occurs while accessing the table.
     */
    public Table getTable(final String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        return this.connection.getTable(name);
    }

    /**
     * Scans a specified HBase table with a limit and time range.
     *
     * @param tableName The name of the HBase table to scan.
     * @param limit     The maximum number of rows to return.
     * @param range     The time range to filter results.
     * @return An iterator of Result objects from the scan.
     * @throws IOException If an I/O error occurs while scanning the table.
     */
    public Iterator<Result> scan(final String tableName, final int limit, final TimeRange range) throws IOException {
        final Scan scan = new Scan();
        scan.setTimeRange(range.getMin(), range.getMax());
        scan.setLimit(limit);
        final Table table = this.getTable(tableName);
        final ResultScanner scanner = table.getScanner(scan);
        return scanner.iterator();
    }

    /**
     * Scans a specified HBase table with a limit and time range, and applies a
     * consumer action to each result.
     *
     * @param table  The name of the HBase table to scan.
     * @param limit  The maximum number of rows to return.
     * @param range  The time range to filter results.
     * @param action The consumer action to apply to each HBaseResult.
     * @throws IOException If an I/O error occurs while scanning the table.
     */
    public void scan(final String table, final int limit, final TimeRange range,
                     final Consumer<HBaseResult> action) throws IOException {
        final Iterator<Result> resultIterator = this.scan(table, limit, range);
        while (resultIterator.hasNext()) {
            final Result result = resultIterator.next();
            action.accept(new HBaseResult(result));
        }
    }

    /**
     * Closes the HBase connection.
     *
     * @throws IOException If an I/O error occurs while closing the connection.
     */
    @Override
    public void close() throws IOException {
        this.connection.close();
    }

    /**
     * A class representing an HBase result. This class provides methods to retrieve
     * values from the result.
     */
    public static final class HBaseResult {

        private final Result result;

        /**
         * Constructs a new HBaseResult with the given Result.
         *
         * @param result The HBase Result to encapsulate.
         */
        public HBaseResult(final Result result) {
            this.result = result;
        }

        /**
         * Gets the value from the specified column family and column.
         *
         * @param family The column family.
         * @param column The column name.
         * @return The value as a byte array.
         */
        public byte[] getValue(final String family, final String column) {
            return result.getValue(Bytes.toBytes(family), Bytes.toBytes(column));
        }

        /**
         * Gets the value from the specified column family and column as a string.
         *
         * @param family The column family.
         * @param column The column name.
         * @return The value as a string, or null if it is not present.
         */
        public String getString(final String family, final String column) {
            final byte[] bytes = getValue(family, column);
            return bytes == null ? null : Bytes.toString(bytes).trim();
        }

        /**
         * Gets the value from the specified column family and column as a long.
         *
         * @param family The column family.
         * @param column The column name.
         * @return The value as a long, or 0L if it is not present.
         */
        public long getLong(final String family, final String column) {
            final byte[] bytes = getValue(family, column);
            return bytes == null ? 0L : Bytes.toLong(bytes);
        }
    }
}
