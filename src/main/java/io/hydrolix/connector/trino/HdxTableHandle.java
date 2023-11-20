package io.hydrolix.connector.trino;

import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;

/**
 * A handle to a Hydrolix table during the query planning process.
 * <p>
 * Starts out with {@link #splits} and {@link #columns} both empty; they get filled in as planning progresses and more
 * is known.
 *
 * @param db      database name
 * @param table   table name
 * @param splits  partitions that need to be scanned for this query, once known
 * @param columns columns that need to be read for this query, once known
 */
public record HdxTableHandle(
    String db,
    String table,
    List<HdxTrinoSplit> splits,
    List<HdxColumnHandle> columns
) implements ConnectorTableHandle
{
    public HdxTableHandle withSplits(List<HdxTrinoSplit> splits) {
        return new HdxTableHandle(this.db, this.table, splits, this.columns);
    }

    public HdxTableHandle withColumns(List<HdxColumnHandle> columns) {
        return new HdxTableHandle(this.db, this.table, this.splits, columns);
    }
}