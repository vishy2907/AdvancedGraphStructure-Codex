package com.self.help;

import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Iterates over the currently valid projected rows of a {@link GraphIngestionEngine}.
 * The iterator starts from the engine's valid row-id snapshot and resolves each
 * row through the engine's iterator projection, so fully deleted rows are not
 * returned and node id/label columns are always present.
 */
public class GraphEngineIterator implements Iterator<String[]> {
    private final GraphIngestionEngine engine;
    private final IntIterator validRowIds;

    private boolean prepared;
    private boolean hasPreparedRow;
    private int preparedRowId = -1;
    private String[] preparedRow;
    private int currentRowId = -1;

    /**
     * Creates an iterator over the valid row ids exposed by the engine.
     *
     * @param engine graph engine to traverse
     */
    public GraphEngineIterator(GraphIngestionEngine engine) {
        this.engine = engine;
        this.validRowIds = engine.getValidRowIds();
    }

    /**
     * Returns whether another valid projected row is available.
     *
     * @return {@code true} when a valid row can be read with {@link #next()}
     */
    @Override
    public boolean hasNext() {
        prepareNext();
        return this.hasPreparedRow;
    }

    /**
     * Returns the next valid projected graph row.
     *
     * @return projected row values for the next valid row id
     * @throws NoSuchElementException when no more valid rows are available
     */
    @Override
    public String[] next() {
        prepareNext();
        if (!this.hasPreparedRow) {
            throw new NoSuchElementException("No more valid graph rows are available.");
        }

        String[] row = this.preparedRow;
        this.currentRowId = this.preparedRowId;
        clearPreparedRow();
        return row;
    }

    /**
     * Returns the next valid projected graph row formatted for direct printing.
     *
     * @return {@link Arrays#toString(Object[])} representation of the next row
     * @throws NoSuchElementException when no more valid rows are available
     */
    public String nextRowAsString() {
        return Arrays.toString(next());
    }

    /**
     * Returns the row id associated with the last row returned by {@link #next()}.
     *
     * @return current row id, or {@code -1} before iteration starts
     */
    public int currentRowId() {
        return this.currentRowId;
    }

    /**
     * Builds directed in/out degree statistics for all currently visible nodes.
     * This method does not consume the iterator's current position. The engine
     * caches the computed statistics until ingestion or deletion changes the
     * graph, then rebuilds them by breadth-first traversal across all graph
     * components. Rows with only one visible endpoint still contribute a node
     * entry, but not an edge count.
     *
     * @return node id to directed degree statistics, ordered by first visible row appearance
     */
    public Map<String, GraphNodeStat> getGraphStatistics() {
        return this.engine.getGraphStatistics();
    }

    private void prepareNext() {
        if (this.prepared) {
            return;
        }

        while (this.validRowIds.hasNext()) {
            int rowId = this.validRowIds.next();
            String[] row = this.engine.getIteratorRow(rowId);
            if (row != null) {
                this.preparedRowId = rowId;
                this.preparedRow = row;
                this.hasPreparedRow = true;
                this.prepared = true;
                return;
            }
        }

        this.prepared = true;
        this.hasPreparedRow = false;
        this.preparedRowId = -1;
        this.preparedRow = null;
    }

    private void clearPreparedRow() {
        this.prepared = false;
        this.hasPreparedRow = false;
        this.preparedRowId = -1;
        this.preparedRow = null;
    }

}
