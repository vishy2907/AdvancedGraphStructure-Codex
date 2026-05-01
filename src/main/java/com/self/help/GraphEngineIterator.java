package com.self.help;

import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterates over the currently valid projected rows of a {@link GraphIngestionEngine}.
 * The iterator starts from the engine's valid row-id snapshot and resolves each
 * row through the engine, so fully deleted rows are not returned to callers.
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

    private void prepareNext() {
        if (this.prepared) {
            return;
        }

        while (this.validRowIds.hasNext()) {
            int rowId = this.validRowIds.next();
            String[] row = this.engine.getRow(rowId);
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
