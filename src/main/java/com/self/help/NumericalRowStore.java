package com.self.help;

import java.util.Arrays;

/**
 * Columnar sidecar store for encoded integer rows.
 * Each column represents one encoded graph field, and each row position matches
 * the raw row id used by {@link GraphIngestionEngine}. This keeps the legacy
 * string store intact while retaining the numeric representation produced
 * during ingestion.
 */
public class NumericalRowStore {
    private static final int DEFAULT_INITIAL_ROW_CAPACITY = 1024;

    private final int columnCount;
    private int[][] columns;
    private int rowCount;

    /**
     * Creates an empty numerical row store with default row capacity.
     *
     * @param columnCount number of encoded columns per row
     */
    public NumericalRowStore(int columnCount) {
        this(columnCount, DEFAULT_INITIAL_ROW_CAPACITY);
    }

    /**
     * Creates an empty numerical row store with explicit row capacity.
     *
     * @param columnCount number of encoded columns per row
     * @param initialRowCapacity initial number of row slots to allocate
     */
    public NumericalRowStore(int columnCount, int initialRowCapacity) {
        if (columnCount <= 0) {
            throw new IllegalArgumentException("Column count must be positive.");
        }
        if (initialRowCapacity < 0) {
            throw new IllegalArgumentException("Initial row capacity cannot be negative.");
        }

        this.columnCount = columnCount;
        this.columns = new int[columnCount][Math.max(1, initialRowCapacity)];
    }

    /**
     * Appends one encoded row at the expected row id.
     * Row ids must be sequential so the numerical row store stays aligned with
     * the raw row store and bitmap indexes.
     *
     * @param rowId expected row id for the encoded row
     * @param values encoded row values in encoded-column order
     * @throws IllegalArgumentException when the row id is not sequential or the row width is invalid
     */
    public synchronized void appendRow(int rowId, int[] values) {
        if (rowId != this.rowCount) {
            throw new IllegalArgumentException("Row ids must be appended sequentially starting at 0.");
        }
        if (values == null || values.length != this.columnCount) {
            throw new IllegalArgumentException("Encoded row width must match the numerical store column count.");
        }

        ensureCapacity(rowId + 1);
        for (int columnIndex = 0; columnIndex < this.columnCount; columnIndex++) {
            this.columns[columnIndex][rowId] = values[columnIndex];
        }
        this.rowCount++;
    }

    /**
     * Reads one encoded integer value.
     *
     * @param rowId row id to read
     * @param columnIndex encoded column index to read
     * @return encoded integer value at the row and column
     */
    public synchronized int getInt(int rowId, int columnIndex) {
        validateRowId(rowId);
        validateColumnIndex(columnIndex);
        return this.columns[columnIndex][rowId];
    }

    /**
     * Reconstructs one encoded row from the columnar store.
     *
     * @param rowId row id to read
     * @return encoded row copy in encoded-column order
     */
    public synchronized int[] getRow(int rowId) {
        validateRowId(rowId);
        int[] row = new int[this.columnCount];
        for (int columnIndex = 0; columnIndex < this.columnCount; columnIndex++) {
            row[columnIndex] = this.columns[columnIndex][rowId];
        }
        return row;
    }

    /**
     * Returns the number of encoded columns per row.
     *
     * @return encoded column count
     */
    public int getColumnCount() {
        return this.columnCount;
    }

    /**
     * Returns the number of encoded rows currently stored.
     *
     * @return encoded row count
     */
    public synchronized int getRowCount() {
        return this.rowCount;
    }

    private void ensureCapacity(int requiredRowCapacity) {
        int currentCapacity = this.columns[0].length;
        if (requiredRowCapacity <= currentCapacity) {
            return;
        }

        int newCapacity = currentCapacity;
        while (newCapacity < requiredRowCapacity) {
            newCapacity *= 2;
        }

        for (int columnIndex = 0; columnIndex < this.columnCount; columnIndex++) {
            this.columns[columnIndex] = Arrays.copyOf(this.columns[columnIndex], newCapacity);
        }
    }

    private void validateRowId(int rowId) {
        if (rowId < 0 || rowId >= this.rowCount) {
            throw new IndexOutOfBoundsException("Row id is outside the stored encoded row range.");
        }
    }

    private void validateColumnIndex(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= this.columnCount) {
            throw new IndexOutOfBoundsException("Column index is outside the encoded column range.");
        }
    }
}
