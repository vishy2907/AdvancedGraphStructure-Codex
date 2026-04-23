package com.self.help;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.*;

@Data
public class RawDataStore {
    private final @NotNull List<String> columnNames;
    int size;
    final Map<String, Integer> columnNameToIndexMap = new LinkedHashMap<>();
    final List<String>[] columns;

    /**
     *
     * @param columnNames
     */
    public RawDataStore(@NotNull List<String> columnNames) {
        columns = new List[columnNames.size()];
        for (int index = 0; index < columnNames.size(); index++) {
            columnNameToIndexMap.putIfAbsent(columnNames.get(index), columnNameToIndexMap.size());
            columns[index] = new ArrayList<String>();
        }
        this.columnNames = columnNames;
    }

    public synchronized int ingestRow(@NotNull String[] values) {
        if (values.length != columnNameToIndexMap.size()) {
            throw new IllegalArgumentException();
        }
        for (int index = 0; index < values.length; index++) {
            columns[index].add(values[index]);
        }
        return size++;
    }

    /**
     *
     * @param rowIndex
     * @return
     */
    public String[] getRow(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= size) {
            return null;
        }
        return Arrays.stream(columns).map(column -> column.get(rowIndex)).
                toList().
                toArray(new String[0]);
    }

    public int getColumnIndex(String columnName) {
        return columnNameToIndexMap.getOrDefault(columnName, -1);
    }

    public String getString(int rowId, int columnIndex) {
        return columns[columnIndex].get(rowId);
    }
}
