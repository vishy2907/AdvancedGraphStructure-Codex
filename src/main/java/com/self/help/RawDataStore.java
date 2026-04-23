package com.self.help;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.*;

@Data
public class RawDataStore {
    int size;
    final Map<String, Integer> columnNameToIndexMap = new LinkedHashMap<>();
    final List<String>[] columns;

    /**
     *
     * @param columnNames
     */
    public RawDataStore(@NotNull String[] columnNames) {
        columns = new List[columnNames.length];
        for (int index = 0; index < columnNames.length; index++) {
            columnNameToIndexMap.putIfAbsent(columnNames[index], columnNameToIndexMap.size());
            columns[index] = new ArrayList<String>();
        }
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
