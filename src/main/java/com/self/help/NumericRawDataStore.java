package com.self.help;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.*;

@Data
public class NumericRawDataStore {
    int size;
    final Map<String, Integer> columnNameToIndexMap = new LinkedHashMap<>();
    final List<Integer>[] columns;

    /**
     *
     * @param columnNames
     */
    public NumericRawDataStore(@NotNull List<String> columnNames) {
        columns = new List[columnNames.size()];
        for (int index = 0; index < columnNames.size(); index++) {
            columnNameToIndexMap.putIfAbsent(columnNames.get(index), columnNameToIndexMap.size());
            columns[index] = new ArrayList<>();
        }
    }

    public synchronized int ingestRow(@NotNull int[] values) {
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
    public Integer[] getRow(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= size) {
            return null;
        }
        return Arrays.stream(columns).map(column -> column.get(rowIndex)).
                toList().
                toArray(new Integer[0]);
    }

    public int getColumnIndex(String columnName) {
        return columnNameToIndexMap.getOrDefault(columnName, -1);
    }
}
