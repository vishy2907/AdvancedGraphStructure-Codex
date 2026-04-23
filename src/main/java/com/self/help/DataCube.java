package com.self.help;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class DataCube {
    int size;
    final Map<String, Integer> columnNameToIndexMap = new HashMap<>();
    final List<String>[] columnarStores;
    /**
     *
     * @param columnNames
     */
    public DataCube(@NotNull String[] columnNames) {
        columnarStores = new List[columnNames.length];
        for (int index = 0; index < columnNames.length; index++) {
            columnNameToIndexMap.putIfAbsent(columnNames[index], columnNameToIndexMap.size());
            columnarStores[index] = new ArrayList<String>();
        }
    }

    public int ingestRow(@NotNull String[] values) {
        if(values.length != columnNameToIndexMap.size()) {
            throw new IllegalArgumentException();
        }

        for (int index = 0; index < values.length; index++) {
            columnarStores[index].add(values[index]);
        }

        return size++;
    }

}
