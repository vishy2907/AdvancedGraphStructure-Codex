package com.self.help;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.IntIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GraphIngestionEngineTestTest {

    private static List<String> collectValidRows(GraphIngestionEngine engine) {
        List<String> validRows = new ArrayList<>();
        IntIterator rowIds = engine.getValidRowIds();
        while (rowIds.hasNext()) {
            validRows.add(Arrays.toString(engine.getRow(rowIds.next())));
        }
        return validRows;
    }

    @Test
    public void testGraphIngestionEngineReturnsAllValidRowsIncludingDuplicates() {
        RawDataStore store = new RawDataStore(List.of("fromCity", "fromArea", "toCity", "toArea", "medium"));
        store.ingestRow(new String[]{"Mumbai", null, "Pune", null, "byRoad"});
        store.ingestRow(new String[]{"Mumbai", null, "Pune", null, "byRoad"});

        NodeSpec fromCity = new NodeSpec("fromCity", null, null);
        NodeSpec toCity = new NodeSpec("toCity", null, null);

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        for (int i = 0; i < store.getSize(); i++) {
            engine.ingest(i, store);
        }

        assertEquals(
                List.of("[Mumbai, Pune, byRoad]", "[Mumbai, Pune, byRoad]"),
                collectValidRows(engine)
        );
    }

    @Test
    public void testGraphIngestionEngineReturnsNullsForDeletedFromNodeAndRelations() {
        RawDataStore store = new RawDataStore(List.of("fromCity", "fromArea", "toCity", "toArea", "medium"));
        store.ingestRow(new String[]{"Mumbai", "kurla", "Pune", "katraj", "byRoad"});

        NodeSpec fromCity = new NodeSpec("fromCity", null, List.of("fromArea"));
        NodeSpec toCity = new NodeSpec("toCity", null, List.of("toArea"));

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        engine.ingest(0, store);
        engine.markDeletedFrom(0);

        assertEquals(List.of("[null, null, Pune, katraj, null]"), collectValidRows(engine));
    }

    @Test
    public void testProjectedDuplicatesAreReturnedAfterPartialDeletion() {
        RawDataStore store = new RawDataStore(List.of("fromCity", "fromArea", "toCity", "toArea", "medium"));
        store.ingestRow(new String[]{"Mumbai", "kurla", "Pune", "katraj", "byRoad"});
        store.ingestRow(new String[]{"Mumbai", "kurla", "Pune", "katraj", "byRoad"});

        NodeSpec fromCity = new NodeSpec("fromCity", null, List.of("fromArea"));
        NodeSpec toCity = new NodeSpec("toCity", null, List.of("toArea"));

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        engine.ingest(0, store);
        engine.ingest(1, store);
        engine.markDeletedFrom(0);
        engine.markDeletedFrom(1);

        assertEquals(
                List.of("[null, null, Pune, katraj, null]", "[null, null, Pune, katraj, null]"),
                collectValidRows(engine)
        );
    }

    @Test
    public void testProjectedRowsRemainDistinctWhenContentsDifferAfterDeletion() {
        RawDataStore store = new RawDataStore(List.of("fromCity", "fromArea", "toCity", "toArea", "medium"));
        store.ingestRow(new String[]{"Mumbai", "kurla", "Pune", "katraj", "byRoad"});
        store.ingestRow(new String[]{"Mumbai", "kurla", "Pune", "katraj", "byRoad"});

        NodeSpec fromCity = new NodeSpec("fromCity", null, List.of("fromArea"));
        NodeSpec toCity = new NodeSpec("toCity", null, List.of("toArea"));

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        engine.ingest(0, store);
        engine.markDeletedFrom(0);
        engine.ingest(1, store);

        assertEquals(
                List.of("[null, null, Pune, katraj, null]", "[Mumbai, kurla, Pune, katraj, byRoad]"),
                collectValidRows(engine)
        );
    }

    @Test
    public void testGraphEngineIteratorSkipsRowsDeletedOnBothSides() {
        RawDataStore store = new RawDataStore(List.of("fromCity", "toCity", "medium"));
        store.ingestRow(new String[]{"Mumbai", "Pune", "byRoad"});
        store.ingestRow(new String[]{"Pune", "Nashik", "byTrain"});

        NodeSpec fromCity = new NodeSpec("fromCity", null, null);
        NodeSpec toCity = new NodeSpec("toCity", null, null);

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        for (int i = 0; i < store.getSize(); i++) {
            engine.ingest(i, store);
        }
        engine.markDeletedFrom(0);
        engine.markDeletedTo(0);

        GraphEngineIterator iterator = engine.graphIterator();
        List<String> rows = new ArrayList<>();
        while (iterator.hasNext()) {
            rows.add(iterator.nextRowAsString());
        }

        assertEquals(List.of("[Pune, Pune, Nashik, Nashik, byTrain]"), rows);
        assertEquals(List.of("[Pune, Nashik, byTrain]"), collectValidRows(engine));
    }

    @Test
    public void testGraphEngineIteratorReturnsAllRowsWhenNothingIsDeleted() {
        RawDataStore store = new RawDataStore(List.of("fromCity", "toCity", "medium"));
        List<String> expectedRows = new ArrayList<>();
        List<String[]> inputRows = new ArrayList<>();

        inputRows.add(new String[]{"Mumbai", "Nashik", "byRoad"});
        inputRows.add(new String[]{"Nashik", "Dhule", "byRoad"});
        inputRows.add(new String[]{"Dhule", "Jalgaon", "byRoad"});
        inputRows.add(new String[]{"Mumbai", "Pune", "byTrain"});
        inputRows.add(new String[]{"Pune", "Satara", "byRoad"});
        inputRows.add(new String[]{"Satara", "Kolhapur", "byRoad"});
        inputRows.add(new String[]{"Kolhapur", "Belagavi", "byRoad"});
        inputRows.add(new String[]{"Pune", "Ahmednagar", "byRoad"});
        inputRows.add(new String[]{"Ahmednagar", "Aurangabad", "byRoad"});
        inputRows.add(new  String[]{"Aurangabad", "Jalna", "byRoad"});
        inputRows.add(new  String[]{"Jalna", "Nanded", "byRoad"});
        inputRows.add(new  String[]{"Nanded", "Yavatmal", "byRoad"});
        inputRows.add(new  String[]{"Yavatmal", "Wardha", "byRoad"});
        inputRows.add(new  String[]{"Wardha", "Nagpur", "byTrain"});

        inputRows.forEach(row -> {
            store.ingestRow(row);
            expectedRows.add(toIteratorString(row));
        });

        NodeSpec fromCity = new NodeSpec("fromCity", null, null);
        NodeSpec toCity = new NodeSpec("toCity", null, null);

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        for (int i = 0; i < store.getSize(); i++) {
            engine.ingest(i, store);
        }

        GraphEngineIterator iterator = engine.graphIterator();
        List<String> actualRows = new ArrayList<>();
        while (iterator.hasNext()) {
            String row = iterator.nextRowAsString();
            System.out.println(row);
            actualRows.add(row);
        }

        assertEquals(expectedRows, actualRows);
    }

    @Test
    public void testGraphEngineIteratorBuildsGraphStatisticsWithBreadthFirstTraversal() {
        RawDataStore store = new RawDataStore(List.of("fromCity", "toCity", "medium"));
        store.ingestRow(new String[]{"Mumbai", "Nashik", "byRoad"});
        store.ingestRow(new String[]{"Mumbai", "Pune", "byTrain"});
        store.ingestRow(new String[]{"Nashik", "Dhule", "byRoad"});
        store.ingestRow(new String[]{"Pune", "Satara", "byRoad"});
        store.ingestRow(new String[]{"Satara", "Kolhapur", "byRoad"});

        NodeSpec fromCity = new NodeSpec("fromCity", null, null);
        NodeSpec toCity = new NodeSpec("toCity", null, null);

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        for (int i = 0; i < store.getSize(); i++) {
            engine.ingest(i, store);
        }

        Map<String, GraphNodeStat> statistics = engine.graphIterator().getGraphStatistics();

        assertEquals(new GraphNodeStat(2, 0), statistics.get("Mumbai"));
        assertEquals(new GraphNodeStat(1, 1), statistics.get("Nashik"));
        assertEquals(new GraphNodeStat(1, 1), statistics.get("Pune"));
        assertEquals(new GraphNodeStat(0, 1), statistics.get("Dhule"));
        assertEquals(new GraphNodeStat(1, 1), statistics.get("Satara"));
        assertEquals(new GraphNodeStat(0, 1), statistics.get("Kolhapur"));
    }

    @Test
    public void testGraphEngineIteratorKeepsDifferentNodeIdsWithSameLabelDistinct() {
        RawDataStore store = new RawDataStore(List.of("fromCityId", "fromCityLabel", "toCityId", "toCityLabel", "medium"));
        store.ingestRow(new String[]{"PUNE-1", "Pune", "MUM-1", "Mumbai", "byRoad"});
        store.ingestRow(new String[]{"PUNE-2", "Pune", "SAT-1", "Satara", "byRoad"});
        store.ingestRow(new String[]{"MUM-1", "Mumbai", "PUNE-2", "Pune", "byTrain"});

        NodeSpec fromCity = new NodeSpec("fromCityId", "fromCityLabel", null);
        NodeSpec toCity = new NodeSpec("toCityId", "toCityLabel", null);

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        for (int i = 0; i < store.getSize(); i++) {
            engine.ingest(i, store);
        }

        GraphEngineIterator iterator = engine.graphIterator();
        List<String> rows = new ArrayList<>();
        while (iterator.hasNext()) {
            rows.add(iterator.nextRowAsString());
        }

        assertEquals(
                List.of(
                        "[PUNE-1, Pune, MUM-1, Mumbai, byRoad]",
                        "[PUNE-2, Pune, SAT-1, Satara, byRoad]",
                        "[MUM-1, Mumbai, PUNE-2, Pune, byTrain]"
                ),
                rows
        );

        Map<String, GraphNodeStat> statistics = engine.graphIterator().getGraphStatistics();
        assertEquals(4, statistics.size());
        assertEquals(new GraphNodeStat(1, 0), statistics.get("PUNE-1"));
        assertEquals(new GraphNodeStat(1, 1), statistics.get("PUNE-2"));
        assertEquals(new GraphNodeStat(1, 1), statistics.get("MUM-1"));
        assertEquals(new GraphNodeStat(0, 1), statistics.get("SAT-1"));
        assertNull(statistics.get("City"));
    }

    @Test
    public void testNumericalRowStoreStoresEncodedRowsColumnWise() {
        NumericalRowStore store = new NumericalRowStore(3, 1);

        store.appendRow(0, new int[]{10, 20, 30});
        store.appendRow(1, new int[]{11, 21, 31});

        assertEquals(3, store.getColumnCount());
        assertEquals(2, store.getRowCount());
        assertEquals(21, store.getInt(1, 1));
        assertArrayEquals(new int[]{10, 20, 30}, store.getRow(0));
        assertThrows(IllegalArgumentException.class, () -> store.appendRow(3, new int[]{12, 22, 32}));
    }

    @Test
    public void testGraphIngestionEnginePersistsEncodedRowsInNumericalRowStore() {
        RawDataStore store = new RawDataStore(List.of("fromCity", "toCity", "medium"));
        store.ingestRow(new String[]{"Mumbai", "Pune", "byRoad"});
        store.ingestRow(new String[]{"Pune", "Nashik", "byTrain"});

        NodeSpec fromCity = new NodeSpec("fromCity", null, null);
        NodeSpec toCity = new NodeSpec("toCity", null, null);

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        for (int i = 0; i < store.getSize(); i++) {
            engine.ingest(i, store);
        }

        NumericalRowStore numericalRowStore = engine.getNumericalRowStore();

        assertEquals(5, numericalRowStore.getColumnCount());
        assertEquals(2, numericalRowStore.getRowCount());
        assertArrayEquals(new int[]{0, 1, 0, 1, 0}, numericalRowStore.getRow(0));
        assertArrayEquals(new int[]{1, 2, 1, 2, 1}, numericalRowStore.getRow(1));
    }

    @Test
    public void testGraphEngineDecodesRowsFromNumericalStoreWhenDefaultAndExplicitLabelsAreMixed() {
        RawDataStore store = new RawDataStore(List.of("fromCityId", "toCityId", "toCityLabel", "medium"));
        store.ingestRow(new String[]{"PUNE-1", "MUM-1", "Mumbai", "byRoad"});
        store.ingestRow(new String[]{"PUNE-2", "SAT-1", "Satara", "byTrain"});

        NodeSpec fromCity = new NodeSpec("fromCityId", null, null);
        NodeSpec toCity = new NodeSpec("toCityId", "toCityLabel", null);

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        for (int i = 0; i < store.getSize(); i++) {
            engine.ingest(i, store);
        }

        GraphEngineIterator iterator = engine.graphIterator();
        List<String> rows = new ArrayList<>();
        while (iterator.hasNext()) {
            rows.add(iterator.nextRowAsString());
        }

        assertEquals(
                List.of(
                        "[PUNE-1, PUNE-1, MUM-1, Mumbai, byRoad]",
                        "[PUNE-2, PUNE-2, SAT-1, Satara, byTrain]"
                ),
                rows
        );
        assertEquals(
                List.of("[PUNE-1, MUM-1, Mumbai, byRoad]", "[PUNE-2, SAT-1, Satara, byTrain]"),
                collectValidRows(engine)
        );
    }

    @Test
    public void testInvertedIndexColumnNullifiesEmptyBitmapAfterRemove() {
        InvertedIndexColumn indexColumn = new InvertedIndexColumn();

        indexColumn.add(3, 99);
        indexColumn.remove(3, 99);

        assertNull(indexColumn.getRowsForValueOrNull(3));
    }

    private String toIteratorString(String[] input) {
        return Arrays.toString(new String[]{input[0], input[0], input[1], input[1], input[2]});
    }
}
