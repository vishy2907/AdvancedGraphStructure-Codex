package com.self.help;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.IntIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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

        GraphEngineIterator iterator = engine.iterator();
        List<String> actualRows = new ArrayList<>();
        while (iterator.hasNext()) {
            String row = iterator.nextRowAsString();
            System.out.println(row);
            actualRows.add(row);
        }

        assertEquals(expectedRows, actualRows);
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
