package com.self.help;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.IntIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public void testInvertedIndexColumnNullifiesEmptyBitmapAfterRemove() {
        InvertedIndexColumn indexColumn = new InvertedIndexColumn();

        indexColumn.add(3, 99);
        indexColumn.remove(3, 99);

        assertNull(indexColumn.getRowsForValueOrNull(3));
    }
}
