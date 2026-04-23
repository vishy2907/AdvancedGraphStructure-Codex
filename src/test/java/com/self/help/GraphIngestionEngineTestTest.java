package com.self.help;

import org.junit.jupiter.api.Test;

import java.util.List;

class GraphIngestionEngineTestTest {

    @Test
    public void testGraphIngestionEngine() {
        RawDataStore store = new RawDataStore(List.of("fromCity", "fromArea", "toCity", "toArea", "medium"));
        store.ingestRow(new String[]{"Mumbai", null, "Pune", null, "byRoad"});
        store.ingestRow(new String[]{"Nasik", null, "Pune", null, "byRoad"});
        store.ingestRow(new String[]{"Pune", null, "Solapur", null, "byRoad"});

        NodeSpec fromCity = new NodeSpec("fromCity", null, null);
        NodeSpec toCity = new NodeSpec("toCity", null, null);

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        for (int i = 0; i < store.getSize(); i++) {
            engine.ingest(i, store);
        }
        System.out.println(engine);
    }

}