package com.self.help;

import org.junit.jupiter.api.Test;

import java.util.List;

class GraphIngestionEngineTestTest {

    @Test
    public void testGraphIngestionEngine() {
        RawDataStore store = new RawDataStore(List.of("fromCity","toCity", "medium"));
        store.ingestRow(new String[]{"Mumbai", "Pune", "byRoad"});
        store.ingestRow(new String[]{"Nasik", "Pune", "byRoad"});
        store.ingestRow(new String[]{"Pune", "Solapur", "byRoad"});

        NodeSpec fromCity = new NodeSpec("fromCity", null, null);
        NodeSpec toCity = new NodeSpec("toCity", null, null);

        MappingSpec spec = new MappingSpec(fromCity, toCity, List.of("medium"));
        GraphIngestionEngine engine = new GraphIngestionEngine(store, spec);
        for(int i=0;i<store.getSize();i++) {
            engine.ingest( i, store);
        }
        System.out.println(engine);
    }

}