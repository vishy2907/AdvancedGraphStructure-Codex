package com.self.help;

import java.util.Collections;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GraphIngestionEngine {

    // ==========================================
    // PHASE 1: PRE-COMPUTED DATACUBE INDICES
    // ==========================================
    private final int fromIdCubeIndex;
    private final int toIdCubeIndex;
    private final int fromLabelCubeIndex; // Falls back to ID if null
    private final int toLabelCubeIndex;   // Falls back to ID if null
    private final int[] fromAttrCubeIndices;
    private final int[] toAttrCubeIndices;
    private final int[] relationCubeIndices;

    // ==========================================
    // PHASE 2: DETERMINISTIC REGISTRY OFFSETS
    // ==========================================
    private final int idDictOffset;
    private final int labelDictOffset;
    private final int attrDictBaseOffset;
    private final int relDictBaseOffset;

    // ==========================================
    // PHASE 3: PARALLEL REGISTRIES (ZERO WASTE)
    // ==========================================
    // Shared Dictionary for consistent String -> Integer translation
    private final BiDirectionalDictionary[] dictionaryRegistry;

    // Split Inverted Indices for Graph Query Engine
    private final InvertedIndexColumn[] fromInvertedIndexRegistry;
    private final InvertedIndexColumn[] toInvertedIndexRegistry;
    private final InvertedIndexColumn[] relationInvertedIndexRegistry;


    private final NumericRawDataStore numericRawDataStore;
    public GraphIngestionEngine(RawDataStore dataCube, MappingSpec spec) {

        // --- 0. THE FAIL-FAST VALIDATION GATE ---
        validateSpec(dataCube, spec);

        NodeSpec fromSpec = spec.getFromNodeSpec();
        NodeSpec toSpec = spec.getToNodeSpec();

        // --- 1. DATACUBE RESOLUTION & LABEL FALLBACK ---
        this.fromIdCubeIndex = dataCube.getColumnIndex(fromSpec.getIdColumnName());
        this.toIdCubeIndex = dataCube.getColumnIndex(toSpec.getIdColumnName());

        this.fromLabelCubeIndex = dataCube.getColumnIndex(fromSpec.getLabelColumnName());
        this.toLabelCubeIndex = dataCube.getColumnIndex(toSpec.getLabelColumnName());

        List<String> fromAttrs = fromSpec.getNodeAttributeNames() == null ? Collections.emptyList() : fromSpec.getNodeAttributeNames();
        List<String> toAttrs = toSpec.getNodeAttributeNames() == null ? Collections.emptyList() : toSpec.getNodeAttributeNames();

        int attrSize = fromAttrs.size();
        this.fromAttrCubeIndices = new int[attrSize];
        this.toAttrCubeIndices = new int[attrSize];
        for (int i = 0; i < attrSize; i++) {
            this.fromAttrCubeIndices[i] = dataCube.getColumnIndex(fromAttrs.get(i));
            this.toAttrCubeIndices[i] = dataCube.getColumnIndex(toAttrs.get(i));
        }

        List<String> relations = spec.getRelationColumnNames();
        int relSize = relations.size();
        this.relationCubeIndices = new int[relSize];
        for (int i = 0; i < relSize; i++) {
            this.relationCubeIndices[i] = dataCube.getColumnIndex(relations.get(i));
        }

        // --- 2. DETERMINISTIC OFFSET CALCULUS (ZERO WASTE) ---
        int currentDictOffset = 0;
        int nodePropertyCount = 0;

        this.idDictOffset = currentDictOffset++;
        nodePropertyCount++;

        this.labelDictOffset = currentDictOffset++;
        nodePropertyCount++;

        this.attrDictBaseOffset = currentDictOffset;
        currentDictOffset += attrSize;
        nodePropertyCount += attrSize;

        this.relDictBaseOffset = currentDictOffset;
        currentDictOffset += relSize;

        // --- 3. PARALLEL REGISTRY ALLOCATION ---
        this.dictionaryRegistry = new BiDirectionalDictionary[currentDictOffset];

        this.fromInvertedIndexRegistry = new InvertedIndexColumn[nodePropertyCount];
        this.toInvertedIndexRegistry = new InvertedIndexColumn[nodePropertyCount];
        this.relationInvertedIndexRegistry = new InvertedIndexColumn[relSize];

        for (int i = 0; i < currentDictOffset; i++) {
            this.dictionaryRegistry[i] = new BiDirectionalDictionary();
        }

        for (int i = 0; i < nodePropertyCount; i++) {
            this.fromInvertedIndexRegistry[i] = new InvertedIndexColumn();
            this.toInvertedIndexRegistry[i] = new InvertedIndexColumn();
        }

        for (int i = 0; i < relSize; i++) {
            this.relationInvertedIndexRegistry[i] = new InvertedIndexColumn();
        }

        numericRawDataStore = new NumericRawDataStore(dataCube.getColumnNames());
    }

    // ==========================================
    // ISOLATED VALIDATION ENGINE
    // ==========================================
    private static void validateSpec(RawDataStore dataCube, MappingSpec spec) {

        NodeSpec fromSpec = spec.getFromNodeSpec();
        NodeSpec toSpec = spec.getToNodeSpec();

        List<String> fromAttrs = fromSpec.getNodeAttributeNames() == null ? Collections.emptyList() : fromSpec.getNodeAttributeNames();
        List<String> toAttrs = toSpec.getNodeAttributeNames() == null ? Collections.emptyList() : toSpec.getNodeAttributeNames();

        if (fromAttrs.size() != toAttrs.size()) {
            throw new IllegalArgumentException("Attribute size mismatch: fromNodeSpec has " + fromAttrs.size() + " attributes, but toNodeSpec has " + toAttrs.size());
        }

        Set<String> fromCore = new HashSet<>();
        fromCore.add(fromSpec.getIdColumnName());
        fromCore.add(fromSpec.getLabelColumnName());

        Set<String> fromAttrSet = new HashSet<>(fromAttrs);
        Set<String> fromIntraOverlap = new HashSet<>(fromCore);
        fromIntraOverlap.retainAll(fromAttrSet);
        if (!fromIntraOverlap.isEmpty()) throw new IllegalArgumentException("Intra-node violation in fromNodeSpec. Overlap: " + fromIntraOverlap);

        Set<String> toCore = new HashSet<>();
        toCore.add(toSpec.getIdColumnName());
        toCore.add(toSpec.getLabelColumnName());

        Set<String> toAttrSet = new HashSet<>(toAttrs);
        Set<String> toIntraOverlap = new HashSet<>(toCore);
        toIntraOverlap.retainAll(toAttrSet);
        if (!toIntraOverlap.isEmpty()) throw new IllegalArgumentException("Intra-node violation in toNodeSpec. Overlap: " + toIntraOverlap);

        Set<String> fromAllCols = new HashSet<>(fromCore);
        fromAllCols.addAll(fromAttrSet);
        Set<String> toAllCols = new HashSet<>(toCore);
        toAllCols.addAll(toAttrSet);

        Set<String> crossOverlap = new HashSet<>(fromAllCols);
        crossOverlap.retainAll(toAllCols);
        if (!crossOverlap.isEmpty()) throw new IllegalArgumentException("Disjoint violation: fromNodeSpec and toNodeSpec share columns: " + crossOverlap);

        verifyColumnExists(dataCube, fromSpec.getIdColumnName());
        verifyColumnExists(dataCube, toSpec.getIdColumnName());
        verifyColumnExists(dataCube, fromSpec.getLabelColumnName());
        verifyColumnExists(dataCube, toSpec.getLabelColumnName());
        for (String attr : fromAttrs) verifyColumnExists(dataCube, attr);
        for (String attr : toAttrs) verifyColumnExists(dataCube, attr);
        for (String rel : spec.getRelationColumnNames()) verifyColumnExists(dataCube, rel);
    }

    private static void verifyColumnExists(RawDataStore dataCube, String columnName) {
        if (columnName == null || columnName.trim().isEmpty() || dataCube.getColumnIndex(columnName) == -1) {
            throw new IllegalArgumentException("Mapping violation: Column '" + columnName + "' does not exist in the DataCube.");
        }
    }

    // ==========================================
    // THE HOT LOOP: ZERO BRANCHES, ZERO ALLOCATIONS
    // ==========================================
    public synchronized void ingest(int rowId, RawDataStore dataCube) {
        int[] numericRowBuffer = new int[dataCube.getColumnNames().size()];
        Arrays.fill(numericRowBuffer, -1);

        // --- 1. ID Encoding & Indexing ---
        String fromIdStr = dataCube.getString(rowId, this.fromIdCubeIndex);
        String toIdStr = dataCube.getString(rowId, this.toIdCubeIndex);

        int encodedFromId = this.dictionaryRegistry[this.idDictOffset].getOrEncode(fromIdStr);
        int encodedToId = this.dictionaryRegistry[this.idDictOffset].getOrEncode(toIdStr);
        numericRowBuffer[this.fromIdCubeIndex] = encodedFromId;
        numericRowBuffer[this.toIdCubeIndex] = encodedToId;

        this.fromInvertedIndexRegistry[0].add(encodedFromId, rowId);
        this.toInvertedIndexRegistry[0].add(encodedToId, rowId);

        // --- 2. Label Encoding & Indexing ---
        String fromLabelStr = dataCube.getString(rowId, this.fromLabelCubeIndex);
        String toLabelStr = dataCube.getString(rowId, this.toLabelCubeIndex);

        int encodedFromLabel = this.dictionaryRegistry[this.labelDictOffset].getOrEncode(fromLabelStr);
        int encodedToLabel = this.dictionaryRegistry[this.labelDictOffset].getOrEncode(toLabelStr);
        numericRowBuffer[this.fromLabelCubeIndex] = encodedFromLabel;
        numericRowBuffer[this.toLabelCubeIndex] = encodedToLabel;

        this.fromInvertedIndexRegistry[1].add(encodedFromLabel, rowId);
        this.toInvertedIndexRegistry[1].add(encodedToLabel, rowId);

        // --- 3. Vectorized Attribute Encoding ---
        for (int i = 0; i < this.fromAttrCubeIndices.length; i++) {
            int dictAddress = this.attrDictBaseOffset + i;
            int nodeIndexAddress = 2 + i; // Offset past ID (0) and Label (1)

            // From Side
            String fromAttrStr = dataCube.getString(rowId, this.fromAttrCubeIndices[i]);
            int encodedFromAttr = this.dictionaryRegistry[dictAddress].getOrEncode(fromAttrStr);
            numericRowBuffer[this.fromAttrCubeIndices[i]] = encodedFromAttr;
            this.fromInvertedIndexRegistry[nodeIndexAddress].add(encodedFromAttr, encodedFromId);

            // To Side
            String toAttrStr = dataCube.getString(rowId, this.toAttrCubeIndices[i]);
            int encodedToAttr = this.dictionaryRegistry[dictAddress].getOrEncode(toAttrStr);
            numericRowBuffer[this.toAttrCubeIndices[i]] = encodedToAttr;
            this.toInvertedIndexRegistry[nodeIndexAddress].add(encodedToAttr, rowId);
        }

        // --- 4. Vectorized Relation Encoding ---
        for (int j = 0; j < this.relationCubeIndices.length; j++) {
            int dictAddress = this.relDictBaseOffset + j;

            String relStr = dataCube.getString(rowId, this.relationCubeIndices[j]);
            int encodedRel = this.dictionaryRegistry[dictAddress].getOrEncode(relStr);
            numericRowBuffer[this.relationCubeIndices[j]] = encodedRel;

            // Direct array access 'j' for relations (Zero waste array alignment)
            this.relationInvertedIndexRegistry[j].add(encodedRel, rowId);
        }

        this.numericRawDataStore.ingestRow(numericRowBuffer);
    }
}
