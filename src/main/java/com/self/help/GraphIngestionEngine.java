package com.self.help;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphIngestionEngine {

    // ==========================================
    // PHASE 1: PRE-COMPUTED DATACUBE INDICES
    // ==========================================
    private final int fromIdCubeIndex;
    private final int toIdCubeIndex;
    private final int fromLabelCubeIndex;
    private final int toLabelCubeIndex;
    private final int[] fromAttrCubeIndices;
    private final int[] toAttrCubeIndices;
    private final int[] relationCubeIndices;

    // ==========================================
    // PHASE 1B: NUMERIC STORE INDICES
    // ==========================================
    private final int fromIdNumericIndex;
    private final int toIdNumericIndex;
    private final int fromLabelNumericIndex;
    private final int toLabelNumericIndex;
    private final int[] fromAttrNumericIndices;
    private final int[] toAttrNumericIndices;
    private final int[] relationNumericIndices;

    // ==========================================
    // PHASE 2: DETERMINISTIC REGISTRY OFFSETS
    // ==========================================
    private final int idDictOffset;
    private final int labelDictOffset;
    private final int attrDictBaseOffset;
    private final int relDictBaseOffset;

    // ==========================================
    // PHASE 3: PARALLEL REGISTRIES
    // ==========================================
    private final BiDirectionalDictionary[] dictionaryRegistry;
    private final InvertedIndexColumn[] fromInvertedIndexRegistry;
    private final InvertedIndexColumn[] toInvertedIndexRegistry;
    private final InvertedIndexColumn[] relationInvertedIndexRegistry;

    private final NumericRawDataStore numericRawDataStore;

    public GraphIngestionEngine(RawDataStore dataCube, MappingSpec spec) {
        validateSpec(dataCube, spec);

        NodeSpec fromSpec = spec.getFromNodeSpec();
        NodeSpec toSpec = spec.getToNodeSpec();

        this.fromIdCubeIndex = dataCube.getColumnIndex(fromSpec.getIdColumnName());
        this.toIdCubeIndex = dataCube.getColumnIndex(toSpec.getIdColumnName());
        this.fromLabelCubeIndex = dataCube.getColumnIndex(fromSpec.getLabelColumnName());
        this.toLabelCubeIndex = dataCube.getColumnIndex(toSpec.getLabelColumnName());

        List<String> fromAttrs = fromSpec.getNodeAttributeNames() == null ? Collections.emptyList() : fromSpec.getNodeAttributeNames();
        List<String> toAttrs = toSpec.getNodeAttributeNames() == null ? Collections.emptyList() : toSpec.getNodeAttributeNames();
        List<String> relations = spec.getRelationColumnNames();

        int attrSize = fromAttrs.size();
        int relSize = relations.size();

        this.fromAttrCubeIndices = new int[attrSize];
        this.toAttrCubeIndices = new int[attrSize];
        for (int i = 0; i < attrSize; i++) {
            this.fromAttrCubeIndices[i] = dataCube.getColumnIndex(fromAttrs.get(i));
            this.toAttrCubeIndices[i] = dataCube.getColumnIndex(toAttrs.get(i));
        }

        this.relationCubeIndices = new int[relSize];
        for (int i = 0; i < relSize; i++) {
            this.relationCubeIndices[i] = dataCube.getColumnIndex(relations.get(i));
        }

        LinkedHashMap<String, Integer> numericColumnIndexMap = new LinkedHashMap<>();
        this.fromIdNumericIndex = registerNumericColumn(numericColumnIndexMap, fromSpec.getIdColumnName());
        this.toIdNumericIndex = registerNumericColumn(numericColumnIndexMap, toSpec.getIdColumnName());
        this.fromLabelNumericIndex = registerNumericColumn(numericColumnIndexMap, fromSpec.getLabelColumnName());
        this.toLabelNumericIndex = registerNumericColumn(numericColumnIndexMap, toSpec.getLabelColumnName());

        this.fromAttrNumericIndices = new int[attrSize];
        this.toAttrNumericIndices = new int[attrSize];
        for (int i = 0; i < attrSize; i++) {
            this.fromAttrNumericIndices[i] = registerNumericColumn(numericColumnIndexMap, fromAttrs.get(i));
            this.toAttrNumericIndices[i] = registerNumericColumn(numericColumnIndexMap, toAttrs.get(i));
        }

        this.relationNumericIndices = new int[relSize];
        for (int i = 0; i < relSize; i++) {
            this.relationNumericIndices[i] = registerNumericColumn(numericColumnIndexMap, relations.get(i));
        }

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

        this.numericRawDataStore = new NumericRawDataStore(List.copyOf(numericColumnIndexMap.keySet()));
    }

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
        if (!fromIntraOverlap.isEmpty()) {
            throw new IllegalArgumentException("Intra-node violation in fromNodeSpec. Overlap: " + fromIntraOverlap);
        }

        Set<String> toCore = new HashSet<>();
        toCore.add(toSpec.getIdColumnName());
        toCore.add(toSpec.getLabelColumnName());

        Set<String> toAttrSet = new HashSet<>(toAttrs);
        Set<String> toIntraOverlap = new HashSet<>(toCore);
        toIntraOverlap.retainAll(toAttrSet);
        if (!toIntraOverlap.isEmpty()) {
            throw new IllegalArgumentException("Intra-node violation in toNodeSpec. Overlap: " + toIntraOverlap);
        }

        Set<String> fromAllCols = new HashSet<>(fromCore);
        fromAllCols.addAll(fromAttrSet);
        Set<String> toAllCols = new HashSet<>(toCore);
        toAllCols.addAll(toAttrSet);

        Set<String> crossOverlap = new HashSet<>(fromAllCols);
        crossOverlap.retainAll(toAllCols);
        if (!crossOverlap.isEmpty()) {
            throw new IllegalArgumentException("Disjoint violation: fromNodeSpec and toNodeSpec share columns: " + crossOverlap);
        }

        verifyColumnExists(dataCube, fromSpec.getIdColumnName());
        verifyColumnExists(dataCube, toSpec.getIdColumnName());
        verifyColumnExists(dataCube, fromSpec.getLabelColumnName());
        verifyColumnExists(dataCube, toSpec.getLabelColumnName());
        for (String attr : fromAttrs) {
            verifyColumnExists(dataCube, attr);
        }
        for (String attr : toAttrs) {
            verifyColumnExists(dataCube, attr);
        }
        for (String rel : spec.getRelationColumnNames()) {
            verifyColumnExists(dataCube, rel);
        }
    }

    private static void verifyColumnExists(RawDataStore dataCube, String columnName) {
        if (columnName == null || columnName.trim().isEmpty() || dataCube.getColumnIndex(columnName) == -1) {
            throw new IllegalArgumentException("Mapping violation: Column '" + columnName + "' does not exist in the DataCube.");
        }
    }

    private static int registerNumericColumn(Map<String, Integer> numericColumnIndexMap, String columnName) {
        Integer existingIndex = numericColumnIndexMap.get(columnName);
        if (existingIndex != null) {
            return existingIndex;
        }
        int newIndex = numericColumnIndexMap.size();
        numericColumnIndexMap.put(columnName, newIndex);
        return newIndex;
    }

    public synchronized void ingest(int rowId, RawDataStore dataCube) {
        int[] numericRowBuffer = new int[this.numericRawDataStore.getColumns().length];
        Arrays.fill(numericRowBuffer, -1);

        String fromIdStr = dataCube.getString(rowId, this.fromIdCubeIndex);
        String toIdStr = dataCube.getString(rowId, this.toIdCubeIndex);
        int encodedFromId = this.dictionaryRegistry[this.idDictOffset].getOrEncode(fromIdStr);
        int encodedToId = this.dictionaryRegistry[this.idDictOffset].getOrEncode(toIdStr);
        numericRowBuffer[this.fromIdNumericIndex] = encodedFromId;
        numericRowBuffer[this.toIdNumericIndex] = encodedToId;

        String fromLabelStr = dataCube.getString(rowId, this.fromLabelCubeIndex);
        String toLabelStr = dataCube.getString(rowId, this.toLabelCubeIndex);
        int encodedFromLabel = this.dictionaryRegistry[this.labelDictOffset].getOrEncode(fromLabelStr);
        int encodedToLabel = this.dictionaryRegistry[this.labelDictOffset].getOrEncode(toLabelStr);
        numericRowBuffer[this.fromLabelNumericIndex] = encodedFromLabel;
        numericRowBuffer[this.toLabelNumericIndex] = encodedToLabel;

        for (int i = 0; i < this.fromAttrCubeIndices.length; i++) {
            int dictAddress = this.attrDictBaseOffset + i;

            String fromAttrStr = dataCube.getString(rowId, this.fromAttrCubeIndices[i]);
            int encodedFromAttr = this.dictionaryRegistry[dictAddress].getOrEncode(fromAttrStr);
            numericRowBuffer[this.fromAttrNumericIndices[i]] = encodedFromAttr;

            String toAttrStr = dataCube.getString(rowId, this.toAttrCubeIndices[i]);
            int encodedToAttr = this.dictionaryRegistry[dictAddress].getOrEncode(toAttrStr);
            numericRowBuffer[this.toAttrNumericIndices[i]] = encodedToAttr;
        }

        for (int j = 0; j < this.relationCubeIndices.length; j++) {
            int dictAddress = this.relDictBaseOffset + j;
            String relStr = dataCube.getString(rowId, this.relationCubeIndices[j]);
            int encodedRel = this.dictionaryRegistry[dictAddress].getOrEncode(relStr);
            numericRowBuffer[this.relationNumericIndices[j]] = encodedRel;
        }

        this.numericRawDataStore.ingestRow(numericRowBuffer);
        addIndexedRow(rowId, encodedFromId, encodedToId, encodedFromLabel, encodedToLabel, numericRowBuffer);
    }

    private void addIndexedRow(int rowId,
                               int encodedFromId,
                               int encodedToId,
                               int encodedFromLabel,
                               int encodedToLabel,
                               int[] numericRowBuffer) {
        this.fromInvertedIndexRegistry[0].add(encodedFromId, rowId);
        this.toInvertedIndexRegistry[0].add(encodedToId, rowId);
        this.fromInvertedIndexRegistry[1].add(encodedFromLabel, rowId);
        this.toInvertedIndexRegistry[1].add(encodedToLabel, rowId);

        for (int i = 0; i < this.fromAttrCubeIndices.length; i++) {
            int nodeIndexAddress = 2 + i;
            int encodedFromAttr = numericRowBuffer[this.fromAttrNumericIndices[i]];
            int encodedToAttr = numericRowBuffer[this.toAttrNumericIndices[i]];
            this.fromInvertedIndexRegistry[nodeIndexAddress].add(encodedFromAttr, rowId);
            this.toInvertedIndexRegistry[nodeIndexAddress].add(encodedToAttr, rowId);
        }

        for (int j = 0; j < this.relationCubeIndices.length; j++) {
            int encodedRel = numericRowBuffer[this.relationNumericIndices[j]];
            this.relationInvertedIndexRegistry[j].add(encodedRel, rowId);
        }
    }
}
