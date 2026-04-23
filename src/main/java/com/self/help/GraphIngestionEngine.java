package com.self.help;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.ArrayList;
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
    // PHASE 1B: ENCODED BUFFER INDICES
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

    private final int encodedColumnCount;
    private final RawDataStore sourceDataStore;
    private final RoaringBitmap deletedRowFrom = new RoaringBitmap();
    private final RoaringBitmap deletedRowTo = new RoaringBitmap();
    private int ingestedRowCount;

    public GraphIngestionEngine(RawDataStore dataCube, MappingSpec spec) {
        validateSpec(dataCube, spec);
        this.sourceDataStore = dataCube;

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

        this.encodedColumnCount = numericColumnIndexMap.size();
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
        if (rowId != this.ingestedRowCount) {
            throw new IllegalArgumentException("Row ids must be ingested sequentially starting at 0.");
        }
        int[] numericRowBuffer = encodeNumericRow(rowId, dataCube);
        int encodedFromId = numericRowBuffer[this.fromIdNumericIndex];
        int encodedToId = numericRowBuffer[this.toIdNumericIndex];
        int encodedFromLabel = numericRowBuffer[this.fromLabelNumericIndex];
        int encodedToLabel = numericRowBuffer[this.toLabelNumericIndex];
        addIndexedRow(rowId, encodedFromId, encodedToId, encodedFromLabel, encodedToLabel, numericRowBuffer);
        this.ingestedRowCount++;
    }

    private int[] encodeNumericRow(int rowId, RawDataStore dataCube) {
        int[] numericRowBuffer = new int[this.encodedColumnCount];
        Arrays.fill(numericRowBuffer, -1);

        encodeCoreValue(rowId, dataCube, this.fromIdCubeIndex, this.idDictOffset, numericRowBuffer, this.fromIdNumericIndex);
        encodeCoreValue(rowId, dataCube, this.toIdCubeIndex, this.idDictOffset, numericRowBuffer, this.toIdNumericIndex);
        encodeCoreValue(rowId, dataCube, this.fromLabelCubeIndex, this.labelDictOffset, numericRowBuffer, this.fromLabelNumericIndex);
        encodeCoreValue(rowId, dataCube, this.toLabelCubeIndex, this.labelDictOffset, numericRowBuffer, this.toLabelNumericIndex);

        for (int i = 0; i < this.fromAttrCubeIndices.length; i++) {
            int dictAddress = this.attrDictBaseOffset + i;
            encodeCoreValue(rowId, dataCube, this.fromAttrCubeIndices[i], dictAddress, numericRowBuffer, this.fromAttrNumericIndices[i]);
            encodeCoreValue(rowId, dataCube, this.toAttrCubeIndices[i], dictAddress, numericRowBuffer, this.toAttrNumericIndices[i]);
        }

        for (int j = 0; j < this.relationCubeIndices.length; j++) {
            encodeCoreValue(rowId, dataCube, this.relationCubeIndices[j], this.relDictBaseOffset + j, numericRowBuffer, this.relationNumericIndices[j]);
        }

        return numericRowBuffer;
    }

    private void encodeCoreValue(int rowId,
                                 RawDataStore dataCube,
                                 int cubeIndex,
                                 int dictAddress,
                                 int[] numericRowBuffer,
                                 int numericIndex) {
        String rawValue = dataCube.getString(rowId, cubeIndex);
        int encodedValue = this.dictionaryRegistry[dictAddress].getOrEncode(rawValue);
        numericRowBuffer[numericIndex] = encodedValue;
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
            updateNodeAttributeIndex(i, numericRowBuffer, rowId);
        }

        for (int j = 0; j < this.relationCubeIndices.length; j++) {
            updateRelationIndex(j, numericRowBuffer, rowId);
        }
    }

    void markDeletedFrom(int rowId) {
        this.deletedRowFrom.add(rowId);
    }

    void markDeletedTo(int rowId) {
        this.deletedRowTo.add(rowId);
    }

    public synchronized IntIterator getValidRowIds() {
        RoaringBitmap validRowIds = new RoaringBitmap();

        for (int rowId = 0; rowId < this.ingestedRowCount; rowId++) {
            boolean fromDeleted = this.deletedRowFrom.contains(rowId);
            boolean toDeleted = this.deletedRowTo.contains(rowId);
            if (!(fromDeleted && toDeleted)) {
                validRowIds.add(rowId);
            }
        }

        return validRowIds.getIntIterator();
    }

    public synchronized String[] getRow(int rowId) {
        return getProjectedRowOrNull(rowId);
    }

    private String[] getProjectedRowOrNull(int rowId) {
        if (rowId < 0 || rowId >= this.ingestedRowCount) {
            return null;
        }

        boolean fromDeleted = this.deletedRowFrom.contains(rowId);
        boolean toDeleted = this.deletedRowTo.contains(rowId);
        if (fromDeleted && toDeleted) {
            return null;
        }

        return buildMappedRow(rowId, fromDeleted, toDeleted);
    }

    private String[] buildMappedRow(int rowId, boolean fromDeleted, boolean toDeleted) {
        List<String> mappedRow = new ArrayList<>(getMappedColumnCount());
        appendNodeValues(mappedRow, rowId, fromDeleted, this.fromIdCubeIndex, this.fromLabelCubeIndex, this.fromAttrCubeIndices);
        appendNodeValues(mappedRow, rowId, toDeleted, this.toIdCubeIndex, this.toLabelCubeIndex, this.toAttrCubeIndices);

        boolean relationDeleted = fromDeleted || toDeleted;
        for (int relationCubeIndex : this.relationCubeIndices) {
            mappedRow.add(relationDeleted ? null : this.sourceDataStore.getString(rowId, relationCubeIndex));
        }

        return mappedRow.toArray(new String[0]);
    }

    private int getMappedColumnCount() {
        return getNodeColumnCount(this.fromIdCubeIndex, this.fromLabelCubeIndex, this.fromAttrCubeIndices)
                + getNodeColumnCount(this.toIdCubeIndex, this.toLabelCubeIndex, this.toAttrCubeIndices)
                + this.relationCubeIndices.length;
    }

    private int getNodeColumnCount(int idCubeIndex, int labelCubeIndex, int[] attrCubeIndices) {
        int labelColumnCount = idCubeIndex == labelCubeIndex ? 0 : 1;
        return 1 + labelColumnCount + attrCubeIndices.length;
    }

    private void appendNodeValues(List<String> mappedRow,
                                  int rowId,
                                  boolean nodeDeleted,
                                  int idCubeIndex,
                                  int labelCubeIndex,
                                  int[] attrCubeIndices) {
        boolean hasDistinctLabel = idCubeIndex != labelCubeIndex;

        mappedRow.add(nodeDeleted ? null : this.sourceDataStore.getString(rowId, idCubeIndex));
        if (hasDistinctLabel) {
            mappedRow.add(nodeDeleted ? null : this.sourceDataStore.getString(rowId, labelCubeIndex));
        }

        for (int attrCubeIndex : attrCubeIndices) {
            mappedRow.add(nodeDeleted ? null : this.sourceDataStore.getString(rowId, attrCubeIndex));
        }
    }

    private void updateNodeAttributeIndex(int attributeIndex,
                                          int[] numericRowBuffer,
                                          int rowId) {
        int nodeIndexAddress = 2 + attributeIndex;
        int encodedFromAttr = numericRowBuffer[this.fromAttrNumericIndices[attributeIndex]];
        int encodedToAttr = numericRowBuffer[this.toAttrNumericIndices[attributeIndex]];
        updateIndex(this.fromInvertedIndexRegistry[nodeIndexAddress], encodedFromAttr, rowId);
        updateIndex(this.toInvertedIndexRegistry[nodeIndexAddress], encodedToAttr, rowId);
    }

    private void updateRelationIndex(int relationIndex, int[] numericRowBuffer, int rowId) {
        updateIndex(
                this.relationInvertedIndexRegistry[relationIndex],
                numericRowBuffer[this.relationNumericIndices[relationIndex]],
                rowId
        );
    }

    private static void updateIndex(InvertedIndexColumn indexColumn, int encodedValue, int rowId) {
        indexColumn.add(encodedValue, rowId);
    }
}
