package com.self.help;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Ingests rows from a raw column store into a graph-oriented index structure.
 * The engine treats each ingested row as an edge with a from-node side, a
 * to-node side, and optional relation columns. During ingestion it dictionary
 * encodes mapped values, stores them in a numerical sidecar, and updates
 * RoaringBitmap-backed inverted indexes.
 */
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
    // PHASE 2: HOT INDEX MAPPINGS
    // ==========================================
    private final int idDictionaryIndex;
    private final int labelDictionaryIndex;
    private final int[] attributeDictionaryIndices;
    private final int[] relationDictionaryIndices;

    // ==========================================
    // PHASE 3: ENGINE INDEX BLOCKS
    // ==========================================
    private final BiDirectionalDictionary[] dictionaryRegistry;
    private final NodeIndexBlock fromNodeIndexes;
    private final NodeIndexBlock toNodeIndexes;
    private final RelationIndexBlock relationIndexes;

    private final int encodedColumnCount;
    private final int mappedColumnCount;
    private final int iteratorMappedColumnCount;
    private final NumericalRowStore numericalRowStore;
    private final RoaringBitmap allRowIds = new RoaringBitmap();
    private final RoaringBitmap deletedRowFrom = new RoaringBitmap();
    private final RoaringBitmap deletedRowTo = new RoaringBitmap();
    private final RoaringBitmap fullyDeletedRowIds = new RoaringBitmap();
    private Map<String, GraphNodeStat> graphStatisticsCache = Collections.emptyMap();
    private int ingestedRowCount;
    private long graphVersion;
    private long graphStatisticsVersion = -1L;

    /**
     * Builds an ingestion engine for the supplied raw column store and mapping.
     * The mapping is validated up front, and all source column positions, index
     * mappings, and engine-owned registries are precomputed so row ingestion
     * does not need repeated column-name lookups.
     *
     * @param dataCube raw source store that owns the string columns
     * @param spec graph mapping that identifies from-node, to-node, and relation columns
     * @throws IllegalArgumentException when mapped columns are missing, attributes are mismatched,
     *                                  or from/to node specs share source columns
     */
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

        int currentNumericIndex = 0;
        this.fromIdNumericIndex = currentNumericIndex++;
        this.toIdNumericIndex = currentNumericIndex++;
        this.fromLabelNumericIndex = currentNumericIndex++;
        this.toLabelNumericIndex = currentNumericIndex++;

        this.fromAttrNumericIndices = new int[attrSize];
        this.toAttrNumericIndices = new int[attrSize];
        for (int i = 0; i < attrSize; i++) {
            this.fromAttrNumericIndices[i] = currentNumericIndex++;
            this.toAttrNumericIndices[i] = currentNumericIndex++;
        }

        this.relationNumericIndices = new int[relSize];
        for (int i = 0; i < relSize; i++) {
            this.relationNumericIndices[i] = currentNumericIndex++;
        }

        GraphIndexStore indexStore = new GraphIndexStore(attrSize, relSize);
        this.idDictionaryIndex = indexStore.idDictionaryIndex();
        this.labelDictionaryIndex = indexStore.labelDictionaryIndex();
        this.attributeDictionaryIndices = indexStore.attributeDictionaryIndices();
        this.relationDictionaryIndices = indexStore.relationDictionaryIndices();

        this.dictionaryRegistry = createDictionaryRegistry(indexStore.dictionaryCount());
        this.fromNodeIndexes = new NodeIndexBlock(attrSize);
        this.toNodeIndexes = new NodeIndexBlock(attrSize);
        this.relationIndexes = new RelationIndexBlock(relSize);
        this.encodedColumnCount = currentNumericIndex;
        this.mappedColumnCount = computeMappedColumnCount();
        this.iteratorMappedColumnCount = computeIteratorMappedColumnCount();
        this.numericalRowStore = new NumericalRowStore(this.encodedColumnCount);
    }

    private static BiDirectionalDictionary[] createDictionaryRegistry(int dictionaryCount) {
        BiDirectionalDictionary[] dictionaries = new BiDirectionalDictionary[dictionaryCount];
        for (int i = 0; i < dictionaries.length; i++) {
            dictionaries[i] = new BiDirectionalDictionary();
        }
        return dictionaries;
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

    /**
     * Encodes and indexes one raw row as a graph edge.
     * Rows must be ingested in strict ascending order starting at {@code 0};
     * the row id is used directly as the bitmap position for all indexes.
     *
     * @param rowId sequential row id to ingest
     * @param dataCube raw store containing the row values
     * @throws IllegalArgumentException when {@code rowId} is not the next expected row id
     */
    public synchronized void ingest(int rowId, RawDataStore dataCube) {
        if (rowId != this.ingestedRowCount) {
            throw new IllegalArgumentException("Row ids must be ingested sequentially starting at 0.");
        }
        int[] numericRowBuffer = encodeNumericRow(rowId, dataCube);
        this.numericalRowStore.appendRow(rowId, numericRowBuffer);
        addIndexedRow(rowId, numericRowBuffer);
        this.allRowIds.add(rowId);
        this.ingestedRowCount++;
        this.graphVersion++;
    }

    /**
     * Returns the encoded numerical sidecar store maintained during ingestion.
     * The store is row-id aligned with the raw data store and contains the same
     * encoded values used to build dictionaries and inverted indexes.
     *
     * @return numerical row store for encoded graph rows
     */
    public NumericalRowStore getNumericalRowStore() {
        return this.numericalRowStore;
    }

    private int[] encodeNumericRow(int rowId, RawDataStore dataCube) {
        int[] numericRowBuffer = new int[this.encodedColumnCount];
        Arrays.fill(numericRowBuffer, -1);

        numericRowBuffer[this.fromIdNumericIndex] = encodeValue(this.idDictionaryIndex, dataCube.getString(rowId, this.fromIdCubeIndex));
        numericRowBuffer[this.toIdNumericIndex] = encodeValue(this.idDictionaryIndex, dataCube.getString(rowId, this.toIdCubeIndex));
        numericRowBuffer[this.fromLabelNumericIndex] = encodeValue(this.labelDictionaryIndex, dataCube.getString(rowId, this.fromLabelCubeIndex));
        numericRowBuffer[this.toLabelNumericIndex] = encodeValue(this.labelDictionaryIndex, dataCube.getString(rowId, this.toLabelCubeIndex));

        for (int i = 0; i < this.fromAttrCubeIndices.length; i++) {
            int dictionaryIndex = this.attributeDictionaryIndices[i];
            numericRowBuffer[this.fromAttrNumericIndices[i]] = encodeValue(dictionaryIndex, dataCube.getString(rowId, this.fromAttrCubeIndices[i]));
            numericRowBuffer[this.toAttrNumericIndices[i]] = encodeValue(dictionaryIndex, dataCube.getString(rowId, this.toAttrCubeIndices[i]));
        }

        for (int j = 0; j < this.relationCubeIndices.length; j++) {
            numericRowBuffer[this.relationNumericIndices[j]] = encodeValue(this.relationDictionaryIndices[j], dataCube.getString(rowId, this.relationCubeIndices[j]));
        }

        return numericRowBuffer;
    }

    private int encodeValue(int dictionaryIndex, String rawValue) {
        return this.dictionaryRegistry[dictionaryIndex].getOrEncode(rawValue);
    }

    private void addIndexedRow(int rowId, int[] numericRowBuffer) {
        this.fromNodeIndexes.idIndex().add(numericRowBuffer[this.fromIdNumericIndex], rowId);
        this.toNodeIndexes.idIndex().add(numericRowBuffer[this.toIdNumericIndex], rowId);
        this.fromNodeIndexes.labelIndex().add(numericRowBuffer[this.fromLabelNumericIndex], rowId);
        this.toNodeIndexes.labelIndex().add(numericRowBuffer[this.toLabelNumericIndex], rowId);

        for (int i = 0; i < this.fromAttrNumericIndices.length; i++) {
            this.fromNodeIndexes.attributeIndex(i).add(numericRowBuffer[this.fromAttrNumericIndices[i]], rowId);
            this.toNodeIndexes.attributeIndex(i).add(numericRowBuffer[this.toAttrNumericIndices[i]], rowId);
        }

        for (int i = 0; i < this.relationNumericIndices.length; i++) {
            this.relationIndexes.relationIndex(i).add(numericRowBuffer[this.relationNumericIndices[i]], rowId);
        }
    }

    synchronized void markDeletedFrom(int rowId) {
        boolean alreadyDeleted = this.deletedRowFrom.contains(rowId);
        this.deletedRowFrom.add(rowId);
        updateFullyDeletedRow(rowId);
        if (!alreadyDeleted) {
            this.graphVersion++;
        }
    }

    synchronized void markDeletedTo(int rowId) {
        boolean alreadyDeleted = this.deletedRowTo.contains(rowId);
        this.deletedRowTo.add(rowId);
        updateFullyDeletedRow(rowId);
        if (!alreadyDeleted) {
            this.graphVersion++;
        }
    }

    private void updateFullyDeletedRow(int rowId) {
        if (this.deletedRowFrom.contains(rowId) && this.deletedRowTo.contains(rowId)) {
            this.fullyDeletedRowIds.add(rowId);
            return;
        }

        this.fullyDeletedRowIds.remove(rowId);
    }

    /**
     * Returns row ids that are still visible after tombstone projection.
     * A row remains valid when at least one side of the edge is not deleted;
     * rows deleted on both the from and to side are omitted.
     *
     * @return iterator over valid row ids in ascending order
     */
    public synchronized IntIterator getValidRowIds() {
        RoaringBitmap validRowIds = this.allRowIds.clone();
        validRowIds.andNot(this.fullyDeletedRowIds);
        return validRowIds.getIntIterator();
    }

    /**
     * Creates a row iterator over the currently valid graph rows.
     * The returned iterator is backed by {@link #getValidRowIds()} and resolves
     * row ids through the iterator projection, so fully deleted rows are skipped
     * and each returned value always includes node id and node label columns.
     *
     * @return custom graph engine iterator over projected rows
     */
    public GraphEngineIterator graphIterator() {
        return new GraphEngineIterator(this);
    }

    /**
     * Returns the mapped graph row for the supplied row id.
     * Deleted node sides are projected as {@code null}; relation values are
     * projected as {@code null} when either node side of the edge is deleted.
     *
     * @param rowId ingested row id to project
     * @return mapped graph row, or {@code null} when the row id is outside the ingested range
     *         or both node sides are deleted
     */
    public synchronized String[] getRow(int rowId) {
        return getProjectedRowOrNull(rowId);
    }

    synchronized String[] getIteratorRow(int rowId) {
        if (rowId < 0 || rowId >= this.ingestedRowCount) {
            return null;
        }

        boolean fromDeleted = this.deletedRowFrom.contains(rowId);
        boolean toDeleted = this.deletedRowTo.contains(rowId);
        if (fromDeleted && toDeleted) {
            return null;
        }

        return buildIteratorRow(rowId, fromDeleted, toDeleted);
    }

    synchronized int[] getVisibleEncodedNodeIds(int rowId) {
        if (rowId < 0 || rowId >= this.ingestedRowCount) {
            return null;
        }

        boolean fromDeleted = this.deletedRowFrom.contains(rowId);
        boolean toDeleted = this.deletedRowTo.contains(rowId);
        if (fromDeleted && toDeleted) {
            return null;
        }

        return new int[]{
                fromDeleted ? -1 : this.numericalRowStore.getInt(rowId, this.fromIdNumericIndex),
                toDeleted ? -1 : this.numericalRowStore.getInt(rowId, this.toIdNumericIndex)
        };
    }

    String decodeNodeId(int encodedNodeId) {
        return this.dictionaryRegistry[this.idDictionaryIndex].getValue(encodedNodeId);
    }

    synchronized Map<String, GraphNodeStat> getGraphStatistics() {
        if (this.graphStatisticsVersion != this.graphVersion) {
            this.graphStatisticsCache = buildGraphStatistics();
            this.graphStatisticsVersion = this.graphVersion;
        }

        return copyGraphStatistics(this.graphStatisticsCache);
    }

    private Map<String, GraphNodeStat> buildGraphStatistics() {
        Map<Integer, List<Integer>> adjacency = new LinkedHashMap<>();
        Map<Integer, GraphNodeStat> encodedStatistics = new LinkedHashMap<>();
        IntIterator rowIds = getValidRowIds();

        while (rowIds.hasNext()) {
            int[] nodeIds = getVisibleEncodedNodeIds(rowIds.next());
            if (nodeIds == null) {
                continue;
            }

            int fromNodeId = nodeIds[0];
            int toNodeId = nodeIds[1];
            if (fromNodeId != -1) {
                encodedStatistics.computeIfAbsent(fromNodeId, ignored -> new GraphNodeStat());
                adjacency.computeIfAbsent(fromNodeId, ignored -> new ArrayList<>());
            }
            if (toNodeId != -1) {
                encodedStatistics.computeIfAbsent(toNodeId, ignored -> new GraphNodeStat());
                adjacency.computeIfAbsent(toNodeId, ignored -> new ArrayList<>());
            }
            if (fromNodeId != -1 && toNodeId != -1) {
                adjacency.get(fromNodeId).add(toNodeId);
            }
        }

        populateDegreesWithBreadthFirstTraversal(adjacency, encodedStatistics);

        Map<String, GraphNodeStat> statistics = new LinkedHashMap<>();
        for (Map.Entry<Integer, GraphNodeStat> entry : encodedStatistics.entrySet()) {
            statistics.put(decodeNodeId(entry.getKey()), entry.getValue());
        }
        return statistics;
    }

    private Map<String, GraphNodeStat> copyGraphStatistics(Map<String, GraphNodeStat> statistics) {
        Map<String, GraphNodeStat> copy = new LinkedHashMap<>();
        for (Map.Entry<String, GraphNodeStat> entry : statistics.entrySet()) {
            GraphNodeStat stat = entry.getValue();
            copy.put(entry.getKey(), new GraphNodeStat(stat.getOutDegree(), stat.getInDegree()));
        }
        return copy;
    }

    private void populateDegreesWithBreadthFirstTraversal(Map<Integer, List<Integer>> adjacency,
                                                          Map<Integer, GraphNodeStat> statistics) {
        Set<Integer> visited = new LinkedHashSet<>();
        Queue<Integer> queue = new ArrayDeque<>();

        for (Integer startNodeId : statistics.keySet()) {
            if (!visited.add(startNodeId)) {
                continue;
            }

            queue.add(startNodeId);
            while (!queue.isEmpty()) {
                Integer currentNodeId = queue.remove();
                GraphNodeStat currentStat = statistics.get(currentNodeId);
                for (Integer targetNodeId : adjacency.getOrDefault(currentNodeId, List.of())) {
                    currentStat.incrementOutDegree();
                    statistics.get(targetNodeId).incrementInDegree();
                    if (visited.add(targetNodeId)) {
                        queue.add(targetNodeId);
                    }
                }
            }
        }
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
        List<String> mappedRow = new ArrayList<>(this.mappedColumnCount);
        appendNodeValues(
                mappedRow,
                rowId,
                fromDeleted,
                this.fromIdCubeIndex != this.fromLabelCubeIndex,
                this.fromIdNumericIndex,
                this.fromLabelNumericIndex,
                this.fromAttrNumericIndices
        );
        appendNodeValues(
                mappedRow,
                rowId,
                toDeleted,
                this.toIdCubeIndex != this.toLabelCubeIndex,
                this.toIdNumericIndex,
                this.toLabelNumericIndex,
                this.toAttrNumericIndices
        );

        boolean relationDeleted = fromDeleted || toDeleted;
        for (int i = 0; i < this.relationNumericIndices.length; i++) {
            mappedRow.add(relationDeleted ? null : decodeRelationValue(rowId, this.relationNumericIndices[i], i));
        }

        return mappedRow.toArray(new String[0]);
    }

    private String[] buildIteratorRow(int rowId, boolean fromDeleted, boolean toDeleted) {
        List<String> mappedRow = new ArrayList<>(this.iteratorMappedColumnCount);
        appendIteratorNodeValues(
                mappedRow,
                rowId,
                fromDeleted,
                this.fromIdNumericIndex,
                this.fromLabelNumericIndex,
                this.fromAttrNumericIndices
        );
        appendIteratorNodeValues(
                mappedRow,
                rowId,
                toDeleted,
                this.toIdNumericIndex,
                this.toLabelNumericIndex,
                this.toAttrNumericIndices
        );

        boolean relationDeleted = fromDeleted || toDeleted;
        for (int i = 0; i < this.relationNumericIndices.length; i++) {
            mappedRow.add(relationDeleted ? null : decodeRelationValue(rowId, this.relationNumericIndices[i], i));
        }

        return mappedRow.toArray(new String[0]);
    }

    private int computeMappedColumnCount() {
        return getNodeColumnCount(this.fromIdCubeIndex, this.fromLabelCubeIndex, this.fromAttrCubeIndices)
                + getNodeColumnCount(this.toIdCubeIndex, this.toLabelCubeIndex, this.toAttrCubeIndices)
                + this.relationCubeIndices.length;
    }

    private int getNodeColumnCount(int idCubeIndex, int labelCubeIndex, int[] attrCubeIndices) {
        int labelColumnCount = idCubeIndex == labelCubeIndex ? 0 : 1;
        return 1 + labelColumnCount + attrCubeIndices.length;
    }

    private int computeIteratorMappedColumnCount() {
        return getIteratorNodeColumnCount(this.fromAttrCubeIndices)
                + getIteratorNodeColumnCount(this.toAttrCubeIndices)
                + this.relationCubeIndices.length;
    }

    private int getIteratorNodeColumnCount(int[] attrCubeIndices) {
        return 2 + attrCubeIndices.length;
    }

    private void appendNodeValues(List<String> mappedRow,
                                  int rowId,
                                  boolean nodeDeleted,
                                  boolean includeLabel,
                                  int idNumericIndex,
                                  int labelNumericIndex,
                                  int[] attrNumericIndices) {
        mappedRow.add(nodeDeleted ? null : decodeIdValue(rowId, idNumericIndex));
        if (includeLabel) {
            mappedRow.add(nodeDeleted ? null : decodeLabelValue(rowId, labelNumericIndex));
        }

        for (int i = 0; i < attrNumericIndices.length; i++) {
            mappedRow.add(nodeDeleted ? null : decodeAttributeValue(rowId, attrNumericIndices[i], i));
        }
    }

    private void appendIteratorNodeValues(List<String> mappedRow,
                                          int rowId,
                                          boolean nodeDeleted,
                                          int idNumericIndex,
                                          int labelNumericIndex,
                                          int[] attrNumericIndices) {
        mappedRow.add(nodeDeleted ? null : decodeIdValue(rowId, idNumericIndex));
        mappedRow.add(nodeDeleted ? null : decodeLabelValue(rowId, labelNumericIndex));

        for (int i = 0; i < attrNumericIndices.length; i++) {
            mappedRow.add(nodeDeleted ? null : decodeAttributeValue(rowId, attrNumericIndices[i], i));
        }
    }

    private String decodeIdValue(int rowId, int numericIndex) {
        return decodeValue(rowId, numericIndex, this.idDictionaryIndex);
    }

    private String decodeLabelValue(int rowId, int numericIndex) {
        return decodeValue(rowId, numericIndex, this.labelDictionaryIndex);
    }

    private String decodeAttributeValue(int rowId, int numericIndex, int attributeIndex) {
        return decodeValue(rowId, numericIndex, this.attributeDictionaryIndices[attributeIndex]);
    }

    private String decodeRelationValue(int rowId, int numericIndex, int relationIndex) {
        return decodeValue(rowId, numericIndex, this.relationDictionaryIndices[relationIndex]);
    }

    private String decodeValue(int rowId, int numericIndex, int dictionaryIndex) {
        int encodedValue = this.numericalRowStore.getInt(rowId, numericIndex);
        return this.dictionaryRegistry[dictionaryIndex].getValue(encodedValue);
    }

}
