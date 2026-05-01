package com.self.help;

/**
 * Stores the inverted indexes for one node side of the graph.
 * Both FROM and TO sides use the same shape: one id index, one label index,
 * and one index per mapped node attribute.
 */
final class NodeIndexBlock {
    private final InvertedIndexColumn idIndex;
    private final InvertedIndexColumn labelIndex;
    private final InvertedIndexColumn[] attributeIndexes;

    NodeIndexBlock(int attributeCount) {
        if (attributeCount < 0) {
            throw new IllegalArgumentException("Attribute count cannot be negative.");
        }

        this.idIndex = new InvertedIndexColumn();
        this.labelIndex = new InvertedIndexColumn();
        this.attributeIndexes = new InvertedIndexColumn[attributeCount];
        for (int i = 0; i < attributeCount; i++) {
            this.attributeIndexes[i] = new InvertedIndexColumn();
        }
    }

    InvertedIndexColumn idIndex() {
        return this.idIndex;
    }

    InvertedIndexColumn labelIndex() {
        return this.labelIndex;
    }

    InvertedIndexColumn attributeIndex(int attributeIndex) {
        return this.attributeIndexes[attributeIndex];
    }
}
