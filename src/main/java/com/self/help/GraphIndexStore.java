package com.self.help;

/**
 * Defines stable index mappings for graph dictionaries.
 * It does not own or mutate graph data structures; the graph engine owns those
 * structures and uses this mapping to address them consistently.
 */
final class GraphIndexStore {
    private final int dictionaryCount;
    private final int idDictionaryIndex;
    private final int labelDictionaryIndex;
    private final int[] attributeDictionaryIndices;
    private final int[] relationDictionaryIndices;

    GraphIndexStore(int attributeCount, int relationCount) {
        if (attributeCount < 0) {
            throw new IllegalArgumentException("Attribute count cannot be negative.");
        }
        if (relationCount < 0) {
            throw new IllegalArgumentException("Relation count cannot be negative.");
        }

        int currentDictionaryIndex = 0;
        this.idDictionaryIndex = currentDictionaryIndex++;
        this.labelDictionaryIndex = currentDictionaryIndex++;
        int attributeDictionaryBaseIndex = currentDictionaryIndex;
        currentDictionaryIndex += attributeCount;
        int relationDictionaryBaseIndex = currentDictionaryIndex;
        currentDictionaryIndex += relationCount;

        this.dictionaryCount = currentDictionaryIndex;

        this.attributeDictionaryIndices = new int[attributeCount];
        for (int i = 0; i < attributeCount; i++) {
            this.attributeDictionaryIndices[i] = attributeDictionaryBaseIndex + i;
        }

        this.relationDictionaryIndices = new int[relationCount];
        for (int i = 0; i < relationCount; i++) {
            this.relationDictionaryIndices[i] = relationDictionaryBaseIndex + i;
        }
    }

    int dictionaryCount() {
        return this.dictionaryCount;
    }

    int idDictionaryIndex() {
        return this.idDictionaryIndex;
    }

    int labelDictionaryIndex() {
        return this.labelDictionaryIndex;
    }

    int attributeDictionaryIndex(int attributeIndex) {
        return this.attributeDictionaryIndices[attributeIndex];
    }

    int relationDictionaryIndex(int relationIndex) {
        return this.relationDictionaryIndices[relationIndex];
    }

    int[] attributeDictionaryIndices() {
        return this.attributeDictionaryIndices;
    }

    int[] relationDictionaryIndices() {
        return this.relationDictionaryIndices;
    }
}
