package com.self.help;

/**
 * Stores the inverted indexes for mapped relation columns.
 * The index position is aligned with the relation order from the mapping spec.
 */
final class RelationIndexBlock {
    private final InvertedIndexColumn[] relationIndexes;

    RelationIndexBlock(int relationCount) {
        if (relationCount < 0) {
            throw new IllegalArgumentException("Relation count cannot be negative.");
        }

        this.relationIndexes = new InvertedIndexColumn[relationCount];
        for (int i = 0; i < relationCount; i++) {
            this.relationIndexes[i] = new InvertedIndexColumn();
        }
    }

    InvertedIndexColumn relationIndex(int relationIndex) {
        return this.relationIndexes[relationIndex];
    }
}
