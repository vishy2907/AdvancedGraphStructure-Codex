package com.self.help;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public record MappingSpec(@NotNull NodeSpec fromNodeSpec, @NotNull NodeSpec toNodeSpec, @NotNull List<String> relations) {
    @NotNull
    /**
     * Returns the source-column mapping for the edge's from-node side.
     *
     * @return from-node mapping
     */
    public NodeSpec getFromNodeSpec() {
        return fromNodeSpec;
    }

    @NotNull
    /**
     * Returns the source-column mapping for the edge's to-node side.
     *
     * @return to-node mapping
     */
    public NodeSpec getToNodeSpec() {
        return toNodeSpec;
    }

    /**
     * Returns the source columns that describe edge/relation properties.
     *
     * @return relation column names in projection order
     */
    public List<String> getRelationColumnNames() {
        return relations;
    }
}
