package com.self.help;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public record MappingSpec(@NotNull NodeSpec fromNodeSpec, @NotNull NodeSpec toNodeSpec, @NotNull List<String> relations) {
    @NotNull
    public NodeSpec getFromNodeSpec() {
        return fromNodeSpec;
    }

    @NotNull
    public NodeSpec getToNodeSpec() {
        return toNodeSpec;
    }

    public List<String> getRelationColumnNames() {
        return relations;
    }
}
