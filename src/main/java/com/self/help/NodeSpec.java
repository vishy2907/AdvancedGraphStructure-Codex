package com.self.help;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public record NodeSpec(@NotNull String idColumnName, @Nullable String labelColumnName, @Nullable List<String> attributes) {
    public NodeSpec(@NotNull String idColumnName, @Nullable String labelColumnName, @Nullable List<String> attributes) {
        this.idColumnName = idColumnName;
        this.labelColumnName = Objects.requireNonNullElse(labelColumnName, idColumnName);
        this.attributes = Objects.requireNonNullElse(attributes, Collections.emptyList());
    }

    @NotNull
    public String getLabelColumnName() {
        return labelColumnName;
    }

    @NotNull
    public String getIdColumnName() {
        return idColumnName;
    }

    public List<String> getNodeAttributeNames() {
        return attributes;
    }

}
