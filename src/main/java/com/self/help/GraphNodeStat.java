package com.self.help;

import java.util.Objects;

/**
 * Stores directed degree information for one graph node.
 * Out degree counts visible edges that leave the node; in degree counts visible
 * edges that enter the node.
 */
public class GraphNodeStat {
    private int outDegree;
    private int inDegree;

    /**
     * Creates a node statistic with zero in and out degree.
     */
    public GraphNodeStat() {
    }

    /**
     * Creates a node statistic with explicit degree values.
     *
     * @param outDegree number of visible outgoing edges
     * @param inDegree number of visible incoming edges
     */
    public GraphNodeStat(int outDegree, int inDegree) {
        this.outDegree = outDegree;
        this.inDegree = inDegree;
    }

    /**
     * Returns the number of visible outgoing edges for the node.
     *
     * @return outgoing edge count
     */
    public int getOutDegree() {
        return this.outDegree;
    }

    /**
     * Returns the number of visible incoming edges for the node.
     *
     * @return incoming edge count
     */
    public int getInDegree() {
        return this.inDegree;
    }

    void incrementOutDegree() {
        this.outDegree++;
    }

    void incrementInDegree() {
        this.inDegree++;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof GraphNodeStat that)) {
            return false;
        }
        return this.outDegree == that.outDegree && this.inDegree == that.inDegree;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.outDegree, this.inDegree);
    }

    @Override
    public String toString() {
        return "GraphNodeStat{"
                + "outDegree=" + this.outDegree
                + ", inDegree=" + this.inDegree
                + '}';
    }
}
