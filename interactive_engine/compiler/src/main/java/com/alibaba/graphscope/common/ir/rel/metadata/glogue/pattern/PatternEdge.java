package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.List;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;

public abstract class PatternEdge {

    public abstract PatternVertex getSrcVertex();

    public abstract PatternVertex getDstVertex();

    public abstract Integer getId();

    public abstract List<EdgeTypeId> getEdgeTypeIds();

    @Override
    public String toString() {
        return getSrcVertex().getId() + "->" + getDstVertex().getId() + "[" + getEdgeTypeIds().toString() + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o){
        if (!(o instanceof PatternEdge)) {
            return false;
        }
        PatternEdge other = (PatternEdge) o;
        return this.getSrcVertex().equals(other.getSrcVertex()) && this.getDstVertex().equals(other.getDstVertex()) && this.getEdgeTypeIds().equals(other.getEdgeTypeIds());
    }

}
