package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.graph.DirectedPseudograph;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class Glogue {
    // the topology of GLogue graph
    private Graph<Pattern, GlogueEdge> glogueGraph;
    private GlogueCardinalityEstimation glogueCardinalityEstimation;
    private List<Pattern> roots;
    private int maxPatternId;

    protected Glogue() {
        this.glogueGraph = new DirectedPseudograph<Pattern, GlogueEdge>(GlogueEdge.class);
        this.roots = new ArrayList<>();
        this.maxPatternId = 0;
    }

    // Construct NewGlogue from a graph schema with default max pattern size = 3
    public Glogue create(GlogueSchema schema) {
        return this.create(schema, 3);
    }

    // Construct NewGlogue from a graph schema with given max pattern size
    public Glogue create(GlogueSchema schema, int maxPatternSize) {
        Deque<Pattern> patternQueue = new ArrayDeque<>();
        for (Integer vertexTypeId : schema.getVertexTypes()) {
            PatternVertex vertex = new PatternVertex(vertexTypeId);
            Pattern new_pattern = new Pattern(vertex);
            this.addPattern(new_pattern);
            this.addRoot(new_pattern);
            patternQueue.add(new_pattern);
        }
        System.out.println("init glogue:\n" + this);
        while (patternQueue.size() > 0) {
            Pattern pattern = patternQueue.pop();
            if (pattern.size() >= maxPatternSize) {
                continue;
            }
            System.out.println("~~~~~~~~pop pattern in queue~~~~~~~~~~");
            List<ExtendStep> extendSteps = pattern.getExtendSteps(schema);
            System.out.println("original pattern " + pattern.toString());
            System.out.println("extend steps number: " + extendSteps.size());
            for (ExtendStep extendStep : extendSteps) {
                System.out.println(extendStep);
                Pattern newPattern = pattern.extend(extendStep);
                Optional<Pattern> existingPattern = this.containsPattern(newPattern);
                if (!existingPattern.isPresent()) {
                    this.addPattern(newPattern);
                    System.out.println("add new pattern: " + newPattern);
                    Map<Integer, Integer> srcToDstPatternMapping = this.computePatternMapping(pattern, newPattern);
                    this.addPatternEdge(pattern, newPattern, extendStep, srcToDstPatternMapping);
                    patternQueue.add(newPattern);
                } else {
                    System.out.println(
                            "pattern already exists: " + existingPattern.get());
                    System.out.println("v.s. the new pattern: " + newPattern);
                    if (!this.containsPatternEdge(pattern, existingPattern.get())) {
                        // notice that the IdMapping should be computed based on pattern and newPattern,
                        // not pattern and existingPattern
                        Map<Integer, Integer> srcToDstPatternMapping = this.computePatternMapping(pattern, newPattern);
                        this.addPatternEdge(pattern, existingPattern.get(), extendStep, srcToDstPatternMapping);
                    } else {
                        System.out
                                .println("edge already exists as well");
                    }
                }
            }
            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        }

        System.out.println("NewGlogue " + this.toString());
        System.out.println();
        System.out.println();
        // compute pattern cardinality
        this.glogueCardinalityEstimation = new GlogueBasicCardinalityEstimationImpl().create(this, schema);

        System.out.println("GlogueBasicCardinalityEstimationImpl " + this.glogueCardinalityEstimation.toString());

        return this;
    }

    public Set<GlogueEdge> getOutEdges(Pattern pattern) {
        return glogueGraph.outgoingEdgesOf(pattern);

    }

    public Set<GlogueEdge> getInEdge(Pattern pattern) {
        return glogueGraph.incomingEdgesOf(pattern);
    }

    private void addRoot(Pattern pattern) {
        this.roots.add(pattern);
    }

    public List<Pattern> getRoots() {
        return roots;
    }

    private Optional<Pattern> containsPattern(Pattern pattern) {
        for (Pattern p : this.glogueGraph.vertexSet()) {
            if (p.equals(pattern)) {
                return Optional.of(p);
            }
        }
        return Optional.empty();
    }

    private boolean containsPatternEdge(Pattern srcPattern, Pattern dstPattern) {
        return this.glogueGraph.containsEdge(srcPattern, dstPattern);
    }

    private boolean addPattern(Pattern pattern) {
        pattern.setPatternId(this.maxPatternId++);
        return this.glogueGraph.addVertex(pattern);
    }

    private boolean addPatternEdge(Pattern srcPattern, Pattern dstPattern, ExtendStep edge,
            Map<Integer, Integer> srcToDstIdMapping) {
        GlogueExtendIntersectEdge glogueEdge = new GlogueExtendIntersectEdge(srcPattern, dstPattern, edge,
                srcToDstIdMapping);
        System.out.println("add glogue edge " + glogueEdge);
        return this.glogueGraph.addEdge(srcPattern, dstPattern, glogueEdge);
    }

    /// Compute the mapping from src pattern to dst pattern.
    /// The mapping preserves srcPatternVertexOrder -> dstPatternVertexOrder.
    /// Notice that, the dstPattern should be extended from srcPattern.
    private Map<Integer, Integer> computePatternMapping(Pattern srcPattern, Pattern dstPattern) {
        Map<Integer, Integer> srcToDstPatternMapping = new HashMap<>();
        for (PatternVertex srcVertex : srcPattern.getVertexSet()) {
            Integer srcVertexOrder = srcPattern.getVertexOrder(srcVertex);
            // dstPattern is extended from srcPatter, so they have the same vertex id.
            PatternVertex dstVertex = dstPattern.getVertexById(srcVertex.getId());
            Integer dstVertexOrder = dstPattern.getVertexOrder(dstVertex);
            srcToDstPatternMapping.put(srcVertexOrder, dstVertexOrder);
        }
        return srcToDstPatternMapping;
    }

    // TODO: implements interface in Calcite
    public Double getRowCount(Pattern pattern) {
        return this.glogueCardinalityEstimation.getCardinality(pattern);
    }

    @Override
    public String toString() {
        return "GlogueVertices: " + this.glogueGraph.vertexSet() + "\nGlogueEdges: " + this.glogueGraph.edgeSet();
    }

    public static void main(String[] args) {
        GlogueSchema g = new GlogueSchema().DefaultGraphSchema();
        Glogue gl = new Glogue().create(g, 3);
        Pattern p = new Pattern();

        // p1 -> s0 <- p2 + p1 -> p2
        PatternVertex v0 = new PatternVertex(1, 0);
        PatternVertex v1 = new PatternVertex(0, 1);
        PatternVertex v2 = new PatternVertex(0, 2);
        // p -> s
        EdgeTypeId e = new EdgeTypeId(0, 1, 1);
        // p -> p
        EdgeTypeId e1 = new EdgeTypeId(0, 0, 0);
        p.addVertex(v0);
        p.addVertex(v1);
        p.addVertex(v2);
        p.addEdge(v1, v0, e);
        p.addEdge(v2, v0, e);
        p.addEdge(v1, v2, e1);
        p.encoding();

        // p0 -> s2 <- p1 + p0 -> p1
        Pattern p2 = new Pattern();
        PatternVertex v00 = new PatternVertex(0, 0);
        PatternVertex v11 = new PatternVertex(0, 1);
        PatternVertex v22 = new PatternVertex(1, 2);
        p2.addVertex(v00);
        p2.addVertex(v11);
        p2.addVertex(v22);
        p2.addEdge(v00, v22, e);
        p2.addEdge(v11, v22, e);
        p2.addEdge(v00, v11, e1);
        p2.encoding();

        System.out.println("Pattern: " + p);

        Double count = gl.getRowCount(p);
        System.out.println("estimated count: " + count);

        System.out.println("Pattern2: " + p2);

        Double count2 = gl.getRowCount(p2);
        System.out.println("estimated count: " + count2);
    }
}