package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.Map;

import org.javatuples.Pair;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternDirection;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class GlogueBasicCardinalityEstimationImpl implements GlogueCardinalityEstimation {
    private Map<Pattern, Double> patternCardinality;

    public GlogueBasicCardinalityEstimationImpl() {
        this.patternCardinality = new HashMap<Pattern, Double>();
    }

    public GlogueBasicCardinalityEstimationImpl create(Glogue glogue, GlogueSchema schema) {
        Deque<Pattern> patternQueue = new ArrayDeque<>();
        List<Pattern> roots = glogue.getRoots();
        for (Pattern pattern : roots) {
            // single vertex pattern
            PatternVertex singleVertexPattern = pattern.getVertexSet().iterator().next();
            Double singleVertexPatternCount = schema.getVertexTypeCardinality(singleVertexPattern.getVertexTypeId());
            this.patternCardinality.put(pattern, singleVertexPatternCount);
            System.out.println("root vertex pattern: " + pattern + ": " + singleVertexPatternCount);
            patternQueue.add(pattern);
        }

        while (patternQueue.size() > 0) {
            Pattern pattern = patternQueue.pop();
            Double patternCount = this.patternCardinality.get(pattern);
            for (GlogueEdge edge : glogue.getOutEdges(pattern)) {
                GlogueExtendIntersectEdge extendIntersectEdge = (GlogueExtendIntersectEdge) edge;
                Pattern newPattern = extendIntersectEdge.getDstPattern();
                ExtendStep extendStep = extendIntersectEdge.getExtendStep();

                if (this.containsPattern(newPattern)) {
                    // if the cardinality of the pattern is already computed previously, compute the
                    // pattern extension cost.
                    System.out.println("pattern already computed: " + newPattern);
                    // sort extend edges based on weights
                    Double extendStepWeight = estimateExtendWeight(schema, extendStep);
                    extendStep.setWeight(extendStepWeight);
                } else {
                    // otherwise, compute the cardinality of the pattern, together with the pattern
                    // extension cost.
                    System.out.println("extend step: " + extendStep.toString());
                    Pair<Double, Double> patternCountWithWeight = estimatePatternCountWithExtendWeight(schema,
                            patternCount,
                            extendStep);
                    this.patternCardinality.put(newPattern, patternCountWithWeight.getValue0());
                    extendStep.setWeight(patternCountWithWeight.getValue1());
                    patternQueue.add(newPattern);
                    System.out.println("new pattern: " + newPattern + ": " + patternCountWithWeight.getValue0());
                }
            }
        }

        return this;
    }

    /// Given the src pattern and extend step, estimate the cardinality of the
    /// target pattern by extending the extendStep from srcPattern, together with
    /// pattern extension cost.
    /// Return the pair of (targetPatternCardinality, extendStepWeight)
    private Pair<Double, Double> estimatePatternCountWithExtendWeight(GlogueSchema schema, Double srcPatternCount,
            ExtendStep extendStep) {
        initEdgeWeightsInExtendStep(schema, extendStep);
        // estimate pattern count and also set the weight of the extend step
        Double commonTargetVertexTypeCount = schema.getVertexTypeCardinality(extendStep.getTargetVertexType());
        Iterator<ExtendEdge> iter = extendStep.getExtendEdges().iterator();
        Double targetPatternCount = srcPatternCount * iter.next().getWeight();
        Double intermediate = targetPatternCount;
        while (iter.hasNext()) {
            ExtendEdge extendEdge = iter.next();
            targetPatternCount *= extendEdge.getWeight() / commonTargetVertexTypeCount;
            intermediate += targetPatternCount;
        }
        return Pair.with(targetPatternCount, intermediate / srcPatternCount);
    }

    /// Given the src pattern and extend step, estimate the pattern extension cost.
    /// Return the estimated extendStepWeight.
    private Double estimateExtendWeight(GlogueSchema schema, ExtendStep extendStep) {
        initEdgeWeightsInExtendStep(schema, extendStep);
        Double commonTargetVertexCount = schema.getVertexTypeCardinality(extendStep.getTargetVertexType());
        Iterator<ExtendEdge> iter = extendStep.getExtendEdges().iterator();
        Double extendStepWeight = iter.next().getWeight();
        Double intermediate = 1.0;
        while (iter.hasNext()) {
            ExtendEdge extendEdge = iter.next();
            intermediate *= extendEdge.getWeight() / commonTargetVertexCount;
            extendStepWeight += intermediate;
        }
        return extendStepWeight;

    }

    private void initEdgeWeightsInExtendStep(GlogueSchema schema, ExtendStep extendStep) {
        for (ExtendEdge extendEdge : extendStep.getExtendEdges()) {
            // each extendEdge extends to a new edge
            // estimate the cardinality by multiplying the edge type cardinality
            Double extendEdgeCount = schema.getEdgeTypeCardinality(extendEdge.getEdgeTypeId());
            // each srcVertex is a common vertex when extending the edge
            // estimate the cardinality by dividing the src vertex type cardinality
            Integer srcVertexType;
            if (extendEdge.getDirection().equals(PatternDirection.OUT)) {
                srcVertexType = extendEdge.getEdgeTypeId().getSrcLabelId();
            } else {
                srcVertexType = extendEdge.getEdgeTypeId().getDstLabelId();
            }
            Double commonSrcVertexCount = schema.getVertexTypeCardinality(srcVertexType);
            // Set the ExtendEdge weight: the estimated average pattern cardinality after
            // extending current expand edge
            extendEdge.setWeight(extendEdgeCount / commonSrcVertexCount);
        }
        extendStep.sortExtendEdges();
    }

    private boolean containsPattern(Pattern pattern) {
        return this.patternCardinality.containsKey(pattern);
    }

    @Override
    public double getCardinality(Pattern queryPattern) {
        for (Pattern pattern : this.patternCardinality.keySet()) {
            if (pattern.equals(queryPattern)) {
                return this.patternCardinality.get(pattern);
            }
        }
        return 0.0;
    }

    @Override
    public String toString() {
        String s = "";
        for (Pattern pattern : this.patternCardinality.keySet()) {
            s += pattern + ": " + this.patternCardinality.get(pattern) + "\n";
        }
        return s;
    }

}
