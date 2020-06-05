/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PeerPressureVertexProgram extends StaticVertexProgram<Pair<Serializable, Double>> {

    private static final Logger log =
            LoggerFactory.getLogger(PeerPressureVertexProgram.class);
    private MessageScope.Local<?> voteScope = MessageScope.Local.of(__::outE);
    private MessageScope.Local<?> countScope = MessageScope.Local.of(new MessageScope.Local.ReverseTraversalSupplier(this.voteScope));

    public static final String CLUSTER = "gremlin.peerPressureVertexProgram.cluster";
    public  static final String VOTE_STRENGTH = "gremlin.peerPressureVertexProgram.voteStrength";
    public static final String LOCAL_VOTE_STRENGTH = "gremlin.peerPressureVertexProgram.localVoteStrength";
    private static final String INITIAL_VOTE_STRENGTH_TRAVERSAL = "gremlin.pageRankVertexProgram.initialVoteStrengthTraversal";
    private static final String PROPERTY = "gremlin.peerPressureVertexProgram.property";
    private static final String MAX_ITERATIONS = "gremlin.peerPressureVertexProgram.maxIterations";
    private static final String DISTRIBUTE_VOTE = "gremlin.peerPressureVertexProgram.distributeVote";
    private static final String EDGE_TRAVERSAL = "gremlin.peerPressureVertexProgram.edgeTraversal";
    private static final String VOTE_TO_HALT = "gremlin.peerPressureVertexProgram.voteToHalt";
    private static final String WRITE_SERVICE = "org.janusgraph.hadoop.PeerPressureWriteBackService";
    private static final String PREFIX = "gremlin.peerPressureVertexProgram.prefix";

    private PureTraversal<Vertex, Edge> edgeTraversal = null;
    private PureTraversal<Vertex, ? extends Number> initialVoteStrengthTraversal = null;
    private int maxIterations = 30;
    private boolean distributeVote = false;
    private String property = CLUSTER;
    private String prefix = "Prefix";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = Collections.singleton(MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true));

    private PeerPressureVertexProgram() {

    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        if (configuration.containsKey(INITIAL_VOTE_STRENGTH_TRAVERSAL))
            this.initialVoteStrengthTraversal = PureTraversal.loadState(configuration, INITIAL_VOTE_STRENGTH_TRAVERSAL, graph);
        if (configuration.containsKey(EDGE_TRAVERSAL)) {
            this.edgeTraversal = PureTraversal.loadState(configuration, EDGE_TRAVERSAL, graph);
            this.voteScope = MessageScope.Local.of(() -> this.edgeTraversal.get().clone());
            this.countScope = MessageScope.Local.of(new MessageScope.Local.ReverseTraversalSupplier(this.voteScope));
        }
        this.prefix = this.prefix + configuration.getString(PREFIX, "");
        this.property = configuration.getString(PROPERTY, CLUSTER);
        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 30);
        this.distributeVote = configuration.getBoolean(DISTRIBUTE_VOTE, false);
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(PROPERTY, this.property);
        configuration.setProperty(PREFIX, this.prefix);
        configuration.setProperty(MAX_ITERATIONS, this.maxIterations);
        configuration.setProperty(DISTRIBUTE_VOTE, this.distributeVote);
        if (null != this.edgeTraversal)
            this.edgeTraversal.storeState(configuration, EDGE_TRAVERSAL);
        if (null != this.initialVoteStrengthTraversal)
            this.initialVoteStrengthTraversal.storeState(configuration, INITIAL_VOTE_STRENGTH_TRAVERSAL);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return new HashSet<>(Arrays.asList(VertexComputeKey.of(this.property, false), VertexComputeKey.of(VOTE_STRENGTH, true), VertexComputeKey.of(LOCAL_VOTE_STRENGTH, false), VertexComputeKey.of(this.prefix, true)));
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        final Set<MessageScope> VOTE_SCOPE = new HashSet<>(Collections.singletonList(this.voteScope));
        final Set<MessageScope> COUNT_SCOPE = new HashSet<>(Collections.singletonList(this.countScope));
        return this.distributeVote && memory.isInitialIteration() ? COUNT_SCOPE : VOTE_SCOPE;
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }

    @Override
    public <P extends WriteBackService> Class<P> getServiceClass() throws ClassNotFoundException {
        return (Class<P>) Class.forName(WRITE_SERVICE);
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, false);
    }

    @Override
    public void execute(final Vertex vertex, Messenger<Pair<Serializable, Double>> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            if (this.distributeVote) {
                messenger.sendMessage(this.countScope, Pair.with('c', 1.0d));
            } else {
                double voteStrength = (null == this.initialVoteStrengthTraversal ?
                        1.0d :
                        TraversalUtil.apply(vertex, this.initialVoteStrengthTraversal.get()).doubleValue());
                vertex.property(VertexProperty.Cardinality.single, this.property, vertex.id());
                vertex.property(VertexProperty.Cardinality.single, VOTE_STRENGTH, voteStrength);
                vertex.property(VertexProperty.Cardinality.single, LOCAL_VOTE_STRENGTH, voteStrength);
                messenger.sendMessage(this.voteScope, new Pair<>((Serializable) vertex.id(), voteStrength));
                memory.add(VOTE_TO_HALT, false);
            }
        } else if (1 == memory.getIteration() && this.distributeVote) {
            double voteStrength = (null == this.initialVoteStrengthTraversal ?
                    1.0d :
                    TraversalUtil.apply(vertex, this.initialVoteStrengthTraversal.get()).doubleValue()) /
                    IteratorUtils.reduce(IteratorUtils.map(messenger.receiveMessages(), Pair::getValue1), 0.0d, (a, b) -> a + b);
            vertex.property(VertexProperty.Cardinality.single, this.property, vertex.id());
            vertex.property(VertexProperty.Cardinality.single, VOTE_STRENGTH, voteStrength);
            vertex.property(VertexProperty.Cardinality.single, LOCAL_VOTE_STRENGTH, voteStrength);
            messenger.sendMessage(this.voteScope, new Pair<>((Serializable) vertex.id(), voteStrength));
            memory.add(VOTE_TO_HALT, false);
        } else {
            final Map<Serializable, Double> votes = new HashMap<>(36);
            //为了防止两点之间多边权重过大，导致一直两点互换id，最终无法在同一个社团里，添加本地权重为上一次投票的非本地最大权重
            votes.put(vertex.value(this.property), vertex.<Double>value(LOCAL_VOTE_STRENGTH));
            messenger.receiveMessages().forEachRemaining(message -> MapHelper.incr(votes, message.getValue0(), message.getValue1()));
            Quartet<Serializable, Double, Serializable, Double> quartet = PeerPressureVertexProgram.largestCount(votes, vertex.value(this.property));
            Serializable cluster = quartet.getValue0();
            if (null == cluster) {
                cluster = (Serializable) vertex.id();
            }
            if(!vertex.value(this.property).equals(cluster)){
                log.debug("vertex [{}] changed community from [{}] to [{}]",vertex.id(), vertex.value(this.property), cluster);
            }
            memory.add(VOTE_TO_HALT, vertex.value(this.property).equals(cluster));
            vertex.property(VertexProperty.Cardinality.single, this.property, cluster);
            vertex.property(VertexProperty.Cardinality.single, LOCAL_VOTE_STRENGTH, quartet.getValue3() != Double.MIN_VALUE ? quartet.getValue3() : vertex.<Double>value(VOTE_STRENGTH));
            messenger.sendMessage(this.voteScope, new Pair<>(cluster, vertex.<Double>value(VOTE_STRENGTH)));
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT) || memory.getIteration() >= (this.distributeVote ? this.maxIterations + 1 : this.maxIterations);
        if (voteToHalt) {
            return true;
        } else {
            memory.set(VOTE_TO_HALT, true);
            return false;
        }
    }

    private static <T> Quartet<T,Double,T,Double> largestCount(final Map<T, Double> map, T local) {
        T largestKey = null;
        T secondKey = null;
        double largestValue = Double.MIN_VALUE;
        double secondValue = Double.MIN_VALUE;
        for (Map.Entry<T, Double> entry : map.entrySet()) {
            if(entry.getValue() > largestValue){
                secondKey = largestKey;
                secondValue = largestValue;
                largestKey = entry.getKey();
                largestValue = entry.getValue();
                continue;
            } else if (entry.getValue() == largestValue){
                if (null != largestKey && largestKey.toString().compareTo(entry.getKey().toString()) > 0) {
                    secondKey = largestKey;
                    secondValue = largestValue;
                    largestKey = entry.getKey();
                    largestValue = entry.getValue();
                    continue;
                }
            }
            if (entry.getValue() > secondValue){
                secondKey = entry.getKey();
                secondValue = entry.getValue();
            } else if (entry.getValue() == secondValue){
                if (null != secondKey && secondKey.toString().compareTo(entry.getKey().toString()) > 0){
                    secondKey = entry.getKey();
                    secondValue = entry.getValue();
                }
            }

//            if (entry.getValue() == largestValue) {
//                if (null != largestKey && largestKey.toString().compareTo(entry.getKey().toString()) > 0) {
//                    largestKey = entry.getKey();
//                    largestValue = entry.getValue();
//                }
//            } else if (entry.getValue() > largestValue) {
//                largestKey = entry.getKey();
//                largestValue = entry.getValue();
//            }
        }

        if(local.equals(largestKey)){
            return new Quartet<>(largestKey, largestValue, secondKey, secondValue);
        } else {
            return new Quartet<>(largestKey, largestValue, largestKey, largestValue);
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "distributeVote=" + this.distributeVote + ", maxIterations=" + this.maxIterations);
    }

    //////////////////////////////

    public static Builder build() {
        return new Builder();
    }

    public static final class Builder extends AbstractVertexProgramBuilder<Builder> {


        private Builder() {
            super(PeerPressureVertexProgram.class);
        }

        public Builder property(final String key) {
            this.configuration.setProperty(PROPERTY, key);
            return this;
        }

        public Builder prefix(final String prefix) {
            this.configuration.setProperty(PREFIX, prefix);
            return this;
        }

        public Builder maxIterations(final int iterations) {
            this.configuration.setProperty(MAX_ITERATIONS, iterations);
            return this;
        }

        public Builder distributeVote(final boolean distributeVote) {
            this.configuration.setProperty(DISTRIBUTE_VOTE, distributeVote);
            return this;
        }

        public Builder edges(final Traversal.Admin<Vertex, Edge> edgeTraversal) {
            PureTraversal.storeState(this.configuration, EDGE_TRAVERSAL, edgeTraversal);
            return this;
        }

        public Builder initialVoteStrength(final Traversal.Admin<Vertex, ? extends Number> initialVoteStrengthTraversal) {
            PureTraversal.storeState(this.configuration, INITIAL_VOTE_STRENGTH_TRAVERSAL, initialVoteStrengthTraversal);
            return this;
        }

        /**
         * @deprecated As of release 3.2.0, replaced by {@link PeerPressureVertexProgram.Builder#edges(Traversal.Admin)}
         */
        @Deprecated
        public Builder traversal(final TraversalSource traversalSource, final String scriptEngine, final String traversalScript, final Object... bindings) {
            return this.edges(new ScriptTraversal<>(traversalSource, scriptEngine, traversalScript, bindings));
        }

        /**
         * @deprecated As of release 3.2.0, replaced by {@link PeerPressureVertexProgram.Builder#edges(Traversal.Admin)}
         */
        @Deprecated
        public Builder traversal(final Traversal.Admin<Vertex, Edge> edgeTraversal) {
            return this.edges(edgeTraversal);
        }

    }

    ////////////////////////////

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }

            @Override
            public boolean requiresWriteBackToOriginalGraph() {
                return true;
            }
        };
    }
}
