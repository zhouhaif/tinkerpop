package org.apache.tinkerpop.gremlin.process.computer.drop;


import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Tuple;

import java.util.Collections;
import java.util.Set;

/**
 * @author zhf
 */
public class DropVertexProgram implements VertexProgram<Tuple> {

    private static final String WRITE_SERVICE = "org.janusgraph.hadoop.DropVertexWriteBackService";
    @Override
    public void setup(Memory memory) {

    }

    @Override
    public void execute(Vertex vertex, Messenger<Tuple> messenger, Memory memory) {

    }

    @Override
    public boolean terminate(Memory memory) {
        return true;
    }

    @Override
    public Set<MessageScope> getMessageScopes(Memory memory) {
        return Collections.emptySet();
    }

    @Override
    public <P extends WriteBackService> Class<P> getServiceClass() throws ClassNotFoundException {
        return (Class<P>) Class.forName(WRITE_SERVICE);
    }

    @Override
    public VertexProgram<Tuple> clone() {
        return this;
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.EDGES;
    }

    @Override
    public Features getFeatures() {
        return new Features() {
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
    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(DropVertexProgram.class);
        }
    }
}
