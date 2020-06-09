package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;


public class ShortestPathStep<S> extends FlatMapStep<S, Path> implements TraversalParent, ByModulating, PathProcessor {

    private Long toIds;
    private Integer distance;
    private Set<String> keepLabels;
    protected Map<Object, supportParent> source = new HashMap<>();
    protected Map<Object, supportParent> target = new HashMap<>();
    private TraversalRing<Object, Object> traversalRing;


    public ShortestPathStep(final Traversal.Admin traversal, final Long toIds, final Integer distance) {
        super(traversal);
        this.toIds = toIds;
        this.distance = distance;
        this.traversalRing = new TraversalRing<>();
    }

    @Override
    protected Traverser.Admin<Path> processNextStart() {
        return PathProcessor.processTraverserPathLabels(super.processNextStart(), this.keepLabels);
    }

    @Override
    protected Iterator<Path> flatMap(Traverser.Admin<S> traverser) {
        {
            traversalRing.reset();
            Vertex from = (Vertex) traverser.get();
            Vertex to;
            Iterator<Vertex> itTo = this.getTraversal().getGraph().get().vertices(this.toIds);
            if (null != itTo && itTo.hasNext()) {
                to = itTo.next();
            } else {
                return null;
            }

            if (null != from && from.equals(to)) {
                Set s = newSet(to.label());
                final Path byPath = MutablePath.make();
                final Path rePath = MutablePath.make();
                byPath.extend(to, s);
                byPath.forEach((object, labels) -> rePath.extend(TraversalUtil.applyNullable(object, this.traversalRing.next()), labels));
                return Collections.singletonList(rePath).iterator();
            }
            Set<Path> byPaths = new HashSet<>();
            Set<Path> rePaths = new HashSet<>();
            source.put(from.id(), new supportParent((Long) from.id(), from, null, null));
            target.put(to.id(), new supportParent((Long) to.id(), to, null, null));
            while (true) {
                if (forward(byPaths) || --distance < 0) {
                    break;
                }
                if (backward(byPaths) || --distance < 0) {
                    break;
                }
            }
            byPaths.forEach(byPath -> {
                Path rePath = MutablePath.make();
                byPath.forEach((object, labels) -> rePath.extend(TraversalUtil.applyNullable(object, this.traversalRing.next()), labels));
                rePaths.add(rePath);
            });

            return rePaths.iterator();

        }
    }

    private boolean forward(Set<Path> byPaths) {
        Map<Object, supportParent> newVertices = new HashMap<>();
        boolean flag = false;
        for (supportParent s : source.values()) {
            Vertex v = s.current;
            Iterator<Edge> edges = v.edges(Direction.BOTH);
            while (edges.hasNext()) {
                Edge e = edges.next();
                Vertex from = e.outVertex();
                Vertex to = e.inVertex();
                if (target.containsKey(from.id())) {
                    path(s, target.get(from.id()), e, byPaths);
                    flag = true;
                } else if (target.containsKey(to.id())) {
                    path(s, target.get(to.id()), e, byPaths);
                    flag = true;
                }
                if (from.equals(v)) {
                    if (!source.containsKey((to.id())) &&
                            !newVertices.containsKey(to.id()) &&
                            notLoop(to, s)) {
                        newVertices.put(to.id(), new supportParent((Long) to.id(), to, e, s));
                    }
                } else if (to.equals(v)) {
                    if (!source.containsKey(from.id()) &&
                            !newVertices.containsKey(from.id()) &&
                            notLoop(from, s)) {
                        newVertices.put(from.id(), new supportParent((Long) from.id(), from, e, s));
                    }
                }

            }

        }
        if (flag) return true;
        source = newVertices;
        return false;
    }

    private boolean backward(Set<Path> byPaths) {
        Map<Object, supportParent> newVertices = new HashMap<>();
        boolean flag = true;
        for (supportParent s : target.values()) {
            Vertex v = s.current;
            Iterator<Edge> edges = v.edges(Direction.BOTH);
            while (edges.hasNext()) {
                Edge e = edges.next();
                Vertex from = e.outVertex();
                Vertex to = e.inVertex();
                if (source.containsKey(from.id())) {
                    path(source.get(from.id()), s, e, byPaths);
                    flag = true;
                } else if (source.containsKey(to.id())) {
                    path(source.get(to.id()), s, e, byPaths);
                    flag = true;
                }
                if (from.equals(v)) {
                    if (!target.containsKey((to.id())) &&
                            !newVertices.containsKey(to.id()) &&
                            notLoop(to, s)) {
                        newVertices.put(to.id(), new supportParent((Long) to.id(), to, e, s));
                    }
                } else if (to.equals(v)) {
                    if (!target.containsKey(from.id()) &&
                            !newVertices.containsKey(from.id()) &&
                            notLoop(from, s)) {
                        newVertices.put(from.id(), new supportParent((Long) from.id(), from, e, s));
                    }
                }
            }
        }
        if (flag) return true;
        target = newVertices;
        return false;
    }

    private Path toPath(supportParent f, Path bypath) {
        if (f.parent == null) {
            Set s = newSet(f.current.label());
            bypath.extend(f.current, s);
            return bypath;
        } else {
            toPath(f.parent, bypath);
            Set s = newSet(f.currentEdge.label());
            bypath.extend(f.currentEdge, s);
            s = newSet(f.current.label());
            bypath.extend(f.current, s);
        }
        return bypath;
    }

    private void path(supportParent f, supportParent b, Edge e, Set<Path> byPaths) {
        Path byPath = MutablePath.make();
        toPath(f, byPath);
        Set s = newSet(e.label());
        byPath.extend(e, s);
        while (b.parent != null) {
            s = newSet(b.current.label());
            byPath.extend(b.current, s);
            s = newSet(b.currentEdge.label());
            byPath.extend(b.currentEdge, s);
            b = b.parent;
        }
        s = newSet(b.current.label());
        byPath.extend(b.current, s);
        byPaths.add(byPath);
    }

    private boolean notLoop(Vertex v, supportParent s) {
        while (s.parent != null) {
            if (v.id().equals(s.parent.id)) return false;
            s = s.parent;
        }
        return true;
    }

    private HashSet<Object> newSet(Object o) {
        HashSet<Object> set = new HashSet<>();
        set.add(o);
        return set;
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> pathTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(pathTraversal));
    }

    @Override
    public void setKeepLabels(final Set<String> keepLabels) {
        this.keepLabels = new HashSet<>(keepLabels);
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }

    @Override
    public ShortestPathStep<S> clone() {
        final ShortestPathStep<S> clone = (ShortestPathStep<S>) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.PATH);
    }
}

class supportParent {
    supportParent parent;
    Vertex current;
    Edge currentEdge;
    Long id;

    protected supportParent(Long id, Vertex current, Edge currentEdge, supportParent parent) {
        this.id = id;
        this.current = current;
        this.currentEdge = currentEdge;
        this.parent = parent;
    }
}
