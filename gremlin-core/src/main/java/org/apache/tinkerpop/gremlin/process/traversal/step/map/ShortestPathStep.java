package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;


public class ShortestPathStep<S> extends MapStep<S, Path> {

    //    private Long fromIds;
    private Long toIds;
    private Integer distance;
    protected Map<Object, supportParent> source = new HashMap<>();
    protected Map<Object, supportParent> target = new HashMap<>();
//    protected Map<Object, Vertex> parent = new HashMap<>();
//    protected Map<Object, Vertex> backParent = new HashMap<>();

    public ShortestPathStep(final Traversal.Admin traversal, final Long toIds, final Integer distance) {
        super(traversal);
        this.toIds = toIds;
        this.distance = distance;
    }

//    public void addFrom(final Long fromIds) {
//        this.fromIds = fromIds;
//    }

//    public void addTo(final Long toIds) {
//        this.toIds = toIds;
//    }

//    public void addDistance(final Integer distance) {
//        this.distance = distance;
//    }

    @Override
    protected Traverser.Admin<Path> processNextStart() {
        return PathProcessor.processTraverserPathLabels(super.processNextStart(), null);
    }

    protected Path map(final Traverser.Admin<S> traverser) {
        final Path byPath = MutablePath.make();
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
            byPath.extend(to, s);
            return byPath;
        }
        source.put(from.id(), new supportParent((Long) from.id(), from, null, null));
        target.put(to.id(), new supportParent((Long) to.id(), to, null, null));
        while (true) {
            if (forward(byPath) || --distance < 0) {
                break;
            }
            if (backward(byPath) || --distance < 0) {
                break;
            }
        }
        return byPath;

    }

    private boolean forward(Path byPath) {
        Map<Object, supportParent> newVertices = new HashMap<>();
        for (supportParent s : source.values()) {
            Vertex v = s.current;
            Iterator<Edge> edges = v.edges(Direction.BOTH);
            while (edges.hasNext()) {
                Edge e = edges.next();
                Vertex from = e.outVertex();
                Vertex to = e.inVertex();
                if (target.containsKey(from.id())) {
                    path(s, target.get(from.id()), e, byPath);
                    return true;
                } else if (target.containsKey(to.id())) {
                    path(s, target.get(to.id()), e, byPath);
                    return true;
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
        source = newVertices;
        return false;
    }

    private boolean backward(Path byPath) {
        Map<Object, supportParent> newVertices = new HashMap<>();
        for (supportParent s : target.values()) {
            Vertex v = s.current;
            Iterator<Edge> edges = v.edges(Direction.BOTH);
            while (edges.hasNext()) {
                Edge e = edges.next();
                Vertex from = e.outVertex();
                Vertex to = e.inVertex();
                if (source.containsKey(from.id())) {
                    path(source.get(from.id()), s, e, byPath);
                    return true;
                } else if (source.containsKey(to.id())) {
                    path(source.get(to.id()), s, e, byPath);
                    return true;
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

    private Path path(supportParent f, supportParent b, Edge e, Path bypath) {
        toPath(f, bypath);
        Set s = newSet(e.label());
        bypath.extend(e,s);
        while (b.parent != null) {
            s = newSet(b.current.label());
            bypath.extend(b.current, s);
            s= newSet(b.currentEdge.label());
            bypath.extend(b.currentEdge,s);
            b = b.parent;
        }
        s = newSet(b.current.label());
        bypath.extend(b.current, s);
        return bypath;
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
