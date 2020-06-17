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
//    static Logger logger = LoggerFactory.getLogger(ShortestPathStep.class);
    private final long toIds;
    private final int lower;
    private int distance;
    private int degree = 0;
    private Set<String> keepLabels;
    private Map<Object, List<supportParent>> source = new HashMap<>();
    private Map<Object, List<supportParent>> target = new HashMap<>();
    private TraversalRing<Object, Object> traversalRing;


    public ShortestPathStep(final Traversal.Admin traversal, final long toIds, final int lower, final int distance) {
        super(traversal);
        this.toIds = toIds;
        this.lower = lower;
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
                Set<String> s = newSet(to.label());
                final Path byPath = MutablePath.make();
                final Path rePath = MutablePath.make();
                byPath.extend(to, s);
                byPath.forEach((object, labels) -> rePath.extend(TraversalUtil.applyNullable(object, this.traversalRing.next()), labels));
                return Collections.singletonList(rePath).iterator();
            }
            Set<Path> byPaths = new HashSet<>();
            Set<Path> rePaths = new HashSet<>();
            source.put(from.id(), Collections.singletonList(new supportParent((Long) from.id(), from, null, null)));
            target.put(to.id(), Collections.singletonList(new supportParent((Long) to.id(), to, null, null)));
            while (true) {
                if (forward(byPaths) || --distance < 1) {
                    break;
                }
                if (backward(byPaths) || --distance < 1) {
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
        degree++;
        Map<Object, List<supportParent>> newVertices = new HashMap<>();
        boolean flag = false;
        for (List<supportParent> list : source.values()) {
            for (supportParent s : list) {
                Vertex v = s.current;
                Iterator<Edge> edges = v.edges(Direction.BOTH);
                while (edges.hasNext()) {
                    Edge e = edges.next();
                    Vertex from = e.outVertex();
                    Vertex to = e.inVertex();
                    if (from.equals(v)) {
                        if (degree >= lower && target.containsKey(to.id())
                                && filterPath(Collections.singletonList(s), target.get(to.id()), e, byPaths)) {
                            flag = true;
                        }
                        if (!source.containsKey(to.id()) &&
//                                !newVertices.containsKey(to.id()) &&
                                notLoop(to, s)) {
                            addNewVertices(newVertices, s, e, to);
                        }
                    } else if (to.equals(v)) {
                        if (degree >= lower && target.containsKey(from.id())
                                && filterPath(Collections.singletonList(s), target.get(from.id()), e, byPaths)) {
                            flag = true;
                        }
                        if (!source.containsKey(from.id()) &&
//                                !newVertices.containsKey(from.id()) &&
                                notLoop(from, s)) {
                            addNewVertices(newVertices, s, e, from);
                        }
                    }
                }
            }
        }
        if (flag) return true;
        source = newVertices;
//        logger.info("第" + degree + "次查找,source key:" + source.keySet().toString());
        return false;
    }

    private boolean backward(Set<Path> byPaths) {
        degree++;
        Map<Object, List<supportParent>> newVertices = new HashMap<>();
        boolean flag = false;
        for (List<supportParent> list : target.values()) {
            for (supportParent s : list) {
                Vertex v = s.current;
                Iterator<Edge> edges = v.edges(Direction.BOTH);
                while (edges.hasNext()) {
                    Edge e = edges.next();
                    Vertex from = e.outVertex();
                    Vertex to = e.inVertex();
                    if (from.equals(v)) {
                        if (degree >= lower && source.containsKey(to.id())
                                && filterPath(source.get(to.id()), Collections.singletonList(s), e, byPaths)) {
//                        path(source.get(to.id()), s, e, byPaths);
                            flag = true;
                        }
                        if (!target.containsKey(to.id()) &&
//                                !newVertices.containsKey(to.id()) &&
                                notLoop(to, s)) {
//                            newVertices.put(to.id(), new supportParent((Long) to.id(), to, e, s));
                            addNewVertices(newVertices, s, e, to);
                        }
                    } else if (to.equals(v)) {
                        if (degree >= lower && source.containsKey(from.id())
                                && filterPath(source.get(from.id()), Collections.singletonList(s), e, byPaths)) {
//                        path(source.get(from.id()), s, e, byPaths);
                            flag = true;
                        }
                        if (!target.containsKey(from.id()) &&
//                                !newVertices.containsKey(from.id()) &&
                                notLoop(from, s)) {
                            addNewVertices(newVertices, s, e, from);
                        }
                    }
                }
            }
        }
        if (flag) return true;
        target = newVertices;
//        logger.info("第" + degree + "次查找,target key:" + target.keySet().toString());
        return false;
    }

    private void addNewVertices(Map<Object, List<supportParent>> newVertices, supportParent s, Edge e, Vertex v) {
        List<supportParent> temp;
        if ((temp = newVertices.get(v.id())) == null) {
            newVertices.put(v.id(), new ArrayList<>(Collections.singletonList(new supportParent((Long) v.id(), v, e, s))));
        } else {
            temp.add(new supportParent((Long) v.id(), v, e, s));
            newVertices.put(v.id(), temp);
        }
    }

    private boolean filterPath(List<supportParent> f, List<supportParent> b, Edge e, Set<Path> byPaths) {
        boolean flag = false;
        for (supportParent forward : f) {
            for (supportParent backward : b) {
                //满足下限步长
                if ((countLength(forward) + countLength(backward) + 1) >= lower) {
                    //筛选成环
                    //a->b->c->b->d     c->b->a     b->d
                    //a->b->c->a->d     c->b->a     a->d
                    //a->b->c           a->b        c
                    Set<Long> forwardSet = traceSupport(forward);
                    Set<Long> backwardSet = traceSupport(backward);
//                    logger.info("forward:" + forwardSet.toString() + "|backward:" + backwardSet.toString());
                    forwardSet.retainAll(backwardSet);
                    if (forwardSet.isEmpty()) {
                        //构建路径
                        path(forward, backward, e, byPaths);
                        flag = true;
                    }
                }
            }
        }
        return flag;
    }

    private Set<Long> traceSupport(supportParent support) {
        Set<Long> s = new HashSet<>();
        supportParent current = support;
        while (current != null) {
            s.add(current.id);
            current = current.parent;
        }
        return s;
    }

    private int countLength(final supportParent support) {
        int length = 0;
        supportParent current = support;
        while (current.parent != null) {
            length++;
            current = current.parent;
        }
        return length;
    }

    private void path(supportParent f, supportParent b, Edge e, Set<Path> byPaths) {
        Path byPath = MutablePath.make();
        toPath(f, byPath);
        Set<String> s = newSet(e.label());
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

    private void toPath(supportParent f, Path bypath) {
        if (f.parent == null) {
            Set<String> s = newSet(f.current.label());
            bypath.extend(f.current, s);
        } else {
            toPath(f.parent, bypath);
            Set<String> s = newSet(f.currentEdge.label());
            bypath.extend(f.currentEdge, s);
            s = newSet(f.current.label());
            bypath.extend(f.current, s);
        }
    }

    private boolean notLoop(Vertex v, supportParent s) {
        while (s.parent != null) {
            if (v.id().equals(s.parent.id)) return false;
            s = s.parent;
        }
        return true;
    }

    private HashSet<String> newSet(String o) {
        HashSet<String> set = new HashSet<>();
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
