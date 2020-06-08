package org.apache.tinkerpop.gremlin.process.traversal.traverser;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Map;
import java.util.Set;

/**
 * @author Think
 * @date 2020/3/31 20:27
 **/
public class GroupVTraverser<T> extends B_LP_O_S_SE_SL_Traverser<T> {
    protected GroupVTraverser() {
    }

    public GroupVTraverser(final T t, final Step<T, ?> step, final long initialBulk) {
        super(t, step, initialBulk);
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public void merge(final Admin<?> other) {
        super.merge(other);
        Object t1 = this.get();
        Object t2 = other.get();
        if (t1 instanceof Map && t2 instanceof Map) {
            for (Object key : ((Map) t1).keySet()) {
                Object value1 = ((Map) t1).get(key);
                Object value2 = ((Map) t2).get(key);
                if (value1 instanceof Long && value2 instanceof Long) {
                    long s = (Long) value1 + (Long) value2;
                    ((Map) t1).put(key, s);
                }
            }
        }
    }


}
