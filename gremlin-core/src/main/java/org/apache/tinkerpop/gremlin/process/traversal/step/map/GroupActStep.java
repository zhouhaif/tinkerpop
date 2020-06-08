package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByPrevious;
import org.apache.tinkerpop.gremlin.util.function.ConcurrentHashMapSupplier;

import java.util.Map;

/**
 * @author Think
 * @date 2020/3/30 14:52
 **/
public class GroupActStep<S, K, V> extends ByPrevious<S, Map<K, V>> {

    private Traversal.Admin<S, V> valueTraversal;

    public GroupActStep(Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier(ConcurrentHashMapSupplier.instance());
    }


    @Override
    public Map<K, V> projectTraverser(Traverser.Admin<S> traverser) {
//        Map<K, V> map = (Map<K, V>)traverser.get();
        return (Map<K, V>) traverser.get();
    }

    @Override
    public Map<K, V> generateFinalResult(final Map<K, V> object) {
        if (this.valueTraversal == null)
            this.valueTraversal = ((GroupVStep) this.previousStep).getValueTraversal();
        return GroupVStep.doFinalReduction((Map<K, Object>) object, this.valueTraversal);
    }

    public static <K, V> Map<K, V> doFinalReduction(final Map<K, Object> map, final Traversal.Admin<?, V> valueTraversal) {
        final Barrier barrierStep = GroupVStep.determineBarrierStep(valueTraversal);
        if (barrierStep != null) {
            for (final K key : map.keySet()) {
                valueTraversal.reset();
                barrierStep.addBarrier(map.get(key));
                if (valueTraversal.hasNext())
                    map.put(key, valueTraversal.next());
            }
        }
        return (Map<K, V>) map;
    }
}
