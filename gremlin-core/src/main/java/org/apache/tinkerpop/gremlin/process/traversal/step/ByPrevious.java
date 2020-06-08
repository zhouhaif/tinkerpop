package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupVStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author zhf
 */
public abstract class ByPrevious<S, E> extends AbstractStep<S, E> implements Barrier<E>, Generating<E, E> {
    protected Supplier<E> seedSupplier;
    protected BinaryOperator<E> reducingBiOperator;
    private boolean hasProcessedOnce = false;
    private E seed = null;

    public ByPrevious(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setSeedSupplier(final Supplier<E> seedSupplier) {
        this.seedSupplier = seedSupplier;
    }

    public Supplier<E> getSeedSupplier() {
        return this.seedSupplier;
    }

    public abstract E projectTraverser(final Traverser.Admin<S> traverser);

    public void setReducingBiOperator(final BinaryOperator<E> reducingBiOperator) {
        this.reducingBiOperator = reducingBiOperator;
    }

    public BinaryOperator<E> getBiOperator() {
        return this.reducingBiOperator;
    }

    @Override
    public void reset() {
        super.reset();
        this.hasProcessedOnce = false;
        this.seed = null;
    }

    @Override
    public void done() {
        this.hasProcessedOnce = true;
        this.seed = null;
    }

    @Override
    public void processAllStarts() {
        if (this.hasProcessedOnce && !this.starts.hasNext()) {
            return;
        }
        if(this.getBiOperator()==null){
            this.setReducingBiOperator((((ByVertex)this.previousStep).getBiOperator()));
        }
        this.hasProcessedOnce = true;
        if (this.seed == null) {
            this.seed = this.seedSupplier.get();
        }
        while (this.starts.hasNext()){
            this.seed = this.reducingBiOperator.apply(this.seed, this.projectTraverser(this.starts.next()));
        }
    }

    @Override
    public boolean hasNextBarrier() {
        this.processAllStarts();
        return null != this.seed;
    }

    @Override
    public E nextBarrier() {
        if (!this.hasNextBarrier()) {
            throw FastNoSuchElementException.instance();
        }
        else {
            final E temp = this.seed;
            this.seed = null;
            return temp;
        }
    }

    @Override
    public void addBarrier(final E barrier) {
        this.seed = null == this.seed ?
                barrier :
                this.reducingBiOperator.apply(this.seed, barrier);
    }

    @Override
    public Traverser.Admin<E> processNextStart() {
        this.processAllStarts();
        if (this.seed == null){
            throw FastNoSuchElementException.instance();
        }
        final Traverser.Admin<E> traverser = this.getTraversal().getTraverserGenerator().generate(this.generateFinalResult(this.seed), (Step<E, E>) this, 1L);
        this.seed = null;
        return traverser;
    }

    @Override
    public ByPrevious<S, E> clone() {
        final ByPrevious<S, E> clone = (ByPrevious<S, E>) super.clone();
        clone.hasProcessedOnce = false;
        clone.seed = null;
        return clone;
    }

    @Override
    public MemoryComputeKey<E> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getId(), this.getBiOperator(), false, true);
    }
}
