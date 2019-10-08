package org.apache.tinkerpop.gremlin.process.udf;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Created by Think on 2019/9/25.
 * @author Think
 */

public abstract class SerializableFunction<T, O> implements Function<T, O>,Serializable {
    private static final long serialVersionUID = -4940539829102830999L;

    @Override
    public abstract O apply(T o);
}
