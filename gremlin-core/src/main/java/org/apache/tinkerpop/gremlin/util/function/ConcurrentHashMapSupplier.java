package org.apache.tinkerpop.gremlin.util.function;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Created by Think on 2019/8/2.
 */
public class ConcurrentHashMapSupplier<K, V> implements Supplier<Map<K, V>>, Serializable {
    private static final ConcurrentHashMapSupplier INSTANCE = new ConcurrentHashMapSupplier();

    private ConcurrentHashMapSupplier() {
    }

    @Override
    public ConcurrentHashMap<K, V> get() {
        return new ConcurrentHashMap<>();
    }

    public static <K, V> ConcurrentHashMapSupplier<K, V> instance() {
        return INSTANCE;
    }
}
