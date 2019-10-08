package org.apache.tinkerpop.gremlin.process.udf;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * @author Think
 */
public final class UdfFunctions implements Serializable{
    private static final long serialVersionUID = -4940539829102830928L;
    private UdfFunctions() {
    }

    public static SerializableFunction<Traverser<Object>, String> castToString() {
        return new SerializableFunction<Traverser<Object>, String>() {
            @Override
            public String apply(Traverser<Object> traverser) {
                Object arg = traverser.get();
                boolean valid = arg == null ||
                        arg instanceof Boolean ||
                        arg instanceof Number ||
                        arg instanceof String;
                if (!valid) {
                    String className = arg.getClass().getName();
                    throw new TypeException("Cannot convert " + className + " to string");
                }
                return Optional.ofNullable(arg)
                        .map(String::valueOf).orElse(null);
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, Boolean> castToBoolean() {
        return new SerializableFunction<Traverser<Object>, Boolean>() {
            @Override
            public Boolean apply(Traverser<Object> traverser) {
                Object arg = traverser.get();
                boolean valid = arg == null ||
                        arg instanceof Boolean ||
                        arg instanceof String;
                if (!valid) {
                    String className = arg.getClass().getName();
                    throw new TypeException("Cannot convert " + className + " to boolean");
                }

                return Optional.ofNullable(arg)
                        .map(String::valueOf)
                        .map(v -> {
                            switch (v.toLowerCase()) {
                                case "true":
                                    return true;
                                case "false":
                                    return false;
                                default:
                                    return null;
                            }
                        }).orElse(null);
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, Integer> castToInteger() {
        return new SerializableFunction<Traverser<Object>, Integer>() {
            @Override
            public Integer apply(Traverser<Object> traverser) {
                Object arg = traverser.get();
                boolean valid = arg == null ||
                        arg instanceof Number ||
                        arg instanceof String;
                if (!valid) {
                    String className = arg.getClass().getName();
                    throw new TypeException("Cannot convert " + className + " to integer");
                }

                return Optional.ofNullable(arg)
                                .map(String::valueOf)
                                .map(v -> {
                                    try {
                                        return Integer.valueOf(v);
                                    } catch (NumberFormatException e1) {
                                        try {
                                            return Integer.valueOf(v).intValue();
                                        } catch (NumberFormatException e2) {
                                            return null;
                                        }
                                    }
                                })
                                .orElse(null);
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, Double> castToDouble() {
        return new SerializableFunction<Traverser<Object>, Double>() {
            @Override
            public Double apply(Traverser<Object> traverser) {
                Object arg = traverser.get();
                boolean valid = arg == null ||
                        arg instanceof Number ||
                        arg instanceof String;
                if (!valid) {
                    String className = arg.getClass().getName();
                    throw new TypeException("Cannot convert " + className + " to double");
                }

                return Optional.ofNullable(arg)
                                .map(String::valueOf)
                                .map(v -> {
                                    try {
                                        return Double.valueOf(v);
                                    } catch (NumberFormatException e) {
                                        return null;
                                    }
                                })
                                .orElse(null);
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, Long> castToLong() {
        return new SerializableFunction<Traverser<Object>, Long>() {
            @Override
            public Long apply(Traverser<Object> traverser) {
                Object arg = traverser.get();
                boolean valid = arg == null ||
                        arg instanceof Number ||
                        arg instanceof String;
                if (!valid) {
                    String className = arg.getClass().getName();
                    throw new TypeException("Cannot convert " + className + " to long");
                }

                return Optional.ofNullable(arg)
                                .map(String::valueOf)
                                .map(v -> {
                                    try {
                                        return Long.valueOf(v);
                                    } catch (NumberFormatException e) {
                                        return null;
                                    }
                                })
                                .orElse(null);
            }
        };
    }

    public static SerializableFunction<Traverser<Object>,Object> mathRound() {
        return commonFunction(a -> (Math.round((Double) a.get(0))), Double.class);
    }

    public static SerializableFunction<Traverser<Object>, Map> allProperties() {
        return new SerializableFunction<Traverser<Object>, Map>() {
            @Override
            public Map apply(Traverser<Object> traverser) {
                Object argument = traverser.get();

                if (argument instanceof Map) {
                    return (Map)argument;
                }
                Iterator<? extends Property<Object>> it = ((Element) argument).properties();
                Map<Object, Object> propertyMap = new HashMap<>(16);
                while (it.hasNext()) {
                    Property<Object> property = it.next();
                    propertyMap.putIfAbsent(property.key(), property.value());
                }
                return propertyMap;
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, Object> containerIndex() {
        return new SerializableFunction<Traverser<Object>, Object>() {
            @Override
            public Object apply(Traverser<Object> traverser) {
                List<?> args = (List<?>) traverser.get();
                Object container = args.get(0);
                Object index = args.get(1);
                if (container instanceof List) {
                    List list = (List) container;
                    int size = list.size();
                    int i = normalizeContainerIndex(index, size);
                    if (i < 0 || i > size) {
                        return null;
                    }
                    return list.get(i);
                }

                if (container instanceof Map) {
                    if (!(index instanceof String)) {
                        String indexClass = index.getClass().getName();
                        throw new IllegalArgumentException("Map element access by non-string: " + indexClass);
                    }
                    Map map = (Map) container;
                    String key = (String) index;
                    return map.get(key);
                }

                if (container instanceof Element) {
                    if (!(index instanceof String)) {
                        String indexClass = index.getClass().getName();
                        throw new IllegalArgumentException("Property access by non-string: " + indexClass);
                    }
                    Element element = (Element) container;
                    String key = (String) index;
                    return element.property(key).orElse(null);
                }

                String containerClass = container.getClass().getName();
                if (index instanceof String) {
                    throw new IllegalArgumentException("Invalid property access of " + containerClass);
                }
                throw new IllegalArgumentException("Invalid element access of " + containerClass);
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, List> listSlice() {
        return new SerializableFunction<Traverser<Object>, List>() {
            @Override
            public List apply(Traverser<Object> traverser) {
                List<?> args = (List<?>) traverser.get();
                Object container = args.get(0);
                Object from = args.get(1);
                Object to = args.get(2);

                if (container instanceof List) {
                    List list = (List) container;
                    int size = list.size();
                    int f = normalizeRangeIndex(from, size);
                    int t = normalizeRangeIndex(to, size);
                    if (f >= t) {
                        return new ArrayList<>();
                    }
                    return list.subList(f, t);
                }

                String containerClass = container.getClass().getName();
                throw new IllegalArgumentException(
                        "Invalid element access of " + containerClass + " by range"
                );
            }
        };
    }

    private static int normalizeContainerIndex(Object index, int containerSize) {
        if (!(index instanceof Number)) {
            String indexClass = index.getClass().getName();
            throw new IllegalArgumentException("List element access by non-integer: " + indexClass);
        }
        int i = ((Number) index).intValue();
        return (i >= 0) ? i : containerSize + i;
    }

    private static int normalizeRangeIndex(Object index, int size) {
        int i = normalizeContainerIndex(index, size);
        if (i < 0) {
            return 0;
        }
        if (i > size) {
            return size;
        }
        return i;
    }

    public static SerializableFunction<Traverser<Object>, Object> cypherPercentileCont() {
        return percentileFunction(
                (data, percentile) -> {
                    int last = data.size() - 1;
                    double lowPercentile = Math.floor(percentile * last) / last;
                    double highPercentile = Math.ceil(percentile * last) / last;
                    if (lowPercentile == highPercentile) {
                        return percentileNearest(data, percentile);
                    }
                    double scale = (percentile - lowPercentile) / (highPercentile - lowPercentile);
                    double low = percentileNearest(data, lowPercentile).doubleValue();
                    double high = percentileNearest(data, highPercentile).doubleValue();
                    return (high - low) * scale + low;
                }
        );
    }

    public static Function<Traverser<Object>, Object> cypherPercentileDisc() {
        return percentileFunction(
                UdfFunctions::percentileNearest
        );
    }

    private static SerializableFunction<Traverser<Object>, Object> percentileFunction(BiFunction<List<Number>, Double, Number> percentileStrategy) {
        return new SerializableFunction<Traverser<Object>, Object>() {
            @Override
            public Object apply(Traverser<Object> traverser) {
                List<?> args = (List<?>) traverser.get();

                double percentile = ((Number) args.get(1)).doubleValue();
                if (percentile < 0 || percentile > 1) {
                    throw new IllegalArgumentException("Number out of range: " + percentile);
                }

                Collection<?> coll = (Collection<?>) args.get(0);
                boolean invalid = coll.stream()
                        .anyMatch(o -> !(o == null || o instanceof Number));
                if (invalid) {
                    throw new IllegalArgumentException("Percentile function can only handle numerical values");
                }
                List<Number> data = coll.stream()
                        .filter(Objects::nonNull)
                        .map(o -> (Number) o)
                        .sorted()
                        .collect(toList());

                int size = data.size();
                if (size == 0) {
                    return null;
                } else if (size == 1) {
                    return data.get(0);
                }

                return percentileStrategy.apply(data, percentile);
            }
        };
    }

    private static <T> T percentileNearest(List<T> sorted, double percentile) {
        int size = sorted.size();
        int index = (int) Math.ceil(percentile * size) - 1;
        if (index == -1) {
            index = 0;
        }
        return sorted.get(index);
    }

    public static SerializableFunction<Traverser<Object>, Long> size() {
        return new SerializableFunction<Traverser<Object>, Long>() {
            @Override
            public Long apply(Traverser<Object> traverser) {
                return traverser.get() instanceof String ?
                        (long) ((String) traverser.get()).length() :
                        (long) ((Collection) traverser.get()).size();
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, Object> plus() {
        return new SerializableFunction<Traverser<Object>, Object>() {
            @Override
            public Object apply(Traverser<Object> traverser) {
                List<?> args = (List<?>) traverser.get();
                Object a = args.get(0);
                Object b = args.get(1);
                if (a instanceof List || b instanceof List) {
                    List<Object> objects = new ArrayList<>();
                    if (a instanceof List) {
                        objects.addAll((List<?>) a);
                    } else {
                        objects.add(a);
                    }
                    if (b instanceof List) {
                        objects.addAll((List<?>) b);
                    } else {
                        objects.add(b);
                    }
                    return objects;
                }

                if (!(a instanceof String || a instanceof Number) ||
                        !(b instanceof String || b instanceof Number)) {
                    throw new TypeException("Illegal use of plus operator");
                }

                if (a instanceof Number && b instanceof Number) {
                    if (a instanceof Double || b instanceof Double ||
                            a instanceof Float || b instanceof Float) {
                        return ((Number) a).doubleValue() + ((Number) b).doubleValue();
                    } else {
                        return ((Number) a).longValue() + ((Number) b).longValue();
                    }
                } else {
                    return String.valueOf(a) + String.valueOf(b);
                }
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, Object> reverse() {
        return new SerializableFunction<Traverser<Object>, Object>() {
            @Override
            public Object apply(Traverser<Object> traverser) {
                Object o = traverser.get();
                if (o instanceof Collection) {
                    ArrayList result = new ArrayList((Collection) o);
                    Collections.reverse(result);
                    return result;
                } else if (o instanceof String) {
                    return new StringBuilder((String) o).reverse().toString();
                } else {
                    throw new TypeException(format("Expected a string or list value for reverse, but got: %s(%s)",
                            o.getClass().getSimpleName(), o));
                }
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, String> subString(Integer b,Integer... c) {
        return new SerializableFunction<Traverser<Object>, String>() {
            @Override
            public String apply(Traverser<Object> traverser) {
                Object args = traverser.get();
                if (!(args instanceof String)) {
                    throw new TypeException(format("Expected substring(String, Integer, [Integer]), but got: (%s, %s)",
                            args, b));
                } else if (c.length == 1) {
                    String s = (String) args;
                    int endIndex = b + c[0];
                    endIndex = endIndex > s.length() ? s.length() : endIndex;
                    return s.substring(b, endIndex);
                } else {
                    return ((String) args).substring(b);
                }
            }
        };
    }

    private static SerializableFunction<Traverser<Object>, Object> commonFunction(Function<List, Object> func, Class<?>... clazzes) {
        return new SerializableFunction<Traverser<Object>, Object>() {
            @Override
            public Object apply(Traverser<Object> traverser) {
                List args = traverser.get() instanceof List ? ((List) traverser.get()) : asList(traverser.get());

                for (int i = 0; i < clazzes.length; i++) {
                    if (!clazzes[i].isInstance(args.get(i))) {
                        throw new TypeException(format("Expected a %s value for <function1>, but got: %s(%s)",
                                clazzes[i].getSimpleName(),
                                args.get(i).getClass().getSimpleName(),
                                args.get(i)));
                    }
                }

                return func.apply(args);
            }
        };
    }

    public static SerializableFunction<Traverser<Object>,String> trim() {
        return new SerializableFunction<Traverser<Object>, String>() {
            @Override
            public String apply(Traverser<Object> traverser) {
                Object args = traverser.get();
                if (args instanceof String) {
                    return ((String) args).trim();
                } else{
                    throw new TypeException(format("Call trim method error, Class %s",
                            args.getClass()));
                }
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, String> toUpper() {
        return new SerializableFunction<Traverser<Object>, String>() {
            @Override
            public String apply(Traverser<Object> traverser) {
                Object args = traverser.get();
                if (args instanceof String) {
                    return ((String) args).toUpperCase();
                } else{
                    throw new TypeException(format("Call toUpper method error, Class %s",
                            args.getClass()));
                }
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, String> toLower() {
        return new SerializableFunction<Traverser<Object>, String>() {
            @Override
            public String apply(Traverser<Object> traverser) {
                Object args = traverser.get();
                if (args instanceof String) {
                    return ((String) args).toLowerCase();
                } else{
                    throw new TypeException(format("Call toLower method error, Class %s",
                            args.getClass()));
                }
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, List<String>> split(String regex) {
        return new SerializableFunction<Traverser<Object>, List<String>>() {
            @Override
            public List<String> apply(Traverser<Object> traverser) {
                Object args = traverser.get();
                if (args instanceof String) {
                    return asList(((String) args).split(regex));
                } else{
                    throw new TypeException(format("Call split method error, Class %s",
                            args.getClass()));
                }
            }
        };
    }

    public static SerializableFunction<Traverser<Object>, String> replace(String target, String replacement) {
        return new SerializableFunction<Traverser<Object>, String>() {
            @Override
            public String apply(Traverser<Object> traverser) {
                Object args = traverser.get();
                if (args instanceof String) {
                    return ((String) args).replace(target,replacement);
                } else{
                    throw new TypeException(format("Call replace method error, Class %s",
                            args.getClass()));
                }
            }
        };
    }

    private static <T> T cast(Object o, Class<T> clazz) {
        if (clazz.isInstance(o)) {
            return clazz.cast(o);
        } else {
            throw new TypeException(format("Expected %s to be %s, but it was %s",
                    o, clazz.getSimpleName(), o.getClass().getSimpleName()));
        }
    }
}
