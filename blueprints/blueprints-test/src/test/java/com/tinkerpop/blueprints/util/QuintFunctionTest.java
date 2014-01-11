package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.util.function.QuintFunction;
import org.junit.Test;

import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class QuintFunctionTest {
    @Test
    public void shouldApplyCurrentFunctionAndThenAnotherSuppliedOne() {
        final QuintFunction<String, String, String, String, String, String> f = (a,b,c,d,e) -> a + b + c + d + e;
        final UnaryOperator<String> after = (s) -> s + "last";
        assertEquals("12345last", f.andThen(after).apply("1", "2", "3", "4", "5"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfAfterFunctionIsNull() {
        final QuintFunction<String, String, String, String, String, String> f = (a,b,c,d,e) -> a + b + c + e;
        f.andThen(null);
    }
}
