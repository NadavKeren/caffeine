package com.github.benmanes.caffeine.cache.simulator.DebugHelpers;

import java.util.function.Supplier;

public class Assert {
    public static void assertCondition(boolean cond, Supplier<String> msgSupplier) {
        if (!cond) {
            throw new IllegalStateException(msgSupplier.get());
        }
    }
}
