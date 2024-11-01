package com.github.benmanes.caffeine.cache.simulator.DebugHelpers;

import java.util.function.Supplier;
import java.lang.System.Logger;

public class Assert {
    public static void assertCondition(boolean cond, String msg) {
        if (!cond) {
            Logger logger = System.getLogger("");
            logger.log(Logger.Level.ERROR, msg);
            System.err.println(msg);
            throw new IllegalStateException(msg);
        }
    }

    public static void assertCondition(boolean cond, Supplier<String> msgSupplier) {
        if (!cond) {
            Logger logger = System.getLogger("");
            String msg = msgSupplier.get();
            logger.log(Logger.Level.ERROR, msg);
            System.err.println(msgSupplier.get());
            throw new IllegalStateException(msg);
        }
    }
}
