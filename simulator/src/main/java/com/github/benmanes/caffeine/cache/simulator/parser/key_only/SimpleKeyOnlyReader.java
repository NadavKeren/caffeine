package com.github.benmanes.caffeine.cache.simulator.parser.key_only;

import com.github.benmanes.caffeine.cache.simulator.parser.TextTraceReader;
import com.github.benmanes.caffeine.cache.simulator.parser.TraceReader.KeyOnlyTraceReader;

import java.util.stream.LongStream;
import java.math.BigInteger;

public class SimpleKeyOnlyReader extends TextTraceReader implements KeyOnlyTraceReader {
    public SimpleKeyOnlyReader(String filePath) {
        super(filePath);
    }

    private long parseKey(String uuid) {
        try {
            return Long.parseLong(uuid);
        } catch (RuntimeException e) {
            BigInteger num = new BigInteger(uuid);
            return num.shiftRight(64).longValue() ^ num.longValue();
        }
    }

    @Override
    public LongStream keys() {
        return lines()
                .filter(line -> !line.isEmpty())
                .mapToLong(this::parseKey);
    }
}
