package com.github.benmanes.caffeine.cache.simulator.admission.summin64;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;

import java.util.Random;

public class SumMin64 {
    static final private long PRIME_MODULUS = (1L << 31) - 1;

    final private double[][] table;
    final private long[] hashA;
    final private int depth;
    final private int width;
    final private int widthMask;

    public SumMin64(double eps, double confidence, int seed) {
        // 2/w = eps ; w = 2/eps
        // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
        // Aligning width and depth to exponents of 2 in order to perform the calculations bitwise
        Assert.assertCondition(eps > 0, () -> String.format("Illegal epsilon: %f", eps));
        Assert.assertCondition(confidence >= 0, () -> String.format("Illegal confidence: %f", confidence));
        int widthExp = findExponent((int) Math.ceil(2 / eps));
        this.width = 1 << widthExp;
        this.widthMask = createMask(widthExp);

        int maxValueAtMask = Integer.MAX_VALUE & this.widthMask;
        Assert.assertCondition(maxValueAtMask == (this.width - 1), "Bad width mask");

        int depthExp = findExponent((int) Math.ceil(-Math.log(1 - confidence) / Math.log(2)));
        this.depth = 1 << depthExp;

        this.table = new double[depth][width];

        // We're using a linear hash functions of the form ((a*x+b) mod p) where a,b are chosen
        // independently for each hash function. However, we can set b = 0 as all it does is shift the
        // results without compromising their uniformity or independence with the other hashes.
        hashA = new Random(seed).longs(depth, 0, Integer.MAX_VALUE).toArray();
    }

    private static int findExponent(int num) {
        int exp = 0;
        --num;
        while (num > 0) {
            ++exp;
            num >>= 1;
        }

        return exp;
    }

    private static int createMask(int exp) {
        int mask = 0;
        for (int i = 0; i < exp; ++i) {
            mask = mask << 1 | 1;
        }

        return mask;
    }

    public void decay(double decayFactor) {
        Assert.assertCondition(decayFactor >= 0 && decayFactor <= 1, () -> String.format("Illegal decay factor: %f", decayFactor));
        final double decayMult = 1 - decayFactor;
        for (int i = 0; i < depth; ++i) {
            for (int j = 0; j < width; ++j) {
                table[i][j] *= decayMult;
            }
        }
    }

    public double estimate(long item) {
        double value = Double.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            value = Math.min(value, table[i][hash(item, i)]);
        }

        return value;
    }

    public void set(long item, double value) {
        for (int i = 0; i < depth; ++i) {
            int loc = hash(item, i);
            double currValue = table[i][loc];
            table[i][loc] = Math.max(currValue, value);
        }
    }

    private int hash(long item, int i) {
        long hash = hashA[i] * item;
        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        // Using the fact that width == 2^k to use bitwise operations
        return ((int) hash) & widthMask;
    }
}
