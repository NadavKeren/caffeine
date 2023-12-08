package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;

import java.util.function.Supplier;

@Policy.PolicySpec(name = "latency-aware.SampleErrorTest")
public class SampleErrorTestPolicy implements Policy {
    final private LongSampler sampler;
    final private PipelinePolicy mainPipeline;
    final private PipelinePolicy sampledPipeline;

    final private SampleErrorTestStats stats;
    final private int sampleOrder;

    final private int sampleErrPeriod;
    private int opsSinceLastErrSampling = 0;

    public SampleErrorTestPolicy(Config config) {
        var settings = new SampleErrorSettings(config);
        mainPipeline = new PipelinePolicy(config);

        sampleOrder = settings.sampleOrderFactor();
        sampler = new FarmHashSampler(sampleOrder);
        sampledPipeline = new PipelinePolicy(config, sampleOrder);

        sampleErrPeriod = settings.sampleDiffPeriod() * mainPipeline.cacheCapacity();

        stats = new SampleErrorTestStats(mainPipeline, sampledPipeline, "Sampling Error testing: O:"
                                                                        + sampleOrder
                                                                        + " main: "
                                                                        + mainPipeline.cacheCapacity()
                                                                        + " sampled: "
                                                                        + sampledPipeline.cacheCapacity());
    }

    @Override
    public void record(AccessEvent event) {
        mainPipeline.record(event);

        switch (event.getStatus()) {
            case HIT:
                stats.recordHit();
                stats.recordHitPenalty(event.hitPenalty());
                break;
            case DELAYED_HIT:
                stats.recordDelayedHit();
                stats.recordDelayedHitPenalty(event.delayedHitPenalty());
                break;
            case MISS:
                stats.recordMiss();
                stats.recordMissPenalty(event.missPenalty());
                break;
        }

        if (sampler.shouldSample(event.key())) {
            sampledPipeline.record(event);
            stats.recordSampled();
        }

        ++opsSinceLastErrSampling;

        if (opsSinceLastErrSampling >= sampleErrPeriod) {
            final double mainAvgPen = mainPipeline.getTimeframeAveragePenalty();
            final double sampledAvgPen = sampledPipeline.getTimeframeAveragePenalty();

            stats.recordDiff(mainAvgPen, sampledAvgPen);

//            System.out.println(String.format("main: %.2f sampled: %.2f", mainAvgPen, sampledAvgPen));

            opsSinceLastErrSampling = 0;
        }
    }

    @Override
    public PolicyStats stats() {
        return this.stats;
    }

    @Override
    public void finished() {
        long mainOpCountProp = mainPipeline.stats().operationCount() >> sampleOrder;
        long sampledOpCount = sampledPipeline.stats().operationCount();
        Assert.assertCondition(Math.abs(mainOpCountProp - sampledOpCount) <= mainOpCountProp * 0.1, () -> String.format("not the same count %d vs %d",
                                                                                                                        mainOpCountProp,
                                                                                                                        sampledOpCount));
    }

    public static class SampleErrorSettings extends BasicSettings {
        final static String BASE_PATH = "sample-error-test";

        public SampleErrorSettings(Config config) {
            super(config);
        }

        public int sampleOrderFactor() { return config().getInt(BASE_PATH + ".sample-order-factor"); }

        public int sampleDiffPeriod() { return config().getInt(BASE_PATH + ".sample-diff-period"); }
    }

    public static class SampleErrorTestStats extends PolicyStats {
        private double err = 0;
        private double squareErr = 0;
        private int numOfAdditions = 0;
        private int sampledCount = 0;
        private PipelinePolicy mainCache;
        private PipelinePolicy sampledCache;

        public SampleErrorTestStats(PipelinePolicy main, PipelinePolicy sampled, String format, Object... args) {
            super(format, args);

            addMetric(Metric.of("Average Error", (Supplier<Double>) this::avgErr, Metric.MetricType.NUMBER, true));
            addMetric(Metric.of("Error STD", (Supplier<Double>) this::errSTD, Metric.MetricType.NUMBER, true));
            addMetric(Metric.of("Real Average Penalty", (Supplier<Double>) this::realAvgPen, Metric.MetricType.NUMBER, true));
            addMetric(Metric.of("Sampled Average Penalty", (Supplier<Double>) this::sampledAvgPen, Metric.MetricType.NUMBER, true));
            addMetric(Metric.of("Sample count", (Supplier<Integer>) () -> sampledCount, Metric.MetricType.NUMBER, true));

            this.mainCache = main;
            this.sampledCache = sampled;
        }


        public void recordSampled() {
            ++sampledCount;
        }

        public void recordDiff(double mainCacheAvg, double sampleCacheAvg) {
            double currDiff = mainCacheAvg - sampleCacheAvg;
            err += currDiff;
            squareErr += currDiff * currDiff;
            ++numOfAdditions;
        }

        public double sampledAvgPen() {
            return sampledCache.stats().averagePenalty();
        }

        public double realAvgPen() {
            return mainCache.stats().averagePenalty();
        }

        public double avgErr() {
            return err / numOfAdditions;
        }

        public double errSTD() {
            double avgErr = avgErr();
            return (squareErr / numOfAdditions - avgErr * avgErr) * (numOfAdditions / (double) (numOfAdditions - 1));
        }
    }
}
