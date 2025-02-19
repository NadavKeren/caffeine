package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.ConsoleColors;
import com.github.benmanes.caffeine.cache.simulator.admission.summin64.SumMin64;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;

public class ApproximateWithSketchBurstEstimator extends ApproximateBurstLatencyEstimator {
    final private static boolean DEBUG = false;
    @Nullable private PrintWriter dumper = null;
    protected SumMin64 sketch;
    final private int decayTimeframe;
    final private double decayFactor;
    private int opsSinceDecay;


    public ApproximateWithSketchBurstEstimator(long agingWindowSize,
                                               double ageSmoothingFactor,
                                               int numOfPartitions,
                                               double eps,
                                               double confidence,
                                               int seed,
                                               int decayTimeframe,
                                               double decayFactor,
                                               int maxSize) {
        super(agingWindowSize, ageSmoothingFactor, numOfPartitions, maxSize);
        sketch = new SumMin64(eps, confidence, seed);
        this.decayTimeframe = decayTimeframe;
        this.decayFactor = decayFactor;

        Assert.assertCondition(decayFactor <= 1 && decayFactor >= 0,
                               () -> String.format("Illegal Decay Factor %f", decayFactor));
        opsSinceDecay = 0;

        if (DEBUG) {
            try {
                System.out.printf("Aging-window: %d ASF: %.4f partitions: %d%n", agingWindowSize, ageSmoothingFactor, numOfPartitions);
                FileWriter fileWriter = new FileWriter("/home/nadav/caching/burst-estimator-ops.dump", Charset.defaultCharset());
                dumper = new PrintWriter(fileWriter);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void record(long key, double value, double recordTime) {
        super.record(key, value, recordTime);
        decayIfNeeded();

        if (DEBUG && dumper != null) {
            dumper.println(ConsoleColors.colorString(key, ConsoleColors.GREEN_BOLD));
            dumper.flush();
        }
    }

    @Override
    public void addValueToRecord(long key, double value, double recordTime) {
        super.addValueToRecord(key, value, recordTime);
        decayIfNeeded();

        if (DEBUG && dumper != null) {
            dumper.println(ConsoleColors.colorString(key, ConsoleColors.BLUE));
            dumper.flush();
        }
    }

    private void decayIfNeeded() {
        ++opsSinceDecay;

        if (opsSinceDecay > decayTimeframe) {
            sketch.decay(decayFactor);
            opsSinceDecay = 0;

            if (DEBUG && dumper != null) {
                dumper.println(ConsoleColors.colorString("Performed values decay", ConsoleColors.YELLOW_BOLD));
                dumper.flush();
            }
        }
    }

    @Override
    public void remove(long key) {
        var entry = storedValues.get(key);
        super.remove(key);

        if (DEBUG && dumper != null) {
            dumper.println(ConsoleColors.colorString(key, ConsoleColors.PURPLE_BOLD));
            dumper.flush();
        }

        double estimation = entry.getValue();
        sketch.set(key, estimation);
    }

    @Override
    public double getLatencyEstimation(long key) {
        var entry = storedValues.get(key);
        double sketchEstimation = sketch.estimate(key);
        return entry != null ? Math.max(entry.getValue(), sketchEstimation) : sketchEstimation;
    }

    @Override
    public double getLatencyEstimation(long key, double time) {
        var entry = storedValues.get(key);
        double sketchEstimation = sketch.estimate(key);

        return entry != null
               ? Math.max(entry.getValue(time), sketchEstimation)
               : sketchEstimation;
    }

//    private void setLogger() {
//        if (logger == null) {
//            logger = Logger.getLogger("");
//            LocalDateTime currentTime = LocalDateTime.now(ZoneId.systemDefault());
//            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("dd-MM-HH-mm-ss");
//            logger.setLevel(Level.ALL);
//            var handlers = logger.getHandlers();
//            logger.removeHandler(handlers[0]);
//            try {
//                FileHandler fileHandler = new FileHandler(String.format("sketch-moving-average-updates-AW-%d-ASF-%.1f-t-%s.log",
//                                                                        this.agingWindowSize,
//                                                                        this.ageSmoothingFactor,
//                                                                        currentTime.format(timeFormatter)));
//                Formatter logFormatter = new Formatter() {
//                    @Override
//                    public String format(LogRecord record) {
//                        return record.getMessage();
//                    }
//                };
//
//                fileHandler.setFormatter(logFormatter);
//                fileHandler.setLevel(Level.ALL);
//                logger.setUseParentHandlers(false);
//                logger.addHandler(fileHandler);
//            } catch (IOException e) {
//                System.err.println("Error creating the log file handler");
//                e.printStackTrace();
//                System.exit(1);
//            }
//        }
//    }
}
