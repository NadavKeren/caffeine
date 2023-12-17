package com.github.benmanes.caffeine.cache.simulator.policy.sketch;

import com.github.benmanes.caffeine.cache.simulator.DebugHelpers.Assert;
import com.github.benmanes.caffeine.cache.simulator.admission.summin64.SumMin64;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class MovingAverageWithSketchBurstEstimator extends MovingAverageBurstLatencyEstimator<Long> {
    @Nullable private static Logger logger = null;
    final private static boolean DEBUG = false;
    protected SumMin64 sketch;
    final private int decayTimeframe;
    final private double decayFactor;
    private int opsSinceDecay;


    public MovingAverageWithSketchBurstEstimator(long agingWindowSize,
                                                 double ageSmoothingFactor,
                                                 int numOfPartitions,
                                                 double eps,
                                                 double confidence,
                                                 int seed,
                                                 int decayTimeframe,
                                                 double decayFactor) {
        super(agingWindowSize, ageSmoothingFactor, numOfPartitions);
        sketch = new SumMin64(eps, confidence, seed);
        this.decayTimeframe = decayTimeframe;
        this.decayFactor = decayFactor;

        Assert.assertCondition(decayFactor <= 1 && decayFactor >= 0,
                               () -> String.format("Illegal Decay Factor %f", decayFactor));
        opsSinceDecay = 0;

        if (DEBUG) {
            System.out.printf("Aging-window: %d ASF: %.4f partitions: %d%n", agingWindowSize, ageSmoothingFactor, numOfPartitions);
            setLogger();
        }
    }

    @Override
    public void record(Long key, double value, double recordTime) {
        super.record(key, value, recordTime);
        decayIfNeeded();
    }

    @Override
    public void addValueToRecord(Long key, double value, double recordTime) {
        super.addValueToRecord(key, value, recordTime);
        decayIfNeeded();
    }

    private void decayIfNeeded() {
        ++opsSinceDecay;

        if (opsSinceDecay > decayTimeframe) {
            sketch.decay(decayFactor);
            opsSinceDecay = 0;
        }
    }

    @Override
    public void remove(Long key) {
        var entry = storedValues.get(key);
        Assert.assertCondition(entry != null, () -> String.format("Trying to fetch non-existent key %d", key));
        double estimation = entry.getValue();
        sketch.set(key, estimation);
    }

    @Override
    public double getLatencyEstimation(Long key) {
        var entry = storedValues.get(key);
        double sketchEstimation = sketch.estimate(key);
        return entry != null ? Math.max(entry.getValue(), sketchEstimation) : sketchEstimation;
    }

    @Override
    public double getLatencyEstimation(Long key, double time) {
        var entry = storedValues.get(key);
        double sketchEstimation = sketch.estimate(key);

        return entry != null
               ? Math.max(entry.getValue(time), sketchEstimation)
               : sketchEstimation;
    }

    private void setLogger() {
        if (logger == null) {
            logger = Logger.getLogger("");
            LocalDateTime currentTime = LocalDateTime.now(ZoneId.systemDefault());
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("dd-MM-HH-mm-ss");
            logger.setLevel(Level.ALL);
            var handlers = logger.getHandlers();
            logger.removeHandler(handlers[0]);
            try {
                FileHandler fileHandler = new FileHandler(String.format("sketch-moving-average-updates-AW-%d-ASF-%.1f-t-%s.log",
                                                                        this.agingWindowSize,
                                                                        this.ageSmoothingFactor,
                                                                        currentTime.format(timeFormatter)));
                Formatter logFormatter = new Formatter() {
                    @Override
                    public String format(LogRecord record) {
                        return record.getMessage();
                    }
                };

                fileHandler.setFormatter(logFormatter);
                fileHandler.setLevel(Level.ALL);
                logger.setUseParentHandlers(false);
                logger.addHandler(fileHandler);
            } catch (IOException e) {
                System.err.println("Error creating the log file handler");
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}
