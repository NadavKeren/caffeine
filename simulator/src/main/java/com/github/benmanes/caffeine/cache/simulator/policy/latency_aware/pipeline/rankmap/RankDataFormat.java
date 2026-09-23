package com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.rankmap;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.latency_aware.pipeline.PipelinePolicy;
import com.typesafe.config.Config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.stream.Stream;

/***
 * The layout of a shadow-ranking recording, and the identity of the run that may use it.
 * <p>
 * The recording is worth having because the boards do not depend on how the cache is split between
 * the stages - they are a function of the request stream and the cache size alone. One recording
 * therefore serves every allocation, quantum, decision interval and objective tried at that size,
 * which is exactly the shape of a configuration sweep.
 * <p>
 * The file is:
 * <pre>
 *   header
 *   snapshot(0), rank(0), rank(1), ... rank(S-1),
 *   snapshot(S), rank(S), ...
 * </pre>
 * There are no markers. The snapshot interval is in the header, so both sides know exactly where a
 * snapshot block falls. Beside it sits a {@code .benefit} file with the latency objective's windows
 * (see {@link BenefitDataWriter}); the two are written by the same run. A replay only needs the snapshots at its own decisions, so one recording
 * serves every decision interval that is a multiple of its snapshot interval.
 */
public final class RankDataFormat {
    public static final String RANK_EXTENSION = ".rankdata";
    public static final String BENEFIT_EXTENSION = ".benefit";

    public static final int MAGIC = 0x524B4D50; // "RKMP"
    public static final int VERSION = 1;
    public static final int BENEFIT_MAGIC = 0x524B4D42; // "RKMB"

    /*** Byte offset of the warm-up request index, which the writer patches once it knows it. */
    public static final int WARM_AT_OFFSET = 4 + 4 + 4 + 4 + 4 + 1;

    private RankDataFormat() {}

    public enum Mode {
        /*** Maintain the boards live, record nothing. */
        OFF,
        /*** Maintain the boards live and record them. */
        WRITE,
        /*** Replay a recording, doing no ranking work at all. */
        READ,
        /*** Replay a matching recording if there is one, otherwise record. */
        AUTO;

        public static Mode parse(String value) {
            return switch (value.trim().toLowerCase(Locale.US)) {
                case "off", "none" -> OFF;
                case "write", "record" -> WRITE;
                case "read", "replay" -> READ;
                case "auto" -> AUTO;
                default -> throw new IllegalArgumentException("No such precompute mode: " + value);
            };
        }
    }

    /*** A rank has to represent every position plus the "off this board" sentinel at capacity + 1. */
    public static int rankBytes(int capacity) {
        return capacity + 1 <= 0xFFFF ? 2 : 4;
    }

    public static List<String> stageTypes(Config config) {
        var settings = new PipelinePolicy.PipelineSettings(config);
        var types = new ArrayList<String>();

        for (Config blockConfig : settings.blocksConfigs()) {
            types.add(new PipelinePolicy.PipelineBlockSettings(blockConfig).type());
        }

        return types;
    }

    /***
     * Everything a recording's contents depend on. A run whose fingerprint differs must not reuse it,
     * and saying so loudly beats silently replaying boards built under other rules.
     */
    public static String fingerprint(Config config) {
        var basic = new BasicSettings(config);
        var pipeline = new PipelinePolicy.PipelineSettings(config);

        var description = new StringBuilder();
        description.append("capacity=").append(basic.maximumSize());
        description.append(";stages=").append(stageTypes(config));
        description.append(";seed=").append(basic.randomSeed());
        description.append(";burst=").append(pipeline.agingWindowSize())
                   .append(',').append(pipeline.ageSmoothFactor())
                   .append(',').append(pipeline.numOfPartitions());
        description.append(";lfu-probation=").append(RankedPipeline.lfuProbationSize(config));
        description.append(";sketch=").append(config.getString("tiny-lfu.sketch"))
                   .append(',').append(config.getString("tiny-lfu.count-min-4.reset"));
        description.append(";trace=").append(traceDescription(basic));

        return description.toString();
    }

    private static String traceDescription(BasicSettings settings) {
        var trace = settings.trace();

        if (trace.isFiles()) {
            var files = trace.traceFiles();
            return files.format() + ":" + files.paths() + ":skip=" + trace.skip() + ":limit=" + trace.limit();
        }

        return "synthetic:" + settings.config().getConfig("synthetic").root().render();
    }

    /***
     * The latency half of a recording, beside the rank half: the benefit windows closed over the same
     * requests. A recording is only complete with both, since one run writes both.
     */
    public static Path benefitFileFor(Path rankFile) {
        String name = rankFile.getFileName().toString();
        String stem = name.endsWith(RANK_EXTENSION) ? name.substring(0, name.length() - RANK_EXTENSION.length()) : name;

        return rankFile.resolveSibling(stem + BENEFIT_EXTENSION);
    }

    /*** A stable, filesystem-safe name for the recording of this run's boards. */
    public static Path fileFor(Config config, String directory, int snapshotInterval) {
        return Path.of(directory, namePrefix(config) + snapshotInterval + nameSuffix(config));
    }

    /***
     * The recording of this run's boards that serves a decision interval, whatever snapshot interval
     * it was written with. Only the snapshots have to line up with the decisions - the ranks are
     * per request - so any recording whose snapshot interval divides the decision interval replays
     * the very same rankings, and the run just skips the snapshots between its decisions. When more
     * than one qualifies the sparsest wins, as it has the least to skip.
     */
    public static Optional<Path> findRecording(Config config, String directory, int decisionInterval) {
        final Path folder = Path.of(directory);
        if (!Files.isDirectory(folder)) {
            return Optional.empty();
        }

        final String prefix = namePrefix(config);
        final String suffix = nameSuffix(config);

        try (Stream<Path> files = Files.list(folder)) {
            return files.filter(Files::isRegularFile)
                        .filter(file -> Files.isRegularFile(benefitFileFor(file)))
                        .filter(file -> {
                            String name = file.getFileName().toString();
                            if (!name.startsWith(prefix) || !name.endsWith(suffix)
                                || name.length() <= prefix.length() + suffix.length()) {
                                return false;
                            }
                            String interval = name.substring(prefix.length(), name.length() - suffix.length());
                            return interval.chars().allMatch(Character::isDigit)
                                   && decisionInterval % Integer.parseInt(interval) == 0;
                        })
                        .max(Comparator.comparingInt(file -> snapshotIntervalOf(file, prefix, suffix)));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot list the shadow ranking recordings in " + folder, e);
        }
    }

    private static int snapshotIntervalOf(Path file, String prefix, String suffix) {
        String name = file.getFileName().toString();
        return Integer.parseInt(name.substring(prefix.length(), name.length() - suffix.length()));
    }

    private static String namePrefix(Config config) {
        var basic = new BasicSettings(config);
        return String.format("boards.C%d.%s.S", basic.maximumSize(), String.join("-", stageTypes(config)));
    }

    private static String nameSuffix(Config config) {
        return "." + shortDigest(fingerprint(config)) + RANK_EXTENSION;
    }

    private static String shortDigest(String value) {
        try {
            var sha = MessageDigest.getInstance("SHA-256");
            byte[] hash = sha.digest(value.getBytes(StandardCharsets.UTF_8));

            return HexFormat.of().formatHex(hash, 0, 6);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required to be available", e);
        }
    }
}
