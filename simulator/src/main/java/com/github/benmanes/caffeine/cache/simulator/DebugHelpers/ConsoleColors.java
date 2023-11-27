package com.github.benmanes.caffeine.cache.simulator.DebugHelpers;

import com.google.errorprone.annotations.FormatMethod;

public class ConsoleColors {
    public static final String RESET = "\033[0m";  // Text Reset

    // Regular Colors
    public static final String BLACK = "\033[0;30m";   // BLACK
    public static final String RED = "\033[0;31m";     // RED
    public static final String GREEN = "\033[0;32m";   // GREEN
    public static final String YELLOW = "\033[0;33m";  // YELLOW
    public static final String BLUE = "\033[0;34m";    // BLUE
    public static final String PURPLE = "\033[0;35m";  // PURPLE
    public static final String CYAN = "\033[0;36m";    // CYAN
    public static final String WHITE = "\033[0;37m";   // WHITE

    // Bold
    public static final String BLACK_BOLD = "\033[1;30m";  // BLACK
    public static final String RED_BOLD = "\033[1;31m";    // RED
    public static final String GREEN_BOLD = "\033[1;32m";  // GREEN
    public static final String YELLOW_BOLD = "\033[1;33m"; // YELLOW
    public static final String BLUE_BOLD = "\033[1;34m";   // BLUE
    public static final String PURPLE_BOLD = "\033[1;35m"; // PURPLE
    public static final String CYAN_BOLD = "\033[1;36m";   // CYAN
    public static final String WHITE_BOLD = "\033[1;37m";  // WHITE

    public static String colorString(String str, String color) {
        return color + str + RESET;
    }

    public static <T> String colorString(T value, String color) {
        return color + value.toString() + RESET;
    }

    @FormatMethod
    public static String errorString(String format, Object... args) {
        return RED_BOLD + String.format(format, args) + RESET;
    }

    @FormatMethod
    public static String majorInfoString(String format, Object... args) {
        return PURPLE_BOLD + String.format(format, args) + RESET;
    }

    @FormatMethod
    public static String infoString(String format, Object... args) {
        return YELLOW_BOLD + String.format(format, args) + RESET;
    }

    @FormatMethod
    public static String minorInfoString(String format, Object... args) {
        return CYAN + String.format(format, args) + RESET;
    }
}
