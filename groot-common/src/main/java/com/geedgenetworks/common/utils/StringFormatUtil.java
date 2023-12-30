package com.geedgenetworks.common.utils;

import java.util.Collections;

public class StringFormatUtil {
    private static final int NUM = 47;

    private StringFormatUtil() {
        throw new IllegalStateException("Utility class and cannot be instantiated");
    }

    public static String formatTable(Object... objects) {
        String title = objects[0].toString();
        int blankNum = (NUM - title.length()) / 2;
        int kvNum = (objects.length - 1) / 2;
        String template =
                "\n"
                        + "***********************************************"
                        + "\n"
                        + String.join("", Collections.nCopies(blankNum, " "))
                        + "%s"
                        + "\n"
                        + "***********************************************"
                        + "\n"
                        + String.join("", Collections.nCopies(kvNum, "%-26s: %19s\n"))
                        + "***********************************************\n";
        return String.format(template, objects);
    }
}
