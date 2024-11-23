package com.geedgenetworks.test.e2e.core;

import lombok.extern.slf4j.Slf4j;

import java.io.File;

@Slf4j
public class TestUtils {
    public static String getResource(String confFile) {
        return System.getProperty("user.dir")
                + File.separator
                + "src"
                + File.separator
                + "test"
                + File.separator
                + "resources"
                + File.separator
                + confFile;
    }

}
