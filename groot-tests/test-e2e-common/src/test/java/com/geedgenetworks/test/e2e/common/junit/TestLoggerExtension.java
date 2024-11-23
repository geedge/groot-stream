package com.geedgenetworks.test.e2e.common.junit;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

public class TestLoggerExtension implements TestWatcher, BeforeEachCallback {
    private static final Logger LOG = LoggerFactory.getLogger(TestLoggerExtension.class);
    @Override
    public void beforeEach(ExtensionContext context) {
        LOG.info(
                "\n================================================================================"
                        + "\nTest {}.{} is running."
                        + "\n--------------------------------------------------------------------------------",
                context.getRequiredTestClass().getCanonicalName(),
                context.getRequiredTestMethod().getName());
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        LOG.info(
                "\n--------------------------------------------------------------------------------"
                        + "\nTest {}.{} successfully run."
                        + "\n================================================================================",
                context.getRequiredTestClass().getCanonicalName(),
                context.getRequiredTestMethod().getName());
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        LOG.error(
                "\n--------------------------------------------------------------------------------"
                        + "\nTest {}.{} failed with:\n{}"
                        + "\n================================================================================",
                context.getRequiredTestClass().getCanonicalName(),
                context.getRequiredTestMethod().getName(),
                exceptionToString(cause));
    }

    private static String exceptionToString(Throwable t) {
        if (t == null) {
            return "(null)";
        }

        try {
            StringWriter stm = new StringWriter();
            PrintWriter wrt = new PrintWriter(stm);
            t.printStackTrace(wrt);
            wrt.close();
            return stm.toString();
        } catch (Throwable ignored) {
            return t.getClass().getName() + " (error while printing stack trace)";
        }
    }
}
