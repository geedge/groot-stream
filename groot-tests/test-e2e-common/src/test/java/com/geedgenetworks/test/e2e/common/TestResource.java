package com.geedgenetworks.test.e2e.common;

/**
 * Basic abstractions for all resources used in connector testing framework.
 *
 * <p>Lifecycle of test resources will be managed by the framework.
 */
public interface TestResource {

    /**
     * Start up the test resource.
     *
     * <p>The implementation of this method should be idempotent.
     *
     * @throws Exception if anything wrong when starting the resource
     */
    void startUp() throws Exception;

    /**
     * Tear down the test resource.
     *
     * <p>The test resource should be able to tear down even without a startup (could be a no-op).
     *
     * @throws Exception if anything wrong when tearing the resource down
     */
    void tearDown() throws Exception;
}
