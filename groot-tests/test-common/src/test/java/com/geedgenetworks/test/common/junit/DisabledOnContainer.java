package com.geedgenetworks.test.common.junit;

import com.geedgenetworks.test.common.container.EngineType;
import com.geedgenetworks.test.common.container.TestContainerId;

import java.lang.annotation.*;

@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface DisabledOnContainer {
    TestContainerId[] value();
    EngineType[] type() default {};

    /**
     * Custom reason to provide if the test container is disabled.
     *
     * <p>If a custom reason is supplied, it will be combined with the default reason for this
     * annotation. If a custom reason is not supplied, the default reason will be used.
     */
    String disabledReason() default "";
}
