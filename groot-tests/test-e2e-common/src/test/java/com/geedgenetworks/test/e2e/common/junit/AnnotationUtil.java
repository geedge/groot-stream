package com.geedgenetworks.test.e2e.common.junit;

import com.geedgenetworks.test.e2e.common.container.TestContainer;
import com.geedgenetworks.test.e2e.common.container.TestContainerId;
import com.geedgenetworks.test.e2e.common.container.EngineType;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.junit.platform.commons.util.AnnotationUtils;
import java.lang.reflect.AnnotatedElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AnnotationUtil {
    public static List<TestContainer> filterDisabledContainers(
            List<TestContainer> containers, AnnotatedElement annotatedElement) {
        // Filters disabled containers
        final List<TestContainerId> disabledContainers = new ArrayList<>();
        final List<EngineType> disabledEngineTypes = new ArrayList<>();
        AnnotationUtils.findAnnotation(annotatedElement, DisabledOnContainer.class)
                .ifPresent(
                        annotation -> {
                            Collections.addAll(disabledContainers, annotation.value());
                            Collections.addAll(disabledEngineTypes, annotation.type());
                        });
        return containers.stream()
                .filter(container -> !disabledContainers.contains(container.identifier()))
                .filter(
                        container ->
                                !disabledEngineTypes.contains(
                                        container.identifier().getEngineType()))
                .collect(Collectors.toList());
    }
}
