package com.geedgenetworks.test.common.junit;

import com.geedgenetworks.test.common.container.ContainerExtendedFactory;
import com.geedgenetworks.test.common.container.TestContainer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import static com.geedgenetworks.test.common.junit.ContainerTestingExtension.TEST_CONTAINERS_STORE_KEY;
import static com.geedgenetworks.test.common.junit.ContainerTestingExtension.TEST_EXTENDED_FACTORY_STORE_KEY;
import static com.geedgenetworks.test.common.junit.ContainerTestingExtension.TEST_RESOURCE_NAMESPACE;
@Slf4j
public class TestCaseInvocationContextProvider implements TestTemplateInvocationContextProvider {
    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        // Only support test cases with TestContainer as parameter
        Class<?>[] parameterTypes = context.getRequiredTestMethod().getParameterTypes();
        return parameterTypes.length == 1
                && Arrays.stream(parameterTypes).anyMatch(TestContainer.class::isAssignableFrom);
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
            ExtensionContext context) {
        List<TestContainer> testContainers =
                AnnotationUtil.filterDisabledContainers(
                        (List<TestContainer>)
                                context.getStore(TEST_RESOURCE_NAMESPACE)
                                        .get(TEST_CONTAINERS_STORE_KEY),
                        context.getRequiredTestMethod());

        ContainerExtendedFactory containerExtendedFactory =
                (ContainerExtendedFactory)
                        context.getStore(TEST_RESOURCE_NAMESPACE)
                                .get(TEST_EXTENDED_FACTORY_STORE_KEY);

        int containerAmount = testContainers.size();
        return testContainers.stream()
                .map(
                        testContainer ->
                                new TestResourceProvidingInvocationContext(
                                        testContainer, containerExtendedFactory, containerAmount));
    }
    static class TestResourceProvidingInvocationContext implements TestTemplateInvocationContext {
        private final TestContainer testContainer;
        private final ContainerExtendedFactory containerExtendedFactory;
        private final Integer containerAmount;

        public TestResourceProvidingInvocationContext(
                TestContainer testContainer,
                ContainerExtendedFactory containerExtendedFactory,
                int containerAmount) {
            this.testContainer = testContainer;
            this.containerExtendedFactory = containerExtendedFactory;
            this.containerAmount = containerAmount;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return String.format(
                    "TestContainer(%s/%s): %s",
                    invocationIndex, containerAmount, testContainer.identifier());
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Arrays.asList(
                    // Extension for injecting parameters
                    new TestContainerResolver(testContainer, containerExtendedFactory),
                    // Extension for closing test container
                    (AfterTestExecutionCallback)
                            ignore -> {
                                testContainer.tearDown();
                                log.info(
                                        "The TestContainer[{}] is closed.",
                                        testContainer.identifier());
                            });
        }
    }

    private static class TestContainerResolver implements ParameterResolver {

        private final TestContainer testContainer;
        private final ContainerExtendedFactory containerExtendedFactory;

        private TestContainerResolver(
                TestContainer testContainer, ContainerExtendedFactory containerExtendedFactory) {
            this.testContainer = testContainer;
            this.containerExtendedFactory = containerExtendedFactory;
        }

        @Override
        public boolean supportsParameter(
                ParameterContext parameterContext, ExtensionContext extensionContext)
                throws ParameterResolutionException {
            return TestContainer.class.isAssignableFrom(parameterContext.getParameter().getType());
        }

        @SneakyThrows
        @Override
        public Object resolveParameter(
                ParameterContext parameterContext, ExtensionContext extensionContext)
                throws ParameterResolutionException {
            testContainer.startUp();
            testContainer.executeExtraCommands(containerExtendedFactory);
            log.info("The TestContainer[{}] is running.", testContainer.identifier());
            return this.testContainer;
        }
    }

}
