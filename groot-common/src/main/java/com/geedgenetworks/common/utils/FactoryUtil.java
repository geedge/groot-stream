package com.geedgenetworks.common.utils;

import com.geedgenetworks.common.exception.IMapStorageException;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class FactoryUtil {
    public static <T> T discoverFactory(
            ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        try {
            final List<T> result = new LinkedList<>();
            ServiceLoader.load(factoryClass, classLoader).iterator().forEachRemaining(result::add);
            List<T> foundFactories =
                    result.stream()
                            .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                            .filter(
                                    t -> {
                                        try {
                                            return t.getClass()
                                                    .getMethod("factoryIdentifier")
                                                    .invoke(t)
                                                    .equals(factoryIdentifier);
                                        } catch (IllegalAccessException
                                                 | InvocationTargetException
                                                 | NoSuchMethodException e) {
                                            throw new IMapStorageException("Failed to call factoryIdentifier method.");
                                        }
                                    })
                            .collect(Collectors.toList());

            if (foundFactories.isEmpty()) {
                throw new IMapStorageException(
                        String.format(
                                "Could not find any factories that implement '%s' in the classpath.",
                                factoryClass.getName()));
            }

            if (foundFactories.size() > 1) {
                throw new IMapStorageException(
                        String.format(
                                "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                        + "Ambiguous factory classes are:\n\n"
                                        + "%s",
                                factoryIdentifier,
                                factoryClass.getName(),
                                foundFactories.stream()
                                        .map(f -> f.getClass().getName())
                                        .sorted()
                                        .collect(Collectors.joining("\n"))));
            }

            return foundFactories.get(0);
        } catch (ServiceConfigurationError e) {
            log.error("Could not load service provider for factories.", e);
            throw new IMapStorageException("Could not load service provider for factories.", e);
        }
    }
}
