package com.geedgenetworks.core.factories;

import com.geedgenetworks.core.connector.format.DecodingFormat;
import com.geedgenetworks.core.connector.format.EncodingFormat;
import org.apache.flink.configuration.*;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public final class FactoryUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FactoryUtil.class);

    public static final ConfigOption<String> FORMAT =
            ConfigOptions.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding data. "
                                    + "The identifier is used to discover a suitable format factory.");

    /**
     * Validates the required and optional {@link ConfigOption}s of a factory.
     *
     */
    public static void validateFactoryOptions(Factory factory, ReadableConfig options) {
        validateFactoryOptions(factory.requiredOptions(), factory.optionalOptions(), options);
    }

    /**
     * Validates the required options and optional options.
     *
     */
    public static void validateFactoryOptions(
            Set<ConfigOption<?>> requiredOptions,
            Set<ConfigOption<?>> optionalOptions,
            ReadableConfig options) {

        final List<String> missingRequiredOptions =
                requiredOptions.stream()
                        .filter(option -> readOption(options, option) == null)
                        .map(ConfigOption::key)
                        .sorted()
                        .collect(Collectors.toList());

        if (!missingRequiredOptions.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "One or more required options are missing.\n\n"
                                    + "Missing required options are:\n\n"
                                    + "%s",
                            String.join("\n", missingRequiredOptions)));
        }

        optionalOptions.forEach(option -> readOption(options, option));
    }

    public static String getFormatPrefix(
            ConfigOption<String> formatOption, String formatIdentifier) {
        final String formatOptionKey = formatOption.key();
        if (formatOptionKey.equals(FORMAT.key())) {
            return formatIdentifier + ".";
        }else {
            throw new ValidationException(
                    "Format identifier key should be 'format' or suffix with '.format', "
                            + "don't support format identifier key '"
                            + formatOptionKey
                            + "'.");
        }
    }

    public static TableFactoryHelper createTableFactoryHelper(
            TableFactory factory, TableFactory.Context context) {
        return new TableFactoryHelper(factory, context);
    }

    public static <T extends Factory> T discoverFactory(
            ClassLoader classLoader, Class<T> factoryClass, String factoryIdentifier) {
        final List<Factory> factories = discoverFactories(classLoader);

        final List<Factory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        if (foundFactories.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any factories that implement '%s' in the classpath.",
                            factoryClass.getName()));
        }

        final List<Factory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equals(factoryIdentifier))
                        .collect(Collectors.toList());

        if (matchingFactories.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Could not find any factory for identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available factory identifiers are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            foundFactories.stream()
                                    .map(Factory::factoryIdentifier)
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        if (matchingFactories.size() > 1) {
            throw new ValidationException(
                    String.format(
                            "Multiple factories for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous factory classes are:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            factoryClass.getName(),
                            matchingFactories.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return (T) matchingFactories.get(0);
    }

    static List<Factory> discoverFactories(ClassLoader classLoader) {
        final List<Factory> result = new LinkedList<>();
        ServiceLoaderUtil.load(Factory.class, classLoader)
                .forEach(
                        loadResult -> {
                            if (loadResult.hasFailed()) {
                                if (loadResult.getError() instanceof NoClassDefFoundError) {
                                    LOG.debug(
                                            "NoClassDefFoundError when loading a "
                                                    + org.apache.flink.table.factories.Factory.class
                                                    + ". This is expected when trying to load a format dependency but no flink-connector-files is loaded.",
                                            loadResult.getError());
                                    // After logging, we just ignore this failure
                                    return;
                                }
                                throw new TableException(
                                        "Unexpected error when trying to load service provider for factories.",
                                        loadResult.getError());
                            }
                            result.add(loadResult.getService());
                        });
        return result;
    }

    public static <T extends TableFactory> T discoverTableFactory(
            Class<T> factoryClass,  String connector) {
        return discoverFactory(Thread.currentThread().getContextClassLoader(), factoryClass, connector);
    }

    public static <T extends DecodingFormatFactory> T discoverDecodingFormatFactory(
            Class<T> factoryClass, String type) {
        return discoverFactory(Thread.currentThread().getContextClassLoader(), factoryClass, type);
    }

    public static <T extends EncodingFormatFactory> T discoverEncodingFormatFactory(
            Class<T> factoryClass, String type) {
        return discoverFactory(Thread.currentThread().getContextClassLoader(), factoryClass, type);
    }

    private static <T> T readOption(ReadableConfig options, ConfigOption<T> option) {
        try {
            return options.get(option);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format("Invalid value for option '%s'.", option.key()), t);
        }
    }

    /** Validates unconsumed option keys. */
    public static void validateUnconsumedKeys(
            String factoryIdentifier,
            Set<String> allOptionKeys,
            Set<String> consumedOptionKeys) {
        final Set<String> remainingOptionKeys = new HashSet<>(allOptionKeys);
        remainingOptionKeys.removeAll(consumedOptionKeys);
        if (!remainingOptionKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Unsupported options found for '%s'.\n\n"
                                    + "Unsupported options:\n\n"
                                    + "%s\n\n"
                                    + "Supported options:\n\n"
                                    + "%s",
                            factoryIdentifier,
                            remainingOptionKeys.stream().sorted().collect(Collectors.joining("\n")),
                            consumedOptionKeys.stream()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
    }

    public static class TableFactoryHelper {
        private final TableFactory factory;
        private final TableFactory.Context context;
        private final Configuration allOptions;
        private final Set<String> consumedOptionKeys;

        public TableFactoryHelper(TableFactory factory, TableFactory.Context context) {
            this.factory = factory;
            this.context = context;
            this.allOptions = context.getConfiguration();

            final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
            consumedOptions.addAll(factory.requiredOptions());
            consumedOptions.addAll(factory.optionalOptions());

            consumedOptionKeys = consumedOptions.stream()
                            .map(option -> option.key())
                            .collect(Collectors.toSet());
        }

        /** Validates the options of the factory. It checks for unconsumed option keys. */
        public void validate() {
            validateFactoryOptions(factory, allOptions);
            validateUnconsumedKeys(
                    factory.factoryIdentifier(),
                    allOptions.keySet(),
                    consumedOptionKeys);
        }

        public void validateExcept(String... prefixesToSkip) {
            Preconditions.checkArgument(
                    prefixesToSkip.length > 0, "Prefixes to skip can not be empty.");
            final List<String> prefixesList = Arrays.asList(prefixesToSkip);
            consumedOptionKeys.addAll(
                    allOptions.keySet().stream()
                            .filter(key -> prefixesList.stream().anyMatch(key::startsWith))
                            .collect(Collectors.toSet()));
            validate();
        }

        public EncodingFormat discoverEncodingFormat(
                Class<EncodingFormatFactory> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalEncodingFormat(formatFactoryClass, formatOption)
                    .orElseThrow(
                            () ->
                                    new ValidationException(
                                            String.format(
                                                    "Could not find required sink format '%s'.",
                                                    formatOption.key())));
        }

        /**
         * Discovers a {@link EncodingFormat} of the given type using the given option (if present)
         * as factory identifier.
         */
        public Optional<EncodingFormat> discoverOptionalEncodingFormat(
                Class<EncodingFormatFactory> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalFormatFactory(formatFactoryClass, formatOption)
                    .map(
                            formatFactory -> {
                                String formatPrefix = formatPrefix(formatFactory, formatOption);
                                try {
                                    return formatFactory.createEncodingFormat(
                                            context,
                                            createFormatOptions(formatPrefix, formatFactory));
                                } catch (Throwable t) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Error creating sink format '%s' in option space '%s'.",
                                                    formatFactory.factoryIdentifier(),
                                                    formatPrefix),
                                            t);
                                }
                            });
        }

        //

        public DecodingFormat discoverDecodingFormat(
                Class<DecodingFormatFactory> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalDecodingFormat(formatFactoryClass, formatOption)
                    .orElseThrow(
                            () ->
                                    new ValidationException(
                                            String.format(
                                                    "Could not find required scan format '%s'.",
                                                    formatOption.key())));
        }

        /**
         * Discovers a {@link DecodingFormat} of the given type using the given option (if present)
         * as factory identifier.
         */
        public Optional<DecodingFormat> discoverOptionalDecodingFormat(
                Class<DecodingFormatFactory> formatFactoryClass, ConfigOption<String> formatOption) {
            return discoverOptionalFormatFactory(formatFactoryClass, formatOption)
                    .map(
                            formatFactory -> {
                                String formatPrefix = formatPrefix(formatFactory, formatOption);
                                try {
                                    return formatFactory.createDecodingFormat(
                                            context,
                                            createFormatOptions(formatPrefix, formatFactory));
                                } catch (Throwable t) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Error creating scan format '%s' in option space '%s'.",
                                                    formatFactory.factoryIdentifier(),
                                                    formatPrefix),
                                            t);
                                }
                            });
        }

        private <F extends Factory> Optional<F> discoverOptionalFormatFactory(
                Class<F> formatFactoryClass, ConfigOption<String> formatOption) {
            final String identifier = allOptions.get(formatOption);
            //checkFormatIdentifierMatchesWithEnrichingOptions(formatOption, identifier);
            if (identifier == null) {
                return Optional.empty();
            }
            final F factory =
                    discoverFactory(Thread.currentThread().getContextClassLoader(), formatFactoryClass, identifier);
            String formatPrefix = formatPrefix(factory, formatOption);

            // log all used options of other factories
            final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
            consumedOptions.addAll(factory.requiredOptions());
            consumedOptions.addAll(factory.optionalOptions());

            consumedOptions.stream()
                    .map(option ->formatPrefix + option.key())
                    .forEach(consumedOptionKeys::add);

            return Optional.of(factory);
        }
        private String formatPrefix(Factory formatFactory, ConfigOption<String> formatOption) {
            String identifier = formatFactory.factoryIdentifier();

            return getFormatPrefix(formatOption, identifier);
        }

        @SuppressWarnings({"unchecked"})
        private ReadableConfig createFormatOptions(
                String formatPrefix, FormatFactory formatFactory) {
            Configuration formatConf = new DelegatingConfiguration(allOptions, formatPrefix);
            return formatConf;
        }

    }


}
