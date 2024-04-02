package com.geedgenetworks.test.common.container;

import cn.hutool.core.util.XmlUtil;
import com.geedgenetworks.bootstrap.utils.ConfigBuilder;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigResolveOptions;
import groovy.lang.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public final class ContainerUtil {

    public static final String PROJECT_ROOT_PATH = getProjectRootPath();
    public static final String PLUGIN_MAPPING_FILE = "plugin-mapping.properties";


    private static String getProjectRootPath() {
        String testCommonRootModuleDir = "groot-tests";
        Path path = Paths.get(System.getProperty("user.dir"));
        while (!path.endsWith(Paths.get(testCommonRootModuleDir))) {
            path = path.getParent();
        }
        return path.getParent().toString();
    }

    public static String getProjectVersion() {
        String pomFile = PROJECT_ROOT_PATH + File.separator + "pom.xml";
        checkPathExist(pomFile);
        Document docResult = XmlUtil.readXML(new File(pomFile));
        Element project = XmlUtil.getRootElement(docResult);
        Element properties = XmlUtil.getElement(project, "properties");
        Element revisionElement = XmlUtil.getElement(properties, "revision");
        return revisionElement.getTextContent();
    }


    public static void checkPathExist(String path) {
        Assertions.assertTrue(new File(path).exists(), path + " must exist");
    }

    public static void copyGrootStreamStarterToContainer(
            GenericContainer<?> container,
            String startModuleName,
            String startModulePath,
            String GrootStreamHomeInContainer) {

        final String[] splits = StringUtils.split(startModuleName, File.separator);
        final String startJarName = splits[splits.length - 1] + ".jar";
        final String startJarPath =
                startModulePath + File.separator + "target" + File.separator + startJarName;
        checkPathExist(startJarPath);

        // don't use container#withFileSystemBind, this isn't supported in Windows.
        container.withCopyFileToContainer(
                MountableFile.forHostPath(startJarPath),
                Paths.get(GrootStreamHomeInContainer, "bootstrap", startJarName).toString());


        // copy libs
        String hbaseClientJar = "hbase-client-shaded-" + getProjectVersion() + ".jar";
        Path hbaseClientJarPath =
                Paths.get(PROJECT_ROOT_PATH, "groot-shaded/hbase-client-shaded", "target", hbaseClientJar);
        container.withCopyFileToContainer(
                MountableFile.forHostPath(hbaseClientJarPath),
                Paths.get(GrootStreamHomeInContainer, "lib", hbaseClientJar).toString());


        String formatJsonJar = "format-json-" + getProjectVersion() + ".jar";
        Path formatJsonJarPath =
                Paths.get(PROJECT_ROOT_PATH, "groot-formats/format-json", "target", formatJsonJar);
        container.withCopyFileToContainer(
                MountableFile.forHostPath(formatJsonJarPath),
                Paths.get(GrootStreamHomeInContainer, "lib", formatJsonJar).toString());

        String formatProtobufJar = "format-protobuf-" + getProjectVersion() + ".jar";
        Path formatProtobufJarPath =
                Paths.get(PROJECT_ROOT_PATH, "groot-formats/format-protobuf", "target", formatProtobufJar);
        container.withCopyFileToContainer(
                MountableFile.forHostPath(formatProtobufJarPath),
                Paths.get(GrootStreamHomeInContainer, "lib", formatProtobufJar).toString());


        //copy system config
        final String configPath = PROJECT_ROOT_PATH + "/config";
        checkPathExist(configPath);
        container.withCopyFileToContainer(MountableFile.forHostPath(configPath),
                Paths.get(GrootStreamHomeInContainer, "config").toString());


        // copy bin
        final String startBinPath = startModulePath + File.separator + "src/main/bin/";
        checkPathExist(startBinPath);
        container.withCopyFileToContainer(
                MountableFile.forHostPath(startBinPath),
                Paths.get(GrootStreamHomeInContainer, "bin").toString());

        // copy plugin-mapping.properties
        container.withCopyFileToContainer(
                MountableFile.forHostPath(PROJECT_ROOT_PATH + "/plugin-mapping.properties"),
                Paths.get(GrootStreamHomeInContainer, "connectors", PLUGIN_MAPPING_FILE).toString());



    }

    public static void copyGrootStreamStarterLoggingToContainer(
            GenericContainer<?> container,
            String startModulePath,
            String GrootStreamHomeInContainer) {
        // copy logging lib
        final String loggingLibPath =
                startModulePath
                        + File.separator
                        + "target"
                        + File.separator
                        + "logging-e2e"
                        + File.separator;
        checkPathExist(loggingLibPath);
        container.withCopyFileToContainer(
                MountableFile.forHostPath(loggingLibPath),
                Paths.get(GrootStreamHomeInContainer, "bootstrap", "logging").toString());
    }

    public static String copyConfigFileToContainer(GenericContainer<?> container, String confFile) {
        final String targetConfInContainer = Paths.get("/tmp", confFile).toString();
        container.copyFileToContainer(
                MountableFile.forHostPath(getResourcesFile(confFile).getAbsolutePath()),
                targetConfInContainer);
        return targetConfInContainer;
    }

    public static File getResourcesFile(String confFile) {
        File file = new File(getCurrentModulePath() + "/src/test/resources" + confFile);
        if (file.exists()) {
            return file;
        }
        throw new IllegalArgumentException(confFile + " doesn't exist");
    }

    public static Path getCurrentModulePath() {
        return Paths.get(System.getProperty("user.dir"));
    }

    public static void copyConnectorJarToContainer(
            GenericContainer<?> container,
            String confFile,
            String connectorsRootPath,
            String connectorPrefix,
            String connectorType,
            String grootStreamHome) {
        Config jobConfig = getJobConfig(getResourcesFile(confFile));
        Config connectorsMapping =
                getPluginProperties(new File(PROJECT_ROOT_PATH + File.separator + PLUGIN_MAPPING_FILE));
        if (!connectorsMapping.hasPath(connectorType)
                || connectorsMapping.getConfig(connectorType).isEmpty()) {
            return;
        }
        Config connectors = connectorsMapping.getConfig(connectorType);
        Set<String> connectorNames = getConnectors(jobConfig, connectors, "source");
        connectorNames.addAll(getConnectors(jobConfig, connectors, "sink"));
        File module = new File(PROJECT_ROOT_PATH + File.separator + connectorsRootPath);

        List<File> connectorFiles = getConnectorFiles(module, connectorNames, connectorPrefix);
        connectorFiles.forEach(
                jar ->
                        container.copyFileToContainer(
                                MountableFile.forHostPath(jar.getAbsolutePath()),
                                Paths.get(grootStreamHome, "connectors", jar.getName()).toString()));
    }

    public static String adaptPathForWin(String path) {
        // Running IT use cases under Windows requires replacing \ with /
        return path == null ? "" : path.replaceAll("\\\\", "/");
    }

    private static List<File> getConnectorFiles(
            File currentModule, Set<String> connectorNames, String connectorPrefix) {
        List<File> connectorFiles = new ArrayList<>();
        for (File file : Objects.requireNonNull(currentModule.listFiles())) {
            getConnectorFiles(file, connectorNames, connectorPrefix, connectorFiles);
        }
        return connectorFiles;
    }

    private static void getConnectorFiles(
            File currentModule,
            Set<String> connectorNames,
            String connectorPrefix,
            List<File> connectors) {
        if (currentModule.isFile() || connectorNames.size() == connectors.size()) {
            return;
        }
        if (connectorNames.contains(currentModule.getName())) {
            File targetPath = new File(currentModule.getAbsolutePath() + File.separator + "target");
            for (File file : Objects.requireNonNull(targetPath.listFiles())) {
                if (file.getName().startsWith(currentModule.getName())
                        && !file.getName().endsWith("javadoc.jar")
                        && !file.getName().endsWith("tests.jar")) {
                    connectors.add(file);
                    return;
                }
            }
        }

        if (currentModule.getName().startsWith(connectorPrefix)) {
            for (File file : Objects.requireNonNull(currentModule.listFiles())) {
                getConnectorFiles(file, connectorNames, connectorPrefix, connectors);
            }
        }
    }

    public static List<TestContainer> discoverTestContainers() {
        try {
            final List<TestContainer> result = new LinkedList<>();
            ServiceLoader.load(TestContainer.class, Thread.currentThread().getContextClassLoader())
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            log.error("Could not load service provider for containers.", e);
            throw new RuntimeException("Could not load service provider for containers.", e);
        }
    }

    private static Set<String> getConnectors(
            Config jobConfig, Config connectorsMap, String pluginType) {
         // using specific needed plugin type in the job config
        Config connectorConfig = jobConfig.getConfig(pluginType+"s");
        List<String> connectorList = Lists.newArrayList();
        connectorConfig.root().unwrapped().forEach((key,value) -> {
            Map<String, Object> map = (Map<String, Object>) value;
            connectorList.add(map.get("type").toString());

        });

        Map<String, String> connectors = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        connectorsMap.getConfig(pluginType).entrySet().forEach(entry -> {
            connectors.put(entry.getKey(), entry.getValue().unwrapped().toString());
        });

        return connectorList.stream()
                .map(String::toLowerCase)
                .filter(connectors::containsKey)
                .map(connectors::get)
                .collect(Collectors.toSet());
    }

    private static Config getJobConfig(File file) {
        return  ConfigBuilder.of(file.getAbsolutePath());
    }

    private static Config getPluginProperties(File file) {
        return ConfigFactory.parseFile(file)
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(
                        ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }

    public static List<String> getJVMThreadNames(GenericContainer<?> container)
            throws IOException, InterruptedException {
        return getJVMThreads(container).stream().map(Tuple2::getV1).collect(Collectors.toList());
    }

    public static Map<String, Integer> getJVMLiveObject(GenericContainer<?> container)
            throws IOException, InterruptedException {
        Container.ExecResult liveObjects =
                container.execInContainer("jmap", "-histo:live", getJVMProcessId(container));
        Assertions.assertEquals(0, liveObjects.getExitCode());
        String value = liveObjects.getStdout().trim();
        return Arrays.stream(value.split("\n"))
                .skip(2)
                .map(
                        str ->
                                Arrays.stream(str.split(" "))
                                        .filter(StringUtils::isNotEmpty)
                                        .collect(Collectors.toList()))
                .filter(list -> list.size() == 4)
                .collect(
                        Collectors.toMap(
                                list -> list.get(3),
                                list -> Integer.valueOf(list.get(1)),
                                (a, b) -> a));
    }

    public static List<Tuple2<String, String>> getJVMThreads(GenericContainer<?> container)
            throws IOException, InterruptedException {
        Container.ExecResult threads =
                container.execInContainer("jstack", getJVMProcessId(container));
        Assertions.assertEquals(0, threads.getExitCode());
        // Thread name line example
        // "hz.main.MetricsRegistry.thread-2" #232 prio=5 os_prio=0 tid=0x0000ffff3c003000 nid=0x5e
        // waiting on condition [0x0000ffff6cf3a000]
        return Arrays.stream(threads.getStdout().trim().split("\n\n"))
                .filter(s -> s.startsWith("\""))
                .map(
                        threadStr ->
                                new Tuple2<>(
                                        Arrays.stream(threadStr.split("\n"))
                                                .filter(s -> s.startsWith("\""))
                                                .map(s -> s.substring(1, s.lastIndexOf("\"")))
                                                .findFirst()
                                                .get(),
                                        threadStr))
                .collect(Collectors.toList());
    }

    private static String getJVMProcessId(GenericContainer<?> container)
            throws IOException, InterruptedException {
        Container.ExecResult processes = container.execInContainer("jps");
        Assertions.assertEquals(0, processes.getExitCode());
        Optional<String> server =
                Arrays.stream(processes.getStdout().trim().split("\n"))
                        .filter(s -> s.contains("GrootstreamServer"))
                        .findFirst();
        Assertions.assertTrue(server.isPresent());
        return server.get().trim().split(" ")[0];
    }









}
