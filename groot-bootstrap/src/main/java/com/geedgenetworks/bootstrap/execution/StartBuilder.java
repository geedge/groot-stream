package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.DeployMode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

public class StartBuilder {
    public static final int COLLECTION_SIZE = 16;
    private static final int APP_LIB_DIR_DEPTH = 2;
    private static final int PLUGIN_LIB_DIR_DEPTH = 3;
    private static DeployMode MODE;
    private static String GROOTSTREAM_HOME;
    private static boolean STARTER = false;

    private StartBuilder() {
        throw new IllegalStateException("StartBuilder is a utility class and cannot be instantiated");
    }

    public static void setDeployMode(DeployMode mode) {
        MODE = mode;
    }

    public static void setStarter(boolean inStarter) {
        STARTER = inStarter;
    }

    public static DeployMode getDeployMode() {
        return MODE;
    }

    @VisibleForTesting
    public static void setGrootStreamHome(String grootStreamHome) {
        GROOTSTREAM_HOME = grootStreamHome;
    }

    public static String getGrootStreamHome() {
        if (StringUtils.isNotEmpty(GROOTSTREAM_HOME)) {
            return GROOTSTREAM_HOME;
        }
        String grootStreamHome = System.getProperty("GROOTSTREAM_HOME");
        if (StringUtils.isBlank(grootStreamHome)) {
            grootStreamHome = System.getenv("GROOTSTREAM_HOME");
        }

        if (StringUtils.isBlank(grootStreamHome)) {
            grootStreamHome = appRootDir().toString();
        }

        GROOTSTREAM_HOME = grootStreamHome;
        return GROOTSTREAM_HOME;

    }


    public static Path appRootDir() {
        if (DeployMode.CLIENT == MODE || DeployMode.RUN == MODE || STARTER) {
            try {
                String path =
                        StartBuilder.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI()
                                .getPath();
                path = new File(path).getPath();
                return Paths.get(path).getParent().getParent();
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } else if (DeployMode.RUN_APPLICATION == MODE) {
            return Paths.get("");
        } else {
            throw new IllegalStateException("deploy mode not support : " + MODE);
        }
    }

    public static Path appBootstrapDir() {
        return appRootDir().resolve("bootstrap");
    }

    /**
     * Plugin Root Dir
     */
    public static Path pluginRootDir() {
        return Paths.get(getGrootStreamHome(), "plugins");
    }
    /** Plugin Connector Dir */
    public static Path connectorDir() {
        return Paths.get(getGrootStreamHome(), "connectors");
    }


    /** lib Dir */
    public static Path libDir() {
        return Paths.get(getGrootStreamHome(), "lib");
    }

    /**
     * return lib jars, which located in 'lib/*' or 'lib/{dir}/*'.
     * @return
     */
    public static List<Path> getLibJars() {
        Path libRootDir = libDir();
        if (Files.exists(libRootDir, new LinkOption[0]) && Files.isDirectory(libRootDir, new LinkOption[0])) {
            try {
                Stream<Path> stream = Files.walk(libRootDir, APP_LIB_DIR_DEPTH, new FileVisitOption[]{FileVisitOption.FOLLOW_LINKS});
                Throwable var2 = null;

                List var3;
                try {
                    var3 = (List)stream.filter((it) -> {
                        return !it.toFile().isDirectory();
                    }).filter((it) -> {
                        return it.getFileName().toString().endsWith(".jar");
                    }).collect(Collectors.toList());
                } catch (Throwable var13) {
                    var2 = var13;
                    throw var13;
                } finally {
                    if (stream != null) {
                        if (var2 != null) {
                            try {
                                stream.close();
                            } catch (Throwable var12) {
                                var2.addSuppressed(var12);
                            }
                        } else {
                            stream.close();
                        }
                    }

                }

                return var3;
            } catch (IOException var15) {
                throw new RuntimeException(var15);
            }
        } else {
            return Collections.emptyList();
        }
    }


    /**
     * return the jar package configured in env jars
     * @param paths
     * @return
     */
    public static Set<Path> getThirdPartyJars(String paths) {
        return (Set) Arrays.stream(paths.split(";")).filter((s) -> {
            return !"".equals(s);
        }).filter((it) -> {
            return it.endsWith(".jar");
        }).map((path) -> {
            return Paths.get(URI.create(path));
        }).collect(Collectors.toSet());
    }


    public static Path pluginTarball() {
        return appRootDir().resolve("plugins.tar.gz");
    }

    /** return plugin's dependent jars, which located in 'plugins/${pluginName}/lib/*'. */
    public static List<Path> getPluginsJarDependencies() {
        Path pluginRootDir = StartBuilder.pluginRootDir();
        if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.walk(pluginRootDir, PLUGIN_LIB_DIR_DEPTH, FOLLOW_LINKS)) {
            return stream.filter(
                            it ->
                                    pluginRootDir.relativize(it).getNameCount()
                                            == PLUGIN_LIB_DIR_DEPTH)
                    .filter(it -> it.getParent().endsWith("lib"))
                    .filter(it -> it.getFileName().toString().endsWith(".jar"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Path> getConnectorJars() {
        Path connectorRootDir = StartBuilder.connectorDir();
        if (!Files.exists(connectorRootDir) || !Files.isDirectory(connectorRootDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.walk(connectorRootDir, 2, FOLLOW_LINKS)) {
            return stream.filter(
                            it -> !it.toFile().isDirectory())
                    .filter(it -> it.getFileName().toString().endsWith(".jar"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
