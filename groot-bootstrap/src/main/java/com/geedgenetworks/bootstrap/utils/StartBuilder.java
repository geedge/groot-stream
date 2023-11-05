package com.geedgenetworks.bootstrap.utils;

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

public class StartBuilder {
    public static final int COLLECTION_SIZE = 16;
    private static final int APP_LIB_DIR_DEPTH = 2;
    private static final int PLUGIN_LIB_DIR_DEPTH = 3;
    private static DeployMode MODE;
    private static String GROOT_STREAM_HOME;
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
        GROOT_STREAM_HOME = grootStreamHome;
    }

    public static Path appRootDir() {
        if (DeployMode.RUN != MODE && !STARTER) {
            if (DeployMode.RUN_APPLICATION != MODE) {
                throw new IllegalStateException("Deploy mode not support : " + MODE);
            } else {
                return Paths.get("");
            }
        } else {
            try {
                String path = StartBuilder.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
                path = (new File(path)).getPath();
                return Paths.get(path).getParent().getParent();
            } catch (URISyntaxException var1) {
                throw new RuntimeException(var1);
            }
        }
    }

    public static Path appBootstrapDir() {
        return appRootDir().resolve("bootstrap");
    }

    public static Path pluginRootDir() {
        return Paths.get(getGrootStreamHome(), "extensions/plugins");
    }

    public static Path connectorDir() {
        return Paths.get(getGrootStreamHome(), "extensions/connectors");
    }

    public static Path libDir() {
        return Paths.get(getGrootStreamHome(), "lib");
    }

    public static List<Path> getLibJars() {
        Path libRootDir = libDir();
        if (Files.exists(libRootDir, new LinkOption[0]) && Files.isDirectory(libRootDir, new LinkOption[0])) {
            try {
                Stream<Path> stream = Files.walk(libRootDir, 2, new FileVisitOption[]{FileVisitOption.FOLLOW_LINKS});
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

    public static List<Path> getPluginsJarDependencies() {
        Path pluginRootDir = pluginRootDir();
        if (Files.exists(pluginRootDir, new LinkOption[0]) && Files.isDirectory(pluginRootDir, new LinkOption[0])) {
            try {
                Stream<Path> stream = Files.walk(pluginRootDir, 3, new FileVisitOption[]{FileVisitOption.FOLLOW_LINKS});
                Throwable var2 = null;

                List var3;
                try {
                    var3 = (List)stream.filter((it) -> {
                        return pluginRootDir.relativize(it).getNameCount() == 3;
                    }).filter((it) -> {
                        return it.getParent().endsWith("lib");
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


    public static String getGrootStreamHome() {
        if (StringUtils.isNotEmpty(GROOT_STREAM_HOME)) {
            return GROOT_STREAM_HOME;
        } else {
            String grootStreamHome = System.getProperty("GROOT_STREAM_HOME");
            if (StringUtils.isBlank(grootStreamHome)) {
                grootStreamHome = System.getenv("GROOT_STREAM_HOME");
            }

            if (StringUtils.isBlank(grootStreamHome)) {
                grootStreamHome = appRootDir().toString();
            }

            GROOT_STREAM_HOME = grootStreamHome;
            return GROOT_STREAM_HOME;
        }
    }





}
