package com.geedgenetworks.bootstrap.main.simple;

import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.execution.*;
import com.geedgenetworks.bootstrap.main.GrootStreamRunner;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.GrootStreamConfig;
import com.geedgenetworks.common.utils.ReflectionUtils;
import com.geedgenetworks.common.Event;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValueFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Data
public class JobExecutionTest {

    protected final JobRuntimeEnvironment jobRuntimeEnvironment;
    private final Executor<SingleOutputStreamOperator, JobRuntimeEnvironment> sourceExecuteProcessor;
    private final Executor<SingleOutputStreamOperator, JobRuntimeEnvironment> filterExecuteProcessor;
    private final Executor<SingleOutputStreamOperator, JobRuntimeEnvironment> preprocessingExecuteProcessor;
    private final Executor<SingleOutputStreamOperator, JobRuntimeEnvironment> processingExecuteProcessor;
    private final Executor<SingleOutputStreamOperator, JobRuntimeEnvironment> postprocessingExecuteProcessor;
    private final Executor<SingleOutputStreamOperator, JobRuntimeEnvironment> sinkExecuteProcessor;

    private final List<Node> nodes;

    private BiConsumer<ClassLoader, URL> ADD_URL_TO_CLASSLOADER =
            (classLoader, url) -> {
                if (classLoader.getClass().getName().endsWith("SafetyNetWrapperClassLoader")) {
                    URLClassLoader c =
                            (URLClassLoader) ReflectionUtils.getField(classLoader, "inner").get();
                    ReflectionUtils.invoke(c, "addURL", url);
                } else if (classLoader instanceof URLClassLoader) {
                    ReflectionUtils.invoke(classLoader, "addURL", url);
                } else {
                    throw new RuntimeException(
                            "Unsupported classloader: " + classLoader.getClass().getName());
                }
            };
    private final List<URL> jarPaths;
    public JobExecutionTest(Config config, GrootStreamConfig grootStreamConfig) {
       try {
            jarPaths = new ArrayList<>(Collections.singletonList(new File(StartBuilder.appBootstrapDir()
                    .resolve(GrootStreamRunner.APP_JAR_NAME).toString())
                    .toURI().toURL()));
        } catch (MalformedURLException e) {
            throw new JobExecuteException("load groot stream bootstrap jar error.", e);
        }
        registerPlugin(config.getConfig(Constants.APPLICATION));

        this.sourceExecuteProcessor = new SourceExecutor(jarPaths, config);
        this.sinkExecuteProcessor = new SinkExecutor(jarPaths, config);
        this.filterExecuteProcessor = new FilterExecutor(jarPaths, config);
        this.preprocessingExecuteProcessor = new PreprocessingExecutor(jarPaths, config);
        this.processingExecuteProcessor = new ProcessingExecutor(jarPaths, config);
        this.postprocessingExecuteProcessor = new PostprocessingExecutor(jarPaths, config);
        this.jobRuntimeEnvironment =
                JobRuntimeEnvironment.getInstance(this.registerPlugin(config, jarPaths), grootStreamConfig);

        this.sourceExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.sinkExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.filterExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.preprocessingExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.processingExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.postprocessingExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.nodes = buildJobNode(config);

    }

    private void registerPlugin(Config appConfig) {
        List<Path> thirdPartyJars = new ArrayList<>();
        Config envConfig = appConfig.getConfig(Constants.APPLICATION_ENV);
        if(envConfig.hasPath(ExecutionConfigKeyName.JARS)) {
            thirdPartyJars = new ArrayList<>(StartBuilder
                    .getThirdPartyJars(envConfig.getString(ExecutionConfigKeyName.JARS)));
        }
        thirdPartyJars.addAll(StartBuilder.getConnectorJars());
        thirdPartyJars.addAll(StartBuilder.getPluginsJarDependencies());

        List<URL> jarDependencies = Stream.concat(thirdPartyJars.stream(), StartBuilder.getLibJars().stream())
                .map(Path::toUri)
                .map(uri -> {
                    try {
                        return uri.toURL();
                    }catch (MalformedURLException e){
                        throw new RuntimeException("the uri of jar illegal: " + uri, e);
                    }
                })
                .collect(Collectors.toList());
        jarDependencies.forEach(url -> {
            ADD_URL_TO_CLASSLOADER.accept(Thread.currentThread().getContextClassLoader(), url);
        });
        jarPaths.addAll(jarDependencies);

    }


    private Config registerPlugin(Config config , List<URL> jars) {
       config = this.injectJarsToConfig(
               config, ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV, ExecutionConfigKeyName.PIPELINE_JARS), jars);
       return  this.injectJarsToConfig(
                config, ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV, ExecutionConfigKeyName.PIPELINE_CLASSPATHS), jars);
    }


    private Config injectJarsToConfig(Config config, String path, List<URL> jars) {
        List<URL> validJars = new ArrayList<>();
        for (URL jarUrl : jars) {
            if (new File(jarUrl.getFile()).exists()) {
                validJars.add(jarUrl);
                log.info("Inject jar to config: {}", jarUrl);
            } else {
                log.warn("Remove invalid jar when inject jars into config: {}", jarUrl);
            }
        }

        if (config.hasPath(path)) {
            Set<URL> paths =
                    Arrays.stream(config.getString(path).split(";"))
                            .map(
                                    uri -> {
                                        try {
                                            return new URL(uri);
                                        } catch (MalformedURLException e) {
                                            throw new RuntimeException(
                                                    "the uri of jar illegal:" + uri, e);
                                        }
                                    })
                            .collect(Collectors.toSet());
            paths.addAll(validJars);

            config =  config.withValue(
                    path,
                    ConfigValueFactory.fromAnyRef(
                            paths.stream()
                                    .map(URL::toString)
                                    .distinct()
                                    .collect(Collectors.joining(";"))));
        } else {
            config =
                    config.withValue(
                            path,
                            ConfigValueFactory.fromAnyRef(
                                    validJars.stream()
                                            .map(URL::toString)
                                            .distinct()
                                            .collect(Collectors.joining(";"))));
        }
        return config;
    }

    private List<Node> buildJobNode(Config config) {


        Map<String, Object> sources = Maps.newHashMap();
        Map<String, Object> sinks =Maps.newHashMap();
        Map<String, Object> filters = Maps.newHashMap();
        Map<String, Object> preprocessingPipelines = Maps.newHashMap();
        Map<String, Object> processingPipelines = Maps.newHashMap();
        Map<String, Object> postprocessingPipelines = Maps.newHashMap();

        if (config.hasPath(Constants.SOURCES)) {
            sources = config.getConfig(Constants.SOURCES).root().unwrapped();
        }
        if (config.hasPath(Constants.SINKS)) {
            sinks =config.getConfig(Constants.SINKS).root().unwrapped();
        }
        if (config.hasPath(Constants.FILTERS)) {
            filters = config.getConfig(Constants.FILTERS).root().unwrapped();
        }
        if (config.hasPath(Constants.PREPROCESSING_PIPELINES)) {
            preprocessingPipelines = config.getConfig(Constants.PREPROCESSING_PIPELINES).root().unwrapped();
        }
        if (config.hasPath(Constants.PROCESSING_PIPELINES)) {
            processingPipelines = config.getConfig(Constants.PROCESSING_PIPELINES).root().unwrapped();
        }
        if (config.hasPath(Constants.POSTPROCESSING_PIPELINES)) {
            postprocessingPipelines = config.getConfig(Constants.POSTPROCESSING_PIPELINES).root().unwrapped();
        }

        List<? extends Config> topology = config.getConfig(Constants.APPLICATION).getConfigList(Constants.APPLICATION_TOPOLOGY);

        List<Node> nodes = Lists.newArrayList();

        topology.forEach(item -> {
            Node node = JSONObject.from(item.root().unwrapped()).toJavaObject(Node.class);
            nodes.add(node);
        });

        for(Node node : nodes) {
            if (sources.containsKey(node.getName())) {
                node.setType(ProcessorType.SOURCE);
            } else if (sinks.containsKey(node.getName())) {
                node.setType(ProcessorType.SINK);
            } else if (filters.containsKey(node.getName())) {
                node.setType(ProcessorType.FILTER);
            } else if (preprocessingPipelines.containsKey(node.getName())) {
                node.setType(ProcessorType.PREPROCESSING);
            } else if (processingPipelines.containsKey(node.getName())) {
                node.setType(ProcessorType.PROCESSING);
            } else if (postprocessingPipelines.containsKey(node.getName())) {
                node.setType(ProcessorType.POSTPROCESSING);
            } else {
                throw new JobExecuteException("unsupported process type " + node.getName());
            }
        }

        return nodes;

    }


    public SingleOutputStreamOperator<Event> getSingleOutputStreamOperator() throws JobExecuteException {

        List<Node> sourceNodes = nodes
                .stream().filter(v -> v.getType().name().equals(ProcessorType.SOURCE.name())).collect(Collectors.toList());

        SingleOutputStreamOperator<Event> singleOutputStreamOperator = null;

        for(Node sourceNode : sourceNodes) {
            singleOutputStreamOperator = sourceExecuteProcessor.execute(singleOutputStreamOperator, sourceNode);
            for (String nodeName : sourceNode.getDownstream()) {
                buildJobGraph(singleOutputStreamOperator, nodeName);
            }
        }

        return singleOutputStreamOperator;


    }

    private void buildJobGraph(SingleOutputStreamOperator<Event> singleOutputStreamOperator, String downstreamNodeName) {
        Node node = getNode(downstreamNodeName).orElseGet(() -> {
            throw new JobExecuteException("can't find downstream node " + downstreamNodeName);
        });
        if (node.getType().name().equals(ProcessorType.FILTER.name())) {
            singleOutputStreamOperator = filterExecuteProcessor.execute(singleOutputStreamOperator, node);
        } else if (node.getType().name().equals(ProcessorType.PREPROCESSING.name())) {
            singleOutputStreamOperator = preprocessingExecuteProcessor.execute(singleOutputStreamOperator, node);
        } else if (node.getType().name().equals(ProcessorType.PROCESSING.name())) {
            singleOutputStreamOperator = processingExecuteProcessor.execute(singleOutputStreamOperator, node);
        } else if (node.getType().name().equals(ProcessorType.POSTPROCESSING.name())) {
            singleOutputStreamOperator = postprocessingExecuteProcessor.execute(singleOutputStreamOperator, node);
        } else if (node.getType().name().equals(ProcessorType.SINK.name())) {
            singleOutputStreamOperator = sinkExecuteProcessor.execute(singleOutputStreamOperator, node);
        }  else {
            throw new JobExecuteException("unsupported process type " + node.getType().name());
        }


        for (String nodeName : node.getDownstream()) {
            buildJobGraph(singleOutputStreamOperator, nodeName);
        }


    }

    private Optional<Node> getNode(String name) {
        return nodes.stream().filter(v-> v.getName().equals(name)).findFirst();
    }


}
