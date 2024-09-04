package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.main.GrootStreamRunner;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.GrootStreamConfig;
import com.geedgenetworks.common.config.SplitConfigOptions;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.common.udf.RuleContext;
import com.geedgenetworks.core.pojo.SplitConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValueFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class JobExecution {

    private final JobRuntimeEnvironment jobRuntimeEnvironment;
    private final Executor<DataStream<Event>, JobRuntimeEnvironment> sourceExecutor;
    private final Executor<DataStream<Event>, JobRuntimeEnvironment> sinkExecutor;
    private final Executor<DataStream<Event>, JobRuntimeEnvironment> filterExecutor;
    private final Executor<DataStream<Event>, JobRuntimeEnvironment> splitExecutor;
    private final Executor<DataStream<Event>, JobRuntimeEnvironment> preprocessingExecutor;
    private final Executor<DataStream<Event>, JobRuntimeEnvironment> processingExecutor;
    private final Executor<DataStream<Event>, JobRuntimeEnvironment> postprocessingExecutor;
    private final List<Node> nodes;
    private final List<URL> jarPaths;
    private final Set<String> splitSet = new HashSet<>();

    public JobExecution(Config jobConfig, GrootStreamConfig grootStreamConfig) {
        try {
            jarPaths = new ArrayList<>(Collections.singletonList(new File(StartBuilder.appBootstrapDir()
                    .resolve(GrootStreamRunner.APP_JAR_NAME).toString())
                    .toURI().toURL()));
        } catch (MalformedURLException e) {
            throw new JobExecuteException("Load GrootStream Bootstrap jar failed.", e);
        }

        registerPlugin(jobConfig.getConfig(Constants.APPLICATION));

        this.sourceExecutor = new SourceExecutor(jarPaths, jobConfig);
        this.sinkExecutor = new SinkExecutor(jarPaths, jobConfig);
        this.filterExecutor = new FilterExecutor(jarPaths, jobConfig);
        this.splitExecutor = new SplitExecutor(jarPaths, jobConfig);
        this.preprocessingExecutor = new PreprocessingExecutor(jarPaths, jobConfig);
        this.processingExecutor = new ProcessingExecutor(jarPaths, jobConfig);
        this.postprocessingExecutor = new PostprocessingExecutor(jarPaths, jobConfig);
        this.jobRuntimeEnvironment =
                JobRuntimeEnvironment.getInstance(this.registerPlugin(jobConfig, jarPaths), grootStreamConfig);

        this.sourceExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.sinkExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.filterExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.splitExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.preprocessingExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.processingExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.postprocessingExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.nodes = buildJobNode(jobConfig);

    }

    private void registerPlugin(Config appConfig) {
        List<Path> thirdPartyJars = new ArrayList<>();
        Config envConfig = appConfig.getConfig(Constants.APPLICATION_ENV);
        if (envConfig.hasPath(ExecutionConfigKeyName.JARS)) {
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
                    } catch (MalformedURLException e) {
                        throw new RuntimeException("the uri of jar illegal: " + uri, e);
                    }
                }).collect(Collectors.toList());
        jarDependencies.forEach(url -> {
            AbstractExecutor.ADD_URL_TO_CLASSLOADER.accept(Thread.currentThread().getContextClassLoader(), url);
        });
        jarPaths.addAll(jarDependencies);

    }


    private Config registerPlugin(Config config, List<URL> jars) {
        config = this.injectJarsToConfig(
                config, ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV, ExecutionConfigKeyName.PIPELINE_JARS), jars);
        return this.injectJarsToConfig(
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
                                                    "The uri of jar illegal:" + uri, e);
                                        }
                                    })
                            .collect(Collectors.toSet());
            paths.addAll(validJars);

            config = config.withValue(
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
        Map<String, Object> sinks = Maps.newHashMap();
        Map<String, Object> filters = Maps.newHashMap();
        Map<String, Object> splits = Maps.newHashMap();
        Map<String, Object> preprocessingPipelines = Maps.newHashMap();
        Map<String, Object> processingPipelines = Maps.newHashMap();
        Map<String, Object> postprocessingPipelines = Maps.newHashMap();

        if (config.hasPath(Constants.SOURCES)) {
            sources = config.getConfig(Constants.SOURCES).root().unwrapped();
        }
        if (config.hasPath(Constants.SINKS)) {
            sinks = config.getConfig(Constants.SINKS).root().unwrapped();
        }
        if (config.hasPath(Constants.FILTERS)) {
            filters = config.getConfig(Constants.FILTERS).root().unwrapped();
        }
        if (config.hasPath(Constants.SPLITS)) {
            splits = config.getConfig(Constants.SPLITS).root().unwrapped();
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

        for (Node node : nodes) {
            if (sources.containsKey(node.getName())) {
                node.setType(ProcessorType.SOURCE);
            } else if (sinks.containsKey(node.getName())) {
                node.setType(ProcessorType.SINK);
            } else if (filters.containsKey(node.getName())) {
                node.setType(ProcessorType.FILTER);
            } else if (splits.containsKey(node.getName())) {
                splits.forEach((key, value) -> {
                    SplitConfig splitConfig = new com.alibaba.fastjson.JSONObject((Map<String, Object>) value).toJavaObject(SplitConfig.class);
                    for(RuleContext ruleContext:splitConfig.getRules()) {
                        splitSet.add(ruleContext.getName());
                    }
                });
                node.setType(ProcessorType.SPLIT);
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


    public void execute() throws JobExecuteException {
        if (!jobRuntimeEnvironment.isLocalMode()) {
            jobRuntimeEnvironment.registerPlugin(jarPaths);
        }
        List<Node> sourceNodes = nodes
                .stream().filter(v -> v.getType().name().equals(ProcessorType.SOURCE.name())).collect(Collectors.toList());

        DataStream<Event> dataStream = null;

        for (Node sourceNode : sourceNodes) {
            dataStream = sourceExecutor.execute(dataStream, sourceNode);
            for (String nodeName : sourceNode.getDownstream()) {
                buildJobGraph(dataStream, nodeName);
            }
        }

        try {
            final long jobStartTime = System.currentTimeMillis();
            log.info('\n' + Constants.GROOT_LOGO);
            log.info("Execution Plan: {}", jobRuntimeEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
            log.info("Execution Job: {}", jobRuntimeEnvironment.getJobName());
            jobRuntimeEnvironment.getStreamExecutionEnvironment().execute(jobRuntimeEnvironment.getJobName());
            final long jobEndTime = System.currentTimeMillis();
            log.info("Job finished, execution duration: {} ms", jobEndTime - jobStartTime);

        } catch (Exception e) {
            throw new JobExecuteException("Execute job error", e);
        }

    }

    private void buildJobGraph(DataStream<Event> dataStream, String downstreamNodeName) {
        Node node = getNode(downstreamNodeName).orElseGet(() -> {
            throw new JobExecuteException("Can't find downstream node " + downstreamNodeName);
        });
        if (node.getType().name().equals(ProcessorType.FILTER.name())) {
            if (splitSet.contains(node.getName())) {
                dataStream = filterExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(node.getName()) {
                }), node);
            } else {
                dataStream = filterExecutor.execute(dataStream, node);
            }
        } else if (node.getType().name().equals(ProcessorType.SPLIT.name())) {
            dataStream = splitExecutor.execute(dataStream, node);

        } else if (node.getType().name().equals(ProcessorType.PREPROCESSING.name())) {
            if (splitSet.contains(node.getName())) {
                dataStream = preprocessingExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(node.getName()) {
                }), node);
            } else {
                dataStream = preprocessingExecutor.execute(dataStream, node);
            }
        } else if (node.getType().name().equals(ProcessorType.PROCESSING.name())) {
            if (splitSet.contains(node.getName())) {
                dataStream = processingExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(node.getName()) {
                }), node);
            } else {
                dataStream = processingExecutor.execute(dataStream, node);
            }
        } else if (node.getType().name().equals(ProcessorType.POSTPROCESSING.name())) {
            if (splitSet.contains(node.getName())) {
                dataStream = postprocessingExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(node.getName()) {
                }), node);
            } else {
                dataStream = postprocessingExecutor.execute(dataStream, node);
            }
        } else if (node.getType().name().equals(ProcessorType.SINK.name())) {
            if (splitSet.contains(node.getName())) {
                dataStream = sinkExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(node.getName()) {
                }), node);
            } else {
                dataStream = sinkExecutor.execute(dataStream, node);
            }
        } else {
            throw new JobExecuteException("unsupported process type " + node.getType().name());
        }


        for (String nodeName : node.getDownstream()) {
            buildJobGraph(dataStream, nodeName);
        }


    }

    private Optional<Node> getNode(String name) {
        return nodes.stream().filter(v -> v.getName().equals(name)).findFirst();
    }


}
