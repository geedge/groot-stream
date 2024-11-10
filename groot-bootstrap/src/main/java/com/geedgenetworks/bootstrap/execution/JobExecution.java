package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.bootstrap.enums.StageType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.main.GrootStreamRunner;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.GrootStreamConfig;
import com.geedgenetworks.spi.table.event.Event;
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
    private final List<OperatorNode> operatorNodes;
    private final List<URL> jarPaths;
    private final Map<String,String> nodeNameWithSplitTags = new HashMap<>();

    public JobExecution(Config jobConfig, GrootStreamConfig grootStreamConfig) {
        try {
            jarPaths = new ArrayList<>(Collections.singletonList(new File(StartBuilder.appBootstrapDir()
                    .resolve(GrootStreamRunner.APP_JAR_NAME).toString())
                    .toURI().toURL()));
        } catch (MalformedURLException e) {
            throw new JobExecuteException("Load GrootStream Bootstrap jar failed.", e);
        }

        registerPlugin(jobConfig.getConfig(Constants.APPLICATION));

        this.sourceExecutor = new SourceExecutor(jobConfig);
        this.sinkExecutor = new SinkExecutor(jobConfig);
        this.filterExecutor = new FilterExecutor(jobConfig);
        this.splitExecutor = new SplitExecutor(jobConfig);
        this.preprocessingExecutor = new PreprocessingExecutor(jobConfig);
        this.processingExecutor = new ProcessingExecutor(jobConfig);
        this.postprocessingExecutor = new PostprocessingExecutor(jobConfig);
        this.jobRuntimeEnvironment =
                JobRuntimeEnvironment.getInstance(this.registerPlugin(jobConfig, jarPaths), grootStreamConfig);

        this.sourceExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.sinkExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.filterExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.splitExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.preprocessingExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.processingExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.postprocessingExecutor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.operatorNodes = buildJobNode(jobConfig);

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

    private List<OperatorNode> buildJobNode(Config config) {

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

        List<OperatorNode> operatorNodes = Lists.newArrayList();

        topology.forEach(item -> {
            OperatorNode operatorNode = JSONObject.from(item.root().unwrapped()).toJavaObject(OperatorNode.class);
            operatorNodes.add(operatorNode);
        });

        for (OperatorNode operatorNode : operatorNodes) {
            if (sources.containsKey(operatorNode.getName())) {
                operatorNode.setType(StageType.SOURCE);
            } else if (sinks.containsKey(operatorNode.getName())) {
                operatorNode.setType(StageType.SINK);
            } else if (filters.containsKey(operatorNode.getName())) {
                operatorNode.setType(StageType.FILTER);
            } else if (splits.containsKey(operatorNode.getName())) {
                operatorNode.setType(StageType.SPLIT);
            } else if (preprocessingPipelines.containsKey(operatorNode.getName())) {
                operatorNode.setType(StageType.PREPROCESSING);
            } else if (processingPipelines.containsKey(operatorNode.getName())) {
                operatorNode.setType(StageType.PROCESSING);
            } else if (postprocessingPipelines.containsKey(operatorNode.getName())) {
                operatorNode.setType(StageType.POSTPROCESSING);
            } else {
                throw new JobExecuteException("unsupported process type " + operatorNode.getName());
            }
        }

        return operatorNodes;

    }


    public void execute() throws JobExecuteException {
        if (!jobRuntimeEnvironment.isLocalMode() && !jobRuntimeEnvironment.isTestMode()) {
            jobRuntimeEnvironment.registerPlugin(jarPaths);
        }
        List<OperatorNode> sourceOperatorNodes = operatorNodes
                .stream().filter(v -> v.getType().name().equals(StageType.SOURCE.name())).collect(Collectors.toList());

        DataStream<Event> dataStream = null;

        for (OperatorNode sourceOperatorNode : sourceOperatorNodes) {
            dataStream = sourceExecutor.execute(dataStream, sourceOperatorNode);
            for (String nodeName : sourceOperatorNode.getDownstream()) {
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
        OperatorNode operatorNode = getNode(downstreamNodeName).orElseGet(() -> {
            throw new JobExecuteException("Can't find downstream node " + downstreamNodeName);
        });
        if (operatorNode.getType().name().equals(StageType.FILTER.name())) {
            if (nodeNameWithSplitTags.containsKey(operatorNode.getName())) {
                dataStream = filterExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream)
                                .getSideOutput(new OutputTag<Event>(nodeNameWithSplitTags.get(operatorNode.getName())) {}), operatorNode);
            } else {
                dataStream = filterExecutor.execute(dataStream, operatorNode);
            }
        } else if (operatorNode.getType().name().equals(StageType.SPLIT.name())) {
            if (operatorNode.getTags().size() == operatorNode.getDownstream().size()) {
                for (int i = 0; i < operatorNode.getDownstream().size(); i++) {
                    nodeNameWithSplitTags.put(operatorNode.getDownstream().get(i), operatorNode.getTags().get(i));
                }
            }
            else {
                throw new JobExecuteException("split node downstream size not equal tags size");
            }
            dataStream = splitExecutor.execute(dataStream, operatorNode);
        } else if (operatorNode.getType().name().equals(StageType.PREPROCESSING.name())) {
            if (nodeNameWithSplitTags.containsKey(operatorNode.getName())) {
                dataStream = preprocessingExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(nodeNameWithSplitTags.get(operatorNode.getName())){
                }), operatorNode);
            } else {
                dataStream = preprocessingExecutor.execute(dataStream, operatorNode);
            }
        } else if (operatorNode.getType().name().equals(StageType.PROCESSING.name())) {
            if (nodeNameWithSplitTags.containsKey(operatorNode.getName())) {
                dataStream = processingExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(nodeNameWithSplitTags.get(operatorNode.getName())) {
                }), operatorNode);
            } else {
                dataStream = processingExecutor.execute(dataStream, operatorNode);
            }
        } else if (operatorNode.getType().name().equals(StageType.POSTPROCESSING.name())) {
            if (nodeNameWithSplitTags.containsKey(operatorNode.getName())) {
                dataStream = postprocessingExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(nodeNameWithSplitTags.get(operatorNode.getName())) {
                }), operatorNode);
            } else {
                dataStream = postprocessingExecutor.execute(dataStream, operatorNode);
            }
        } else if (operatorNode.getType().name().equals(StageType.SINK.name())) {
            if (nodeNameWithSplitTags.containsKey(operatorNode.getName())) {
                dataStream = sinkExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(nodeNameWithSplitTags.get(operatorNode.getName())) {
                }), operatorNode);
            } else {
                dataStream = sinkExecutor.execute(dataStream, operatorNode);
            }
        } else {
            throw new JobExecuteException("unsupported process type " + operatorNode.getType().name());
        }


        for (String nodeName : operatorNode.getDownstream()) {
            buildJobGraph(dataStream, nodeName);
        }


    }

    private Optional<OperatorNode> getNode(String name) {
        return operatorNodes.stream().filter(v -> v.getName().equals(name)).findFirst();
    }


}
