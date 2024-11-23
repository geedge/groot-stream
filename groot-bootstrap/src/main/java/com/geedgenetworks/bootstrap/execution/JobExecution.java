package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.bootstrap.enums.JobStage;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.main.GrootStreamRunner;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.GrootStreamConfig;
import com.geedgenetworks.api.event.Event;
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

import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkArgument;
import static org.apache.flink.shaded.guava18.com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class JobExecution {

    private final JobRuntimeEnvironment jobRuntimeEnvironment;
    private final Executor<DataStream<Event>> sourceExecutor;
    private final Executor<DataStream<Event>> sinkExecutor;
    private final Executor<DataStream<Event>> processorExecutor;
    private final List<JobTopologyNode> jobTopologyNodes;
    private final List<URL> jarPaths;

    public JobExecution(Config jobConfig, GrootStreamConfig grootStreamConfig) {
        try {
            jarPaths = new ArrayList<>(Collections.singletonList(new File(StartBuilder.bootstrapDir()
                    .resolve(GrootStreamRunner.APP_JAR_NAME).toString())
                    .toURI().toURL()));
        } catch (MalformedURLException e) {
            throw new JobExecuteException("Load GrootStream Bootstrap jar failed.", e);
        }

        registerPlugin(jobConfig.getConfig(Constants.APPLICATION));

        this.jobRuntimeEnvironment =
                JobRuntimeEnvironment.getInstance(this.configureExecutionJars(jobConfig, jarPaths), grootStreamConfig);

        this.sourceExecutor = new SourceExecutor(jobRuntimeEnvironment, jobConfig);
        this.sinkExecutor = new SinkExecutor(jobRuntimeEnvironment, jobConfig);
        this.processorExecutor = new ProcessorExecutor(jobRuntimeEnvironment, jobConfig);
        this.jobTopologyNodes = buildJobNode(jobConfig);

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
                        throw new RuntimeException("The uri of jar illegal:" + uri, e);
                    }
                }).collect(Collectors.toList());
        jarDependencies.forEach(url -> {
            AbstractExecutor.ADD_URL_TO_CLASSLOADER.accept(Thread.currentThread().getContextClassLoader(), url);
        });
        jarPaths.addAll(jarDependencies);

    }


    private Config configureExecutionJars(Config config, List<URL> jars) {
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

    private List<JobTopologyNode> buildJobNode(Config config) {

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

        List<JobTopologyNode> jobTopologyNodes = Lists.newArrayList();

        topology.forEach(item -> {
            JobTopologyNode jobTopologyNode = JSONObject.from(item.root().unwrapped()).toJavaObject(JobTopologyNode.class);
            jobTopologyNodes.add(jobTopologyNode);
        });

        for (JobTopologyNode jobTopologyNode : jobTopologyNodes) {
            checkNotNull(jobTopologyNode.getName(), "Node name is null");
            if (sources.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setStage(JobStage.SOURCE);
            } else if (sinks.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setStage(JobStage.SINK);
            } else if (filters.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setStage(JobStage.FILTER);
            } else if (splits.containsKey(jobTopologyNode.getName())) {
                checkArgument( jobTopologyNode.getTags().size() == jobTopologyNode.getDownstream().size(),
                        "split node downstream size not equal tags size");
                jobTopologyNode.setStage(JobStage.SPLIT);
            } else if (preprocessingPipelines.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setStage(JobStage.PREPROCESSING);
            } else if (processingPipelines.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setStage(JobStage.PROCESSING);
            } else if (postprocessingPipelines.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setStage(JobStage.POSTPROCESSING);
            } else {
                throw new JobExecuteException("Unsupported operator type " + jobTopologyNode.getName());
            }
        }

        return jobTopologyNodes;

    }


    public void execute() throws JobExecuteException {

        if (!jobRuntimeEnvironment.isLocalMode() && !jobRuntimeEnvironment.isTestMode()) {
            jobRuntimeEnvironment.registerPlugin(jarPaths);
        }
        List<JobTopologyNode> sourceNodes = jobTopologyNodes
                .stream().filter(v -> v.getStage().name().equals(JobStage.SOURCE.name())).collect(Collectors.toList());

        DataStream<Event> dataStream = null;

        for (JobTopologyNode sourceNode : sourceNodes) {
            dataStream = sourceExecutor.execute(dataStream, sourceNode);
            for (String nodeName : sourceNode.getDownstream()) {
                buildJobGraph(dataStream, sourceNode, nodeName);
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
            throw new JobExecuteException("Job execution failed.", e);
        }

    }

    private void buildJobGraph(DataStream<Event> dataStream, JobTopologyNode inputNode, String nodeName) {
        JobTopologyNode currentNode = getNode(nodeName).orElseThrow(() ->
                                    new JobExecuteException("Node not found: " + nodeName));
        switch (currentNode.getStage()) {
            case SPLIT:
            case FILTER:
            case PREPROCESSING:
            case PROCESSING:
            case POSTPROCESSING:
                dataStream = executeProcessorNode(dataStream, inputNode,currentNode);
                break;
            case SINK:
                dataStream = executeSinkNode(dataStream, inputNode, currentNode);
                break;
            default:
                throw new JobExecuteException("Unsupported Job stage: " + currentNode.getStage());
        }
        // Recursively build job graph for downstream nodes
        for (String downstreamNodeName : currentNode.getDownstream()) {
            buildJobGraph(dataStream, currentNode, downstreamNodeName);
        }

    }

    private DataStream<Event> executeProcessorNode(DataStream<Event> dataStream, JobTopologyNode inputNode, JobTopologyNode currentNode) {
        if (isSplitAndHasTag(inputNode, currentNode)) {
            return executeWithSideOutput(dataStream, inputNode, currentNode, processorExecutor);
        } else {
            return processorExecutor.execute(dataStream, currentNode);
        }
    }

    private DataStream<Event> executeSinkNode(DataStream<Event> dataStream, JobTopologyNode inputNode, JobTopologyNode currentNode) {
        if (isSplitAndHasTag(inputNode, currentNode)) {
            return executeWithSideOutput(dataStream, inputNode, currentNode, sinkExecutor);
        } else {
            return sinkExecutor.execute(dataStream, currentNode);
        }
    }

    // Helper method to check if input node is SPLIT and has a tag for downstream
    private boolean isSplitAndHasTag(JobTopologyNode inputNode, JobTopologyNode currentNode) {
        return inputNode.getStage().equals(JobStage.SPLIT)
                && inputNode.getTagForDownstream(currentNode.getName()) != null;
    }

    // Helper method to execute with side output
    private DataStream<Event> executeWithSideOutput(DataStream<Event> dataStream, JobTopologyNode inputNode,
                                                    JobTopologyNode currentNode, Executor<DataStream<Event>> executor) {
        OutputTag<Event> outputTag = new OutputTag<Event>(inputNode.getTagForDownstream(currentNode.getName())) {};
        SingleOutputStreamOperator<Event> singleOutputStream = (SingleOutputStreamOperator<Event>) dataStream;
        DataStream<Event> sideOutput = singleOutputStream.getSideOutput(outputTag);
        return executor.execute(sideOutput, currentNode);
    }

    // Helper method to get the node based on the name
    private Optional<JobTopologyNode> getNode(String name) {
        return jobTopologyNodes.stream().filter(v -> v.getName().equals(name)).findFirst();
    }


}
