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

@Slf4j
public class JobExecution {

    private final JobRuntimeEnvironment jobRuntimeEnvironment;
    private final Executor<DataStream<Event>> sourceExecutor;
    private final Executor<DataStream<Event>> sinkExecutor;
    private final Executor<DataStream<Event>> processorExecutor;
    private final List<JobTopologyNode> jobTopologyNodes;
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
        this.jobRuntimeEnvironment =
                JobRuntimeEnvironment.getInstance(this.registerPlugin(jobConfig, jarPaths), grootStreamConfig);

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
            if (sources.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setType(JobStage.SOURCE);
            } else if (sinks.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setType(JobStage.SINK);
            } else if (filters.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setType(JobStage.FILTER);
            } else if (splits.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setType(JobStage.SPLIT);
            } else if (preprocessingPipelines.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setType(JobStage.PREPROCESSING);
            } else if (processingPipelines.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setType(JobStage.PROCESSING);
            } else if (postprocessingPipelines.containsKey(jobTopologyNode.getName())) {
                jobTopologyNode.setType(JobStage.POSTPROCESSING);
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
        List<JobTopologyNode> sourceJobTopologyNodes = jobTopologyNodes
                .stream().filter(v -> v.getType().name().equals(JobStage.SOURCE.name())).collect(Collectors.toList());

        DataStream<Event> dataStream = null;

        for (JobTopologyNode sourceJobTopologyNode : sourceJobTopologyNodes) {
            dataStream = sourceExecutor.execute(dataStream, sourceJobTopologyNode);
            for (String nodeName : sourceJobTopologyNode.getDownstream()) {
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
        JobTopologyNode jobTopologyNode = getNode(downstreamNodeName).orElseGet(() -> {
            throw new JobExecuteException("Can't find downstream node " + downstreamNodeName);
        });
        if (jobTopologyNode.getType().name().equals(JobStage.FILTER.name())) {
            if (nodeNameWithSplitTags.containsKey(jobTopologyNode.getName())) {
                dataStream = processorExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream)
                                .getSideOutput(new OutputTag<Event>(nodeNameWithSplitTags.get(jobTopologyNode.getName())) {}), jobTopologyNode);
            } else {
                dataStream = processorExecutor.execute(dataStream, jobTopologyNode);
            }
        } else if (jobTopologyNode.getType().name().equals(JobStage.SPLIT.name())) {
            if (jobTopologyNode.getTags().size() == jobTopologyNode.getDownstream().size()) {
                for (int i = 0; i < jobTopologyNode.getDownstream().size(); i++) {
                    nodeNameWithSplitTags.put(jobTopologyNode.getDownstream().get(i), jobTopologyNode.getTags().get(i));
                }
            }
            else {
                throw new JobExecuteException("split node downstream size not equal tags size");
            }
            dataStream = processorExecutor.execute(dataStream, jobTopologyNode);
        } else if (jobTopologyNode.getType().name().equals(JobStage.PREPROCESSING.name())) {
            if (nodeNameWithSplitTags.containsKey(jobTopologyNode.getName())) {
                dataStream = processorExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(nodeNameWithSplitTags.get(jobTopologyNode.getName())){
                }), jobTopologyNode);
            } else {
                dataStream = processorExecutor.execute(dataStream, jobTopologyNode);
            }
        } else if (jobTopologyNode.getType().name().equals(JobStage.PROCESSING.name())) {
            if (nodeNameWithSplitTags.containsKey(jobTopologyNode.getName())) {
                dataStream = processorExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(nodeNameWithSplitTags.get(jobTopologyNode.getName())) {
                }), jobTopologyNode);
            } else {
                dataStream = processorExecutor.execute(dataStream, jobTopologyNode);
            }
        } else if (jobTopologyNode.getType().name().equals(JobStage.POSTPROCESSING.name())) {
            if (nodeNameWithSplitTags.containsKey(jobTopologyNode.getName())) {
                dataStream = processorExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(nodeNameWithSplitTags.get(jobTopologyNode.getName())) {
                }), jobTopologyNode);
            } else {
                dataStream = processorExecutor.execute(dataStream, jobTopologyNode);
            }
        } else if (jobTopologyNode.getType().name().equals(JobStage.SINK.name())) {
            if (nodeNameWithSplitTags.containsKey(jobTopologyNode.getName())) {
                dataStream = sinkExecutor.execute(((SingleOutputStreamOperator<Event>) dataStream).getSideOutput(new OutputTag<Event>(nodeNameWithSplitTags.get(jobTopologyNode.getName())) {
                }), jobTopologyNode);
            } else {
                dataStream = sinkExecutor.execute(dataStream, jobTopologyNode);
            }
        } else {
            throw new JobExecuteException("unsupported process type " + jobTopologyNode.getType().name());
        }


        for (String nodeName : jobTopologyNode.getDownstream()) {
            buildJobGraph(dataStream, nodeName);
        }


    }

    private Optional<JobTopologyNode> getNode(String name) {
        return jobTopologyNodes.stream().filter(v -> v.getName().equals(name)).findFirst();
    }


}
