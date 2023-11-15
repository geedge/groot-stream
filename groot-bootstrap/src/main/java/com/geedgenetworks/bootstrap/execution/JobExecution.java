package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.bootstrap.enums.DeployMode;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.main.GrootStreamRunner;
import com.geedgenetworks.bootstrap.utils.ExecutionConfigKey;
import com.geedgenetworks.bootstrap.utils.StartBuilder;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValueFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

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
    private final ExecuteProcessor<SingleOutputStreamOperator, JobRuntimeEnvironment> sourceExecuteProcessor;
    private final ExecuteProcessor<SingleOutputStreamOperator, JobRuntimeEnvironment> sinkExecuteProcessor;
    private final ExecuteProcessor<SingleOutputStreamOperator, JobRuntimeEnvironment> filterExecuteProcessor;
    private final ExecuteProcessor<SingleOutputStreamOperator, JobRuntimeEnvironment> preprocessingExecuteProcessor;
    private final ExecuteProcessor<SingleOutputStreamOperator, JobRuntimeEnvironment> processingExecuteProcessor;
    private final ExecuteProcessor<SingleOutputStreamOperator, JobRuntimeEnvironment> postprocessingExecuteProcessor;
    private final DAGInfo dagInfo;

    private final List<URL> jarPaths;
    public JobExecution(Config config) {
       try {
            jarPaths = new ArrayList<>(Collections.singletonList(new File(StartBuilder.appBootstrapDir()
                    .resolve(GrootStreamRunner.APP_JAR_NAME).toString())
                    .toURI().toURL()));
        } catch (MalformedURLException e) {
            throw new JobExecuteException("load groot stream bootstrap jar error.", e);
        }

        registerPlugin(config.getConfig(Constants.APPLICATION));

        this.sourceExecuteProcessor = new SourceExecuteProcessor(jarPaths, config);
        this.sinkExecuteProcessor = new SinkExecuteProcessor(jarPaths, config);
        this.filterExecuteProcessor = new FilterExecuteProcessor(jarPaths, config);
        this.preprocessingExecuteProcessor = new PreprocessingExecuteProcessor(jarPaths, config);
        this.processingExecuteProcessor = new ProcessingExecuteProcessor(jarPaths, config);
        this.postprocessingExecuteProcessor = new PostprocessingExecuteProcessor(jarPaths, config);
        this.jobRuntimeEnvironment =
                JobRuntimeEnvironment.getInstance(this.registerPlugin(config, jarPaths));

        this.sourceExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.sinkExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.filterExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.preprocessingExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.processingExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.postprocessingExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.dagInfo = buildJobDAG(config);

    }

    private void registerPlugin(Config appConfig) {
        List<Path> thirdPartyJars = new ArrayList<>();
        Config envConfig = appConfig.getConfig(Constants.APPLICATION_ENV);
        if(envConfig.hasPath(ExecutionConfigKey.JARS)) {
            thirdPartyJars = new ArrayList<>(StartBuilder
                    .getThirdPartyJars(envConfig.getString(ExecutionConfigKey.JARS)));
        }
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
            AbstractExecuteProcessor.ADD_URL_TO_CLASSLOADER.accept(Thread.currentThread().getContextClassLoader(), url);
        });
        jarPaths.addAll(jarDependencies);

    }


    private Config registerPlugin(Config config , List<URL> jars) {
       config = this.injectJarsToConfig(
               config, ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV, ExecutionConfigKey.PIPELINE_JARS), jars);
       return  this.injectJarsToConfig(
                config, ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV, ExecutionConfigKey.PIPELINE_CLASSPATHS), jars);
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

    private DAGInfo buildJobDAG(Config config) {

        Map<String, Object> sources = null;
        Map<String, Object> sinks = null;
        Map<String, Object> filters = null;
        Map<String, Object> preprocessingPipelines = null;
        Map<String, Object> processingPipelines = null;
        Map<String, Object> postprocessingPipelines = null;

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

        List<? extends Config> topology = jobRuntimeEnvironment.getConfig().getConfigList(Constants.APPLICATION_TOPOLOGY);

        List<Edge> edges = Lists.newArrayList();

        topology.forEach(item -> {
            Edge edge = JSONObject.from(item.root().unwrapped()).toJavaObject(Edge.class);
            edges.add(edge);
        });
        List<String> vertexList = new ArrayList<>();
        for(Edge edge : edges) {
            vertexList.add(edge.getInput());
            vertexList.addAll(edge.getTargets());
        }
        vertexList = vertexList.stream().distinct().collect(Collectors.toList());

        List<VertexInfo> vertexInfoList = new ArrayList<>();
        for(String vertex : vertexList) {
            VertexInfo vertexInfo = new VertexInfo();
            vertexInfo.setName(vertex);
            if (sources.containsKey(vertex)) {
                vertexInfo.setType(ProcessorType.SOURCE);
            } else if (sinks.containsKey(vertex)) {
                vertexInfo.setType(ProcessorType.SINK);
            } else if (filters.containsKey(vertex)) {
                vertexInfo.setType(ProcessorType.FILTER);
            } else if (preprocessingPipelines.containsKey(vertex)) {
                vertexInfo.setType(ProcessorType.PREPROCESSING);
            } else if (processingPipelines.containsKey(vertex)) {
                vertexInfo.setType(ProcessorType.PROCESSING);
            } else if (postprocessingPipelines.containsKey(vertex)) {
                vertexInfo.setType(ProcessorType.POSTPROCESSING);
            }
            vertexInfoList.add(vertexInfo);
        }
        DAGInfo tmpDAG = new DAGInfo();
        tmpDAG.setEdges(edges);
        tmpDAG.setVertexList(vertexInfoList);
        return tmpDAG;

    }


    public void execute() throws JobExecuteException {

        if (!StartBuilder.getDeployMode().name().equals(DeployMode.CLIENT.name())) {
            jobRuntimeEnvironment.registerPlugin(jarPaths);
        }
        List<VertexInfo> sourceVertexInfoList = dagInfo.getVertexList()
                .stream().filter(v -> v.getType().name().equals(ProcessorType.SOURCE.name())).collect(Collectors.toList());

        SingleOutputStreamOperator<Event> singleOutputStreamOperator = null;

        for(VertexInfo sourceVertex : sourceVertexInfoList) {
            Edge sourceEdge = next(sourceVertex.getName());
            singleOutputStreamOperator = sourceExecuteProcessor.execute(singleOutputStreamOperator, sourceEdge);
            for (String target : sourceEdge.getTargets()) {
                buildJobGraph(singleOutputStreamOperator, sourceEdge, target);
            }
        }

        try {
            log.info("Execution Plan: {}", jobRuntimeEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
            log.info('\n' + Constants.GROOT_LOGO);
            jobRuntimeEnvironment.getStreamExecutionEnvironment()
                    .execute(jobRuntimeEnvironment.getConfig().getString(Constants.APPLICATION_NAME));
        } catch (Exception e) {
            throw new JobExecuteException("Execute job error", e);
        }

    }

    private void buildJobGraph(SingleOutputStreamOperator<Event> singleOutputStreamOperator, Edge edge, String target) {
        VertexInfo vertexInfo = dagInfo.getVertexList().stream().filter(v -> v.getName().equals(target)).findFirst().orElse(null);
        Edge nextEdge = next(target);
        if (vertexInfo.getType().name().equals(ProcessorType.FILTER.name())) {
            singleOutputStreamOperator = filterExecuteProcessor.execute(singleOutputStreamOperator, nextEdge);
        } else if (vertexInfo.getType().name().equals(ProcessorType.PREPROCESSING.name())) {
            singleOutputStreamOperator = preprocessingExecuteProcessor.execute(singleOutputStreamOperator, nextEdge);
        } else if (vertexInfo.getType().name().equals(ProcessorType.PROCESSING.name())) {
            singleOutputStreamOperator = processingExecuteProcessor.execute(singleOutputStreamOperator, nextEdge);
        } else if (vertexInfo.getType().name().equals(ProcessorType.POSTPROCESSING.name())) {
            singleOutputStreamOperator = postprocessingExecuteProcessor.execute(singleOutputStreamOperator, nextEdge);
        } else if (vertexInfo.getType().name().equals(ProcessorType.SINK.name())) {
            Edge endEdge = new Edge();
            endEdge.setInput(target);
            endEdge.setTargets(Lists.newArrayList(target));
            singleOutputStreamOperator = sinkExecuteProcessor.execute(singleOutputStreamOperator, endEdge);
        } else {
            throw  new JobExecuteException("unsupported process type " + vertexInfo.getType().name());
        }

        if (nextEdge != null) {
            for (String nextTarget : nextEdge.getTargets()) {
                buildJobGraph(singleOutputStreamOperator, nextEdge, nextTarget);
            }
        }

    }

    private Edge next(String input) {
        Edge edge = dagInfo.getEdges().stream().filter(v-> v.getInput().equals(input)).findFirst().orElse(null);
        if (StringUtil.isNotEmpty(edge)) {
            dagInfo.getEdges().remove(edge);
        }
        return edge;
    }


}
