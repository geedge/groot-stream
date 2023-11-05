package com.geedgenetworks.bootstrap.execution;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.main.GrootStreamRunner;
import com.geedgenetworks.bootstrap.utils.ExecutionConfigKey;
import com.geedgenetworks.bootstrap.utils.StartBuilder;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.GrootYamlParser;
import com.geedgenetworks.core.pojo.*;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Lists;
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
    public JobExecution(Map<String, Object> config) {
        try {
            jarPaths = new ArrayList<>(Collections.singletonList(new File(StartBuilder.appBootstrapDir()
                    .resolve(GrootStreamRunner.APP_JAR_NAME).toString())
                    .toURI().toURL()));
        } catch (MalformedURLException e) {
            throw new JobExecuteException("load groot stream bootstrap error.", e);
        }

        registerPlugin(GrootYamlParser.getMap(Constants.APPLICATION, config));

        this.sourceExecuteProcessor = new SourceExecuteProcessor(config);
        this.sinkExecuteProcessor = new SinkExecuteProcessor(config);
        this.filterExecuteProcessor = new FilterExecuteProcessor(config);
        this.preprocessingExecuteProcessor = new PreprocessingExecuteProcessor(config);
        this.processingExecuteProcessor = new ProcessingExecuteProcessor(config);
        this.postprocessingExecuteProcessor = new PostprocessingExecuteProcessor(config);

        this.jobRuntimeEnvironment =
                JobRuntimeEnvironment.getInstance(this.registerPlugin(GrootYamlParser.getMap(Constants.APPLICATION, config), jarPaths));

        this.sourceExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.sinkExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.filterExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.preprocessingExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.processingExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.postprocessingExecuteProcessor.setRuntimeEnvironment(jobRuntimeEnvironment);
        this.dagInfo = buildJobDAG(config);

    }

    private void registerPlugin(Map<String, Object> appConfig) {
        List<Path> thirdPartyJars = new ArrayList<>();
        Map<String, Object> envConfig = GrootYamlParser.getMap(Constants.APPLICATION_ENV_OPTIONS,appConfig);
        if(envConfig.containsKey(ExecutionConfigKey.JARS)) {
            thirdPartyJars = new ArrayList<>(StartBuilder.getThirdPartyJars(GrootYamlParser.getStringProperty(ExecutionConfigKey.JARS, envConfig)));
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
            //todo
        });
        jarPaths.addAll(jarDependencies);

    }


    private Map<String, Object> registerPlugin(Map<String, Object> appConfig, List<URL> jars) {
       this.injectJarsToConfig(
                appConfig, ExecutionConfigKey.PIPELINE_JARS, jars);
        this.injectJarsToConfig(
                appConfig, ExecutionConfigKey.PIPELINE_CLASSPATHS, jars);
        return appConfig;
    }


    private void injectJarsToConfig(Map<String, Object>  appConfig, String key, List<URL> jars) {
        Map<String, Object> envConfig = GrootYamlParser.getMap(Constants.APPLICATION_ENV_OPTIONS,appConfig);
        List<URL> validJars = new ArrayList<>();
        for (URL jarUrl : jars) {
            if (new File(jarUrl.getFile()).exists()) {
                validJars.add(jarUrl);
                log.info("Inject jar to config: {}", jarUrl);
            } else {
                log.warn("Remove invalid jar when inject jars into config: {}", jarUrl);
            }
        }

        if (envConfig.containsKey(key)) {
            Set<URL> paths =
                    Arrays.stream(GrootYamlParser.getStringProperty(key, envConfig).split(";"))
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

            envConfig.put(key, paths.stream()
                    .map(URL::toString)
                    .distinct()
                    .collect(Collectors.joining(";")));


        } else {
            envConfig.put(key, validJars.stream()
                    .map(URL::toString)
                    .distinct()
                    .collect(Collectors.joining(";")));
        }
    }

    private DAGInfo buildJobDAG(Map<String, Object> config) {
        Map<String, Object> sources = GrootYamlParser.getMap(Constants.SOURCES, config);
        Map<String, Object> sinks = GrootYamlParser.getMap(Constants.SINKS, config);
        Map<String, Object> filters = GrootYamlParser.getMap(Constants.FILTERS, config);
        Map<String, Object> preprocessingPipelines =
                GrootYamlParser.getMap(Constants.PREPROCESSING_PIPELINES, config);
        Map<String, Object> processingPipelines =
                GrootYamlParser.getMap(Constants.PROCESSING_PIPELINES, config);
        Map<String, Object> postprocessingPipelines =
                GrootYamlParser.getMap(Constants.POSTPROCESSING_PIPELINES, config);

        List topology = GrootYamlParser.getList(Constants.APPLICATION_TOPOLOGY, jobRuntimeEnvironment.getConfig());

        List<Edge> edges = Lists.newArrayList();
        topology.forEach(item -> {
            Edge edge = com.alibaba.fastjson2.JSONObject.from(item).toJavaObject(Edge.class);
            edges.add(edge);
        });
        List<String> vertexList = new ArrayList<>();
        for(Edge edge : edges) {
            vertexList.add(edge.getInput());
            vertexList.add(edge.getTarget());
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

        List<VertexInfo> sourceVertexInfoList = dagInfo.getVertexList()
                .stream().filter(v -> v.getType().name().equals(ProcessorType.SOURCE.name())).collect(Collectors.toList());

        SingleOutputStreamOperator<Event> singleOutputStreamOperator = null;

        for(VertexInfo sourceVertex : sourceVertexInfoList) {
            Edge start = next(sourceVertex.getName());
            singleOutputStreamOperator = sourceExecuteProcessor.execute(singleOutputStreamOperator, start);
            buildRouterPipeline(singleOutputStreamOperator, next(start.getTarget()));
        }

        try {
            jobRuntimeEnvironment.getStreamExecutionEnvironment()
                    .execute(jobRuntimeEnvironment.getConfig().getOrDefault("name", Constants.DEFAULT_JOB_NAME).toString());
        } catch (Exception e) {
            throw new JobExecuteException("Job Execute error", e);
        }

    }

    private void buildRouterPipeline(SingleOutputStreamOperator<Event> singleOutputStreamOperator, Edge edge) {
        VertexInfo vertexInfo = dagInfo.getVertexList().stream().filter(v -> v.getName().equals(edge.getInput())).findFirst().orElse(null);
        if (vertexInfo.getType().name().equals(ProcessorType.FILTER.name())) {
            singleOutputStreamOperator = filterExecuteProcessor.execute(singleOutputStreamOperator, edge);
        } else if (vertexInfo.getType().name().equals(ProcessorType.PREPROCESSING.name())) {
            singleOutputStreamOperator = preprocessingExecuteProcessor.execute(singleOutputStreamOperator, edge);
        } else if (vertexInfo.getType().name().equals(ProcessorType.PROCESSING.name())) {
            singleOutputStreamOperator = processingExecuteProcessor.execute(singleOutputStreamOperator, edge);
        } else if (vertexInfo.getType().name().equals(ProcessorType.POSTPROCESSING.name())) {
            singleOutputStreamOperator = postprocessingExecuteProcessor.execute(singleOutputStreamOperator, edge);
        } else {
            throw  new JobExecuteException("unsupported process type " + vertexInfo.getType().name());
        }

        Edge nextEdge = next(edge.getTarget());
        if (StringUtil.isNotEmpty(nextEdge)) {
            buildRouterPipeline(singleOutputStreamOperator, nextEdge);
        } else {
            vertexInfo = dagInfo.getVertexList().stream().filter(v -> v.getName().equals(edge.getTarget())).findFirst().orElse(null);
            if (vertexInfo.getType().name().equals(ProcessorType.SINK.name())) {
                singleOutputStreamOperator = sinkExecuteProcessor.execute(singleOutputStreamOperator, edge);
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
