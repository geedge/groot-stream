package com.starrocks.connector.flink.table.sink;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.spi.metrics.InternalMetrics;
import com.geedgenetworks.spi.table.event.Event;
import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.manager.StarRocksStreamLoadListener;
import com.starrocks.connector.flink.tools.EnvUtils;
import com.starrocks.connector.flink.tools.JsonWrapper;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class EventStarRocksDynamicSinkFunctionV2 extends StarRocksDynamicSinkFunctionBase<Event> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(StarRocksDynamicSinkFunctionV2.class);

    private static final int NESTED_ROW_DATA_HEADER_SIZE = 256;

    private final StarRocksSinkOptions sinkOptions;
    private final boolean logFailuresOnly;
    private final StreamLoadProperties properties;
    private StreamLoadManagerV2 sinkManager;

    private transient volatile ListState<StarrocksSnapshotState> snapshotStates;

    private transient long restoredCheckpointId;

    private transient List<ExactlyOnceLabelGeneratorSnapshot> restoredGeneratorSnapshots;

    private transient Map<Long, List<StreamLoadSnapshot>> snapshotMap;

    private transient StarRocksStreamLoadListener streamLoadListener;

    // Only valid when using exactly-once and label prefix is set
    @Nullable
    private transient ExactlyOnceLabelGeneratorFactory exactlyOnceLabelFactory;

    @Deprecated
    private transient ListState<Map<String, StarRocksSinkBufferEntity>> legacyState;
    @Deprecated
    private transient List<StarRocksSinkBufferEntity> legacyData;
    private transient JsonWrapper jsonWrapper;
    private transient InternalMetrics internalMetrics;

    public EventStarRocksDynamicSinkFunctionV2(StarRocksSinkOptions sinkOptions, boolean logFailuresOnly) {
        Preconditions.checkArgument(sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE);
        this.sinkOptions = sinkOptions;
        this.logFailuresOnly = logFailuresOnly;
        this.properties = sinkOptions.getProperties(null);
        this.sinkManager = new StreamLoadManagerV2(sinkOptions.getProperties(null),
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE);
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {
        internalMetrics.incrementInEvents(1);
        String json;
        try {
            json = JSON.toJSONString(value.getExtractedFields());
        } catch (Exception e) {
            internalMetrics.incrementErrorEvents(1);
            log.error("json convert error", e);
            return;
        }
        try {
            sinkManager.write(null, sinkOptions.getDatabaseName(), sinkOptions.getTableName(), json);
        } catch (Exception e) {
            internalMetrics.incrementErrorEvents(1);
            if (logFailuresOnly) {
                log.error("write error", e);
                resetSinkManager();
            } else {
                throw e;
            }
        }
    }

    private void resetSinkManager(){
        try {
            StreamLoadSnapshot snapshot = sinkManager.snapshot();
            sinkManager.abort(snapshot);
        } catch (Exception ex) {
            log.error("write error", ex);
        }
        sinkManager.close();

        this.sinkManager = new StreamLoadManagerV2(this.properties,
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE);
        sinkManager.setStreamLoadListener(streamLoadListener);

        LabelGeneratorFactory labelGeneratorFactory;
        String labelPrefix = sinkOptions.getLabelPrefix();
        if (labelPrefix == null ||
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE ||
                !sinkOptions.isEnableExactlyOnceLabelGen()) {
            labelGeneratorFactory = new LabelGeneratorFactory.DefaultLabelGeneratorFactory(
                    labelPrefix == null ? "flink" : labelPrefix);
        } else {
            labelGeneratorFactory = exactlyOnceLabelFactory;
        }
        sinkManager.setLabelGeneratorFactory(labelGeneratorFactory);

        sinkManager.init();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        internalMetrics = new InternalMetrics(getRuntimeContext());
        this.streamLoadListener = new EventStreamLoadListener(getRuntimeContext(), sinkOptions, internalMetrics);
        sinkManager.setStreamLoadListener(streamLoadListener);

        LabelGeneratorFactory labelGeneratorFactory;
        String labelPrefix = sinkOptions.getLabelPrefix();
        if (labelPrefix == null ||
                sinkOptions.getSemantic() == StarRocksSinkSemantic.AT_LEAST_ONCE ||
                !sinkOptions.isEnableExactlyOnceLabelGen()) {
            labelGeneratorFactory = new LabelGeneratorFactory.DefaultLabelGeneratorFactory(
                    labelPrefix == null ? "flink" : labelPrefix);
        } else {
            this.exactlyOnceLabelFactory = new ExactlyOnceLabelGeneratorFactory(
                    labelPrefix,
                    getRuntimeContext().getNumberOfParallelSubtasks(),
                    getRuntimeContext().getIndexOfThisSubtask(),
                    restoredCheckpointId);
            exactlyOnceLabelFactory.restore(restoredGeneratorSnapshots);
            labelGeneratorFactory = exactlyOnceLabelFactory;
        }
        sinkManager.setLabelGeneratorFactory(labelGeneratorFactory);

        sinkManager.init();

        if (sinkOptions.getSemantic() == StarRocksSinkSemantic.EXACTLY_ONCE) {
            openForExactlyOnce();
        }

        log.info("Open sink function v2. {}", EnvUtils.getGitInformation());
    }

    private void openForExactlyOnce() throws Exception {
        if (sinkOptions.isAbortLingeringTxns()) {
            LingeringTransactionAborter aborter = new LingeringTransactionAborter(
                    sinkOptions.getLabelPrefix(),
                    restoredCheckpointId,
                    getRuntimeContext().getIndexOfThisSubtask(),
                    sinkOptions.getAbortCheckNumTxns(),
                    sinkOptions.getDbTables(),
                    restoredGeneratorSnapshots,
                    sinkManager.getStreamLoader());
            aborter.execute();
        }

        notifyCheckpointComplete(Long.MAX_VALUE);
    }

    private JsonWrapper getOrCreateJsonWrapper() {
        if (jsonWrapper == null) {
            this.jsonWrapper = new JsonWrapper();
        }

        return jsonWrapper;
    }

    public void finish() {
        sinkManager.flush();
    }

    @Override
    public void close() {
        log.info("Close sink function");
        try {
            sinkManager.flush();
        } catch (Exception e) {
            log.error("Failed to flush when closing", e);
            throw e;
        } finally {
            StreamLoadSnapshot snapshot = sinkManager.snapshot();
            sinkManager.abort(snapshot);
            sinkManager.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        sinkManager.flush();
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return;
        }

        StreamLoadSnapshot snapshot = sinkManager.snapshot();

        if (sinkManager.prepare(snapshot)) {
            snapshotMap.put(functionSnapshotContext.getCheckpointId(), Collections.singletonList(snapshot));

            snapshotStates.clear();
            List<ExactlyOnceLabelGeneratorSnapshot> labelSnapshots = exactlyOnceLabelFactory == null ? null
                    : exactlyOnceLabelFactory.snapshot(functionSnapshotContext.getCheckpointId());
            snapshotStates.add(StarrocksSnapshotState.of(snapshotMap, labelSnapshots));
        } else {
            sinkManager.abort(snapshot);
            throw new RuntimeException("Snapshot state failed by prepare");
        }

        if (legacyState != null) {
            legacyState.clear();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        log.info("Initialize state");
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return;
        }

        ListStateDescriptor<byte[]> descriptor =
                new ListStateDescriptor<>(
                        "starrocks-sink-transaction",
                        TypeInformation.of(new TypeHint<byte[]>() {})
                );

        ListState<byte[]> listState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);
        snapshotStates = new SimpleVersionedListState<>(listState, new StarRocksVersionedSerializer(getOrCreateJsonWrapper()));

        // old version
        ListStateDescriptor<Map<String, StarRocksSinkBufferEntity>> legacyDescriptor =
                new ListStateDescriptor<>(
                        "buffered-rows",
                        TypeInformation.of(new TypeHint<Map<String, StarRocksSinkBufferEntity>>(){})
                );
        legacyState = functionInitializationContext.getOperatorStateStore().getListState(legacyDescriptor);
        this.restoredCheckpointId = 0;
        this.restoredGeneratorSnapshots = new ArrayList<>();
        this.snapshotMap = new ConcurrentHashMap<>();
        if (functionInitializationContext.isRestored()) {
            for (StarrocksSnapshotState state : snapshotStates.get()) {
                for (Map.Entry<Long, List<StreamLoadSnapshot>> entry : state.getData().entrySet()) {
                    snapshotMap.compute(entry.getKey(), (k, v) -> {
                        if (v == null) {
                            return new ArrayList<>(entry.getValue());
                        }
                        v.addAll(entry.getValue());
                        return v;
                    });
                }

                if (state.getLabelSnapshots() != null) {
                    List<ExactlyOnceLabelGeneratorSnapshot> labelSnapshots = state.getLabelSnapshots();
                    restoredGeneratorSnapshots.addAll(labelSnapshots);
                    long checkpointId = labelSnapshots.isEmpty() ? -1 : labelSnapshots.get(0).getCheckpointId();
                    restoredCheckpointId = Math.max(restoredCheckpointId, checkpointId);
                }
            }

            legacyData = new ArrayList<>();
            for (Map<String, StarRocksSinkBufferEntity> entry : legacyState.get()) {
                legacyData.addAll(entry.values());
            }
            log.info("There are {} items from legacy state", legacyData.size());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (sinkOptions.getSemantic() != StarRocksSinkSemantic.EXACTLY_ONCE) {
            return;
        }

        boolean succeed = true;
        List<Long> commitCheckpointIds = snapshotMap.keySet().stream()
                .filter(cpId -> cpId <= checkpointId)
                .sorted(Long::compare)
                .collect(Collectors.toList());

        for (Long cpId : commitCheckpointIds) {
            try {
                for (StreamLoadSnapshot snapshot : snapshotMap.get(cpId)) {
                    if (!sinkManager.commit(snapshot)) {
                        succeed = false;
                        break;
                    }
                }

                if (!succeed) {
                    throw new RuntimeException(String.format("Failed to commit some transactions for snapshot %s, " +
                            "please check taskmanager logs for details", cpId));
                }
            } catch (Exception e) {
                log.error("Failed to notify checkpoint complete, checkpoint id : {}", checkpointId, e);
                throw new RuntimeException("Failed to notify checkpoint complete for checkpoint id " + checkpointId, e);
            }

            snapshotMap.remove(cpId);
        }

        // set legacyState to null to avoid clear it in latter snapshotState
        legacyState = null;
    }

}