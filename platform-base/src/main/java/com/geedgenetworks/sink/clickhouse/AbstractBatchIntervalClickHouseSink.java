package com.geedgenetworks.sink.clickhouse;

import com.geedgenetworks.sink.clickhouse.jdbc.BytesCharVarSeq;
import com.geedgenetworks.sink.clickhouse.jdbc.ClickHouseBatchInsertConnection;
import com.geedgenetworks.sink.clickhouse.jdbc.ClickHousePreparedBatchInsertStatement;
import com.geedgenetworks.sink.clickhouse.jdbc.DataTypeStringV2;
import com.github.housepower.data.*;
import com.github.housepower.data.type.*;
import com.github.housepower.data.type.complex.*;
import com.github.housepower.exception.ClickHouseSQLException;
import com.github.housepower.jdbc.ClickHouseArray;
import com.github.housepower.jdbc.ClickHouseStruct;
import com.github.housepower.misc.BytesCharSeq;
import com.github.housepower.misc.DateTimeUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.runtime.util.StringUtf8Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.housepower.misc.ExceptionUtil.unchecked;


public abstract class AbstractBatchIntervalClickHouseSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    static final Logger LOG = LoggerFactory.getLogger(AbstractBatchIntervalClickHouseSink.class);
    public static long UINT32_MAX = (1L << 32) -1;
    // 标准日期时间格式，精确到秒：yyyy-MM-dd HH:mm:ss
    public static final String NORM_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter NORM_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(NORM_DATETIME_PATTERN);
    // 标准日期时间格式，精确到毫秒：yyyy-MM-dd HH:mm:ss.SSS
    public static final String NORM_DATETIME_MS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final DateTimeFormatter NORM_DATETIME_MS_FORMATTER = DateTimeFormatter.ofPattern(NORM_DATETIME_MS_PATTERN);
    private int batchSize;
    private long batchIntervalMs;
    transient private boolean closed;
    transient private Thread outThread;
    transient private ReentrantLock lock;
    transient private Block batch;
    transient private BlockingQueue<Block> outBatchQueue;
    transient private BlockingQueue<Block> freeBatchQueue;
    transient private Exception flushException;
    transient private long lastFlushTs;
    // flush ck 相关
    private final String[] urls;
    private int urlIndex;
    private final Properties connInfo;
    private final String table;
    private String insertSql;
    protected ZoneId tz;
    protected String[] columnNames;
    protected Object[] columnDefaultValues;
    protected IDataType<?, ?>[] columnTypes;
    protected ValueConverter[] columnConverters;

    public AbstractBatchIntervalClickHouseSink(int batchSize, long batchIntervalMs, String host, String table, Properties connInfo) {
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.urls = ClickHouseUtils.buildUrlsFromHost(host);
        this.table = table;
        this.connInfo = connInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lock = new ReentrantLock();
        outBatchQueue = new LinkedBlockingQueue<>(1);
        freeBatchQueue = new LinkedBlockingQueue<>(2);
        initClickHouseParams();
        onInit(parameters);
        lastFlushTs = System.currentTimeMillis();
        outThread = new Thread(() -> {
            while (!closed) {
                try {
                    Block list;
                    try {
                        list = outBatchQueue.poll(2, TimeUnit.SECONDS);
                        if (list == null) {
                            if (System.currentTimeMillis() - lastFlushTs >= batchIntervalMs) {
                                flush();
                            }
                            continue;
                        }
                    } catch (InterruptedException e) {
                        continue;
                    }

                    onFlush(list);
                } catch (Exception e) {
                    flushException = e;
                }
            }
        }, "BatchIntervalSink-" + (getRuntimeContext().getIndexOfThisSubtask() + 1) + "/" + getRuntimeContext().getNumberOfParallelSubtasks());
        outThread.start();
    }

    private void initClickHouseParams() throws Exception {
        //urlIndex = new Random().nextInt(urls.length);
        urlIndex = getRuntimeContext().getIndexOfThisSubtask() % urls.length;

        // 获取要插入的列信息
        Tuple2<String[], Object[]> columnsAndDefaultValues = ClickHouseUtils.getInsertColumnsAndDefaultValuesForTable(urls, urlIndex, connInfo, table);
        columnNames = columnsAndDefaultValues.f0;
        columnDefaultValues = columnsAndDefaultValues.f1;
        insertSql = ClickHouseUtils.genePreparedInsertSql(this.table, this.columnNames);

        LOG.warn("insertColumnsCount:" + columnNames.length);
        LOG.warn("insertColumns:" + String.join(",", columnNames));
        LOG.warn("insertColumnDefaultValues:" + IntStream.range(0, columnNames.length).mapToObj(i -> columnNames[i] + ":" + columnDefaultValues[i]).collect(Collectors.joining(",")));
        LOG.warn("insertSql:" + insertSql);

        // 获取时区用于解析DateTime类型
        tz =  ClickHouseUtils.chooseTimeZone(urls, urlIndex, connInfo);

        // 初始化Block
        Block block = ClickHouseUtils.getInsertBlockForSql(urls, urlIndex, connInfo, insertSql);
        assert block.columnCnt() == columnNames.length;
        block.initWriteBuffer();
        freeBatchQueue.put(block);

        Field typeField = AbstractColumn.class.getDeclaredField("type");
        typeField.setAccessible(true);
        columnTypes = new IDataType<?, ?>[block.columnCnt()];
        for (int i = 0; i < columnNames.length; i++) {
            IColumn column = block.getColumn(i);
            columnTypes[i] = column.type();
            assert columnNames[i] == column.name();

            if(column.type() instanceof DataTypeString || column.type() instanceof DataTypeFixedString){
                if(columnDefaultValues[i].equals("")){
                    columnDefaultValues[i] = ClickHouseUtils.EMPTY_BYTES_CHAR_SEQ;
                }else{
                    columnDefaultValues[i] = new BytesCharSeq(StringUtf8Utils.encodeUTF8(columnDefaultValues[i].toString()));
                }
            }

            if(column.type() instanceof DataTypeString && column instanceof Column){
                DataTypeStringV2 dataTypeStringV2 = new DataTypeStringV2(StandardCharsets.UTF_8);
                typeField.set(column, dataTypeStringV2);
                columnTypes[i] = dataTypeStringV2;
            }
        }

        columnConverters = Arrays.stream(columnTypes).map(this::makeConverter).toArray(ValueConverter[]::new);

        // 从block复制block
        block = ClickHouseUtils.newInsertBlockFrom(block);
        block.initWriteBuffer();
        freeBatchQueue.put(block);
        batch = freeBatchQueue.take();
    }

    public final void checkFlushException() throws Exception {
        if (flushException != null)
            throw flushException;
    }

    void onInit(Configuration parameters) throws Exception {

    }

    abstract boolean addBatch(Block batch, T data) throws Exception;

    @Override
    public final void invoke(T value, Context context) throws Exception {
        checkFlushException();
        lock.lock();
        try {
            if(addBatch(batch, value)){
                batch.appendRow();
            }
            if (batchSize > 0 && batch.rowCnt() >= batchSize) {
                //LOG.warn("flush");
                flush();
            }
        } finally {
            lock.unlock();
        }
    }

    public final void flush() throws Exception {
        checkFlushException();
        lock.lock();
        try {
            if (batch.rowCnt() <= 0) {
                return;
            }
            outBatchQueue.put(batch);
            batch = freeBatchQueue.take();
        } finally {
            lock.unlock();
        }
    }

    private void onFlush(Block block) throws Exception {
        long start = System.currentTimeMillis();
        int rowCnt = block.rowCnt();
        ClickHouseUtils.showBlockColumnsByteSize(block); // 仅用于测试
        LOG.warn("flush " + rowCnt + " start:" + new java.sql.Timestamp(start) + "," + (start - lastFlushTs));
        lastFlushTs = System.currentTimeMillis();

        int retryCount = 0;
        while (true) {
            retryCount++;

            String url = urls[urlIndex];
            urlIndex++;
            if (urlIndex == urls.length) {
                urlIndex = 0;
            }

            ClickHouseBatchInsertConnection connection = null;
            ClickHousePreparedBatchInsertStatement stmt = null;
            try {
                connection = ClickHouseBatchInsertConnection.connect(url, connInfo);
                stmt = connection.prepareStatement(insertSql);
                ClickHouseUtils.copyInsertBlockColumns(block, stmt.getBlock());
                stmt.executeBatch();

                ClickHouseUtils.resetInsertBlockColumns(block);
                freeBatchQueue.put(block);

                LOG.warn("flush " + rowCnt + " end:" + new java.sql.Timestamp(System.currentTimeMillis()) + "," + (System.currentTimeMillis() - start));

                return;
            } catch (SQLException e) {
                LOG.error("ClickHouseBatchInsertFail url:" + url, e);
                if (retryCount >= 3) {
                    //throw e;
                    return;
                }
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public final void close() throws Exception {
        if (!closed) {
            closed = true;

            if (outThread != null) {
                outThread.join();
            }

            // init中可能抛出异常
            if (lock != null) {
                lock.lock();
                try {
                    if (batch.rowCnt() > 0) {
                        onFlush(batch);
                    }
                } catch (Exception e) {
                    flushException = e;
                } finally {
                    lock.unlock();
                }
            }

        }

        checkFlushException();
    }

    protected ValueConverter makeConverter(IDataType<?, ?> type) {
        // put the most common cast at first to avoid `instanceof` test overhead
        if (type instanceof DataTypeString || type instanceof DataTypeFixedString) {
            return this::convertString;
        }

        if (type instanceof DataTypeStringV2) {
            return makeStringV2Converter();
        }

        if (type instanceof DataTypeDate) {
            return this::convertDate;
        }

        if (type instanceof DataTypeDate32) {
            return this::convertDate32;
        }

        if (type instanceof DataTypeDateTime) {
            return this::convertDateTime;
        }

        if (type instanceof DataTypeDateTime64) {
            return this::convertDateTime64;
        }

        if (type instanceof DataTypeInt8) {
            return this::convertInt8;
        }

        if (type instanceof DataTypeUInt8 || type instanceof DataTypeInt16) {
            return this::convertInt16;
        }

        if (type instanceof DataTypeUInt16 || type instanceof DataTypeInt32) {
            return this::convertInt32;
        }

        if (type instanceof DataTypeUInt32 || type instanceof DataTypeInt64) {
            return this::convertInt64;
        }

        if (type instanceof DataTypeUInt64) {
            return this::convertUInt64;
        }

        if (type instanceof DataTypeFloat32) {
            return this::convertFloat32;
        }

        if (type instanceof DataTypeFloat64) {
            return this::convertFloat64;
        }

        if (type instanceof DataTypeDecimal) {
            return this::convertDecimal;
        }

        if (type instanceof DataTypeUUID) {
            return this::convertUUID;
        }

        if (type instanceof DataTypeNothing) {
            return this::convertNothing;
        }

        if (type instanceof DataTypeNullable) {
            IDataType nestedDataType = ((DataTypeNullable) type).getNestedDataType();
            ValueConverter converter = this.makeConverter(nestedDataType);
            return obj -> {
                if (obj == null) {
                    return null;
                }
                return converter.convert(obj);
            };
        }

        if (type instanceof DataTypeArray) {
            return this::convertArray;
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    // TODO we actually need a type cast system rather than put all type cast stuffs here
    protected final Object convertToCkDataType(IDataType<?, ?> type, Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            if (type.nullable() || type instanceof DataTypeNothing)
                return null;
            throw new ClickHouseSQLException(-1, "type[" + type.name() + "] doesn't support null value");
        }
        // put the most common cast at first to avoid `instanceof` test overhead
        if (type instanceof DataTypeString || type instanceof DataTypeFixedString) {
            if (obj instanceof CharSequence)
                return obj;
            if (obj instanceof byte[])
                return new BytesCharSeq((byte[]) obj);
            String objStr = obj.toString();
            LOG.debug("set value[{}]: {} on String Column", obj.getClass(), obj);
            return objStr;
        }
        if (type instanceof DataTypeDate) {
            if (obj instanceof java.util.Date)
                return ((Date) obj).toLocalDate();
            if (obj instanceof LocalDate)
                return obj;
        }

        if (type instanceof DataTypeDate32) {
            if (obj instanceof java.util.Date)
                return ((Date) obj).toLocalDate();
            if (obj instanceof LocalDate)
                return obj;
        }
        // TODO support
        //   1. other Java8 time, i.e. OffsetDateTime, Instant
        //   2. unix timestamp, but in second or millisecond?
        if (type instanceof DataTypeDateTime || type instanceof DataTypeDateTime64) {
            if (obj instanceof Number){
                long ts = ((Number) obj).longValue();
                // 小于UINT32_MAX认为单位是s
                if(ts < UINT32_MAX){
                    ts  = ts * 1000;
                }
                Instant instant = Instant.ofEpochMilli(ts);
                return LocalDateTime.ofInstant(instant, tz).atZone(tz);
            }

            if (obj instanceof Timestamp)
                return DateTimeUtil.toZonedDateTime((Timestamp) obj, tz);
            if (obj instanceof LocalDateTime)
                return ((LocalDateTime) obj).atZone(tz);
            if (obj instanceof ZonedDateTime)
                return obj;
            if (obj instanceof String){
                String str = (String) obj;
                if(str.length() == 19){
                    return LocalDateTime.parse(str, NORM_DATETIME_FORMATTER).atZone(tz);
                }else if(str.length() == 23){
                    return LocalDateTime.parse(str, NORM_DATETIME_MS_FORMATTER).atZone(tz);
                }
            }
            throw new ClickHouseSQLException(-1, "require ClickHouseStruct for column: " + type.name() + ", but found " + obj.getClass());
        }
        if (type instanceof DataTypeInt8) {
            if (obj instanceof Number)
                return ((Number) obj).byteValue();
            if (obj instanceof String)
                return (byte) Integer.parseInt((String) obj);
        }
        if (type instanceof DataTypeUInt8 || type instanceof DataTypeInt16) {
            if (obj instanceof Number)
                return ((Number) obj).shortValue();
            if (obj instanceof String)
                return (short) Integer.parseInt((String) obj);
        }
        if (type instanceof DataTypeUInt16 || type instanceof DataTypeInt32) {
            if (obj instanceof Number)
                return ((Number) obj).intValue();
            if (obj instanceof String)
                return Integer.parseInt((String) obj);
        }
        if (type instanceof DataTypeUInt32 || type instanceof DataTypeInt64) {
            if (obj instanceof Number)
                return ((Number) obj).longValue();
            if (obj instanceof String)
                return Long.parseLong((String) obj);
        }
        if (type instanceof DataTypeUInt64) {
            if (obj instanceof BigInteger)
                return obj;
            if (obj instanceof BigDecimal)
                return ((BigDecimal) obj).toBigInteger();
            if (obj instanceof Number)
                return BigInteger.valueOf(((Number) obj).longValue());
            if (obj instanceof String)
                return BigInteger.valueOf(Long.parseLong((String) obj));
        }
        if (type instanceof DataTypeFloat32) {
            if (obj instanceof Number)
                return ((Number) obj).floatValue();
        }
        if (type instanceof DataTypeFloat64) {
            if (obj instanceof Number)
                return ((Number) obj).doubleValue();
        }
        if (type instanceof DataTypeDecimal) {
            if (obj instanceof BigDecimal)
                return obj;
            if (obj instanceof BigInteger)
                return new BigDecimal((BigInteger) obj);
            if (obj instanceof Number)
                return ((Number) obj).doubleValue();
        }
        if (type instanceof DataTypeUUID) {
            if (obj instanceof UUID)
                return obj;
            if (obj instanceof String) {
                return UUID.fromString((String) obj);
            }
        }
        if (type instanceof DataTypeNothing) {
            return null;
        }
        if (type instanceof DataTypeNullable) {
            // handled null at first, so obj also not null here
            return convertToCkDataType(((DataTypeNullable) type).getNestedDataType(), obj);
        }
        if (type instanceof DataTypeArray) {
            if (!(obj instanceof ClickHouseArray)) {
                throw new ClickHouseSQLException(-1, "require ClickHouseArray for column: " + type.name() + ", but found " + obj.getClass());
            }
            //return ((ClickHouseArray) obj).mapElements(unchecked(this::convertToCkDataType));
            //既然是ClickHouseArray，认为传入元素类型正确
            return obj;
        }
        if (type instanceof DataTypeTuple) {
            if (!(obj instanceof ClickHouseStruct)) {
                throw new ClickHouseSQLException(-1, "require ClickHouseStruct for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((ClickHouseStruct) obj).mapAttributes(((DataTypeTuple) type).getNestedTypes(), unchecked(this::convertToCkDataType));
        }
        if (type instanceof DataTypeMap) {
            if (obj instanceof Map) {
                // return obj;
                Map<Object, Object> result = new HashMap<Object, Object>();
                IDataType<?, ?>[] nestedTypes = ((DataTypeMap) type).getNestedTypes();
                Map<?, ?> dataMap = (Map<?, ?>) obj;
                for (Map.Entry<?, ?> entry : dataMap.entrySet()) {
                    Object key = convertToCkDataType(nestedTypes[0], entry.getKey());
                    Object value = convertToCkDataType(nestedTypes[1], entry.getValue());
                    result.put(key, value);
                }
                return result;
            } else {
                throw new ClickHouseSQLException(-1, "require Map for column: " + type.name() + ", but found " + obj.getClass());
            }
        }

        LOG.debug("unhandled type: {}[{}]", type.name(), obj.getClass());
        return obj;
    }

    private static final int MAX_STR_BYTES_LENGTH = 1024 * 64;

    private ValueConverter makeStringV2Converter() {
        return new ValueConverter() {
            final byte[] bytes = new byte[MAX_STR_BYTES_LENGTH];
            final BytesCharVarSeq bytesCharVarSeq = new BytesCharVarSeq(bytes, 0);
            @Override
            public Object convert(Object obj) throws ClickHouseSQLException {
                if (obj == null) {
                    throw new ClickHouseSQLException(-1, "type doesn't support null value");
                }
                if (obj instanceof byte[]){
                    byte[] bs = (byte[])obj;
                    bytesCharVarSeq.setBytesAndLen(bs, bs.length);
                    return bytesCharVarSeq;
                }
                if (obj instanceof CharSequence){
                    if(((CharSequence)obj).length() == 0){
                        return ClickHouseUtils.EMPTY_BYTES_CHAR_SEQ;
                    }
                }else{
                    //LOG.debug("set value[{}]: {} on String Column", obj.getClass(), obj);
                }
                String str = obj.toString();
                int length = str.length() * 3;
                byte[] bs = bytes;
                if (length > MAX_STR_BYTES_LENGTH) {
                    bs = new byte[length];
                }
                int len = StringUtf8Utils.encodeUTF8(str, bs);
                bytesCharVarSeq.setBytesAndLen(bs, len);
                return bytesCharVarSeq;
            }
        };
    }

    private Object convertString(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof CharSequence){
            if(((CharSequence)obj).length() == 0){
                return ClickHouseUtils.EMPTY_BYTES_CHAR_SEQ;
            }
            return new BytesCharSeq(StringUtf8Utils.encodeUTF8(obj.toString()));
            //return obj;
        }
        if (obj instanceof byte[])
            return new BytesCharSeq((byte[]) obj);
        String objStr = obj.toString();
        //LOG.debug("set value[{}]: {} on String Column", obj.getClass(), obj);
        return new BytesCharSeq(StringUtf8Utils.encodeUTF8(objStr));
    }

    private Object convertDate(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof java.util.Date)
            return ((Date) obj).toLocalDate();
        if (obj instanceof LocalDate)
            return obj;

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertDate32(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof java.util.Date)
            return ((Date) obj).toLocalDate();
        if (obj instanceof LocalDate)
            return obj;

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertDateTime(Object obj) throws ClickHouseSQLException {
        if (obj instanceof Number){
            long ts = ((Number) obj).longValue();
            // 小于UINT32_MAX认为单位是s
            if(ts < UINT32_MAX){
                ts  = ts * 1000;
            }
            Instant instant = Instant.ofEpochMilli(ts);
            return LocalDateTime.ofInstant(instant, tz).atZone(tz);
        }

        if (obj instanceof Timestamp)
            return DateTimeUtil.toZonedDateTime((Timestamp) obj, tz);
        if (obj instanceof LocalDateTime)
            return ((LocalDateTime) obj).atZone(tz);
        if (obj instanceof ZonedDateTime)
            return obj;
        if (obj instanceof String){
            String str = (String) obj;
            if(str.length() == 19){
                return LocalDateTime.parse(str, NORM_DATETIME_FORMATTER).atZone(tz);
            }else if(str.length() == 23){
                return LocalDateTime.parse(str, NORM_DATETIME_MS_FORMATTER).atZone(tz);
            }
        }

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertDateTime64(Object obj) throws ClickHouseSQLException {
        if (obj instanceof Number){
            long ts = ((Number) obj).longValue();
            // 小于UINT32_MAX认为单位是s
            if(ts < UINT32_MAX){
                ts  = ts * 1000;
            }
            Instant instant = Instant.ofEpochMilli(ts);
            return LocalDateTime.ofInstant(instant, tz).atZone(tz);
        }

        if (obj instanceof Timestamp)
            return DateTimeUtil.toZonedDateTime((Timestamp) obj, tz);
        if (obj instanceof LocalDateTime)
            return ((LocalDateTime) obj).atZone(tz);
        if (obj instanceof ZonedDateTime)
            return obj;
        if (obj instanceof String){
            String str = (String) obj;
            if(str.length() == 19){
                return LocalDateTime.parse(str, NORM_DATETIME_FORMATTER).atZone(tz);
            }else if(str.length() == 23){
                return LocalDateTime.parse(str, NORM_DATETIME_MS_FORMATTER).atZone(tz);
            }
        }

        throw new ClickHouseSQLException(-1, "unhandled type: {}" + obj.getClass());
    }

    private Object convertInt8(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number)
            return ((Number) obj).byteValue();
        if (obj instanceof String)
            return (byte) Integer.parseInt((String) obj);

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertInt16(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number)
            return ((Number) obj).shortValue();
        if (obj instanceof String)
            return (short) Integer.parseInt((String) obj);

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertInt32(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number)
            return ((Number) obj).intValue();
        if (obj instanceof String)
            return Integer.parseInt((String) obj);

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertInt64(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number)
            return ((Number) obj).longValue();
        if (obj instanceof String)
            return Long.parseLong((String) obj);

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertUInt64(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof BigInteger)
            return obj;
        if (obj instanceof BigDecimal)
            return ((BigDecimal) obj).toBigInteger();
        if (obj instanceof Number)
            return BigInteger.valueOf(((Number) obj).longValue());
        if (obj instanceof String)
            return BigInteger.valueOf(Long.parseLong((String) obj));

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertFloat32(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number)
            return ((Number) obj).floatValue();
        if (obj instanceof String)
            return Float.parseFloat((String) obj);

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertFloat64(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof Number)
            return ((Number) obj).doubleValue();
        if (obj instanceof String)
            return Double.parseDouble((String) obj);

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertDecimal(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof BigDecimal)
            return obj;
        if (obj instanceof BigInteger)
            return new BigDecimal((BigInteger) obj);
        if (obj instanceof Number)
            return new BigDecimal(((Number) obj).doubleValue());
        if (obj instanceof String)
            return new BigDecimal(Double.parseDouble((String) obj));

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertUUID(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (obj instanceof UUID)
            return obj;
        if (obj instanceof String) {
            return UUID.fromString((String) obj);
        }

        LOG.debug("unhandled type: {}", obj.getClass());
        return obj;
    }

    private Object convertNothing(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        return null;
    }

    private Object convertArray(Object obj) throws ClickHouseSQLException {
        if (obj == null) {
            throw new ClickHouseSQLException(-1, "type doesn't support null value");
        }
        if (!(obj instanceof ClickHouseArray)) {
            throw new ClickHouseSQLException(-1, "require ClickHouseArray for column, but found " + obj.getClass());
        }
        //return ((ClickHouseArray) obj).mapElements(unchecked(this::convertToCkDataType));
        //既然是ClickHouseArray，认为传入元素类型正确
        return obj;
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(Object obj) throws ClickHouseSQLException;
    }
}
