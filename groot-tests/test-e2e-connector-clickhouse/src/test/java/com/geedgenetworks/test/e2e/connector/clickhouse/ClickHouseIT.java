package com.geedgenetworks.test.e2e.connector.clickhouse;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.test.e2e.common.TestResource;
import com.geedgenetworks.test.e2e.common.TestSuiteBase;
import com.geedgenetworks.test.e2e.common.container.ContainerUtil;
import com.geedgenetworks.test.e2e.common.container.TestContainer;
import com.geedgenetworks.test.e2e.common.container.TestContainerId;
import com.geedgenetworks.test.e2e.common.junit.DisabledOnContainer;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.FLINK_1_17},
        disabledReason = "Override TestSuiteBase @DisabledOnContainer")
public class ClickHouseIT extends TestSuiteBase implements TestResource {
    private static final String CLICKHOUSE_DOCKER_IMAGE = "clickhouse/clickhouse-server:23.3.19.32";
    private static final String DRIVER_CLASS = "com.clickhouse.jdbc.ClickHouseDriver";
    private static final String INIT_CLICKHOUSE_PATH = "/init/clickhouse_test_sql.conf";
    private static final String DATABASE = "default";
    private static final String SOURCE_TABLE = "source_table";
    private static final String SINK_TABLE = "sink_table";
    private static final String INSERT_SQL = "insert_sql";
    private static final String COMPARE_SQL = "compare_sql";
    private static final String HOST = "clickhouse";
    private static final Config CONFIG = getInitClickhouseConfig();
    private ClickHouseContainer clickHouseContainer;
    private Connection connection;
    private static final String[] default_columns = new String[] {
            "id",
            "c_array_string",
            "c_array_short",
            "c_array_int",
            "c_array_long",
            "c_array_float",
            "c_array_double",
            "c_string",
            "c_int8",
            "c_int16",
            "c_int32",
            "c_int64",
            "c_float32",
            "c_float64",
            "c_decimal",
            "c_nullable",
            "c_lowcardinality"
    };
    private static final List<Map<String, Object>>TEST_DATASET = generateTestDataSet();


    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.clickHouseContainer =
                new ClickHouseContainer(CLICKHOUSE_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withCopyFileToContainer(MountableFile.forClasspathResource("init/users.xml"), "/etc/clickhouse-server/users.xml")
                        .withCopyFileToContainer(MountableFile.forClasspathResource("init/init-clickhouse.sql"), "/docker-entrypoint-initdb.d/init-clickhouse.sql")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CLICKHOUSE_DOCKER_IMAGE)));

        Startables.deepStart(Stream.of(this.clickHouseContainer)).join();
        System.out.println("Clickhouse JDBC URL: " + this.clickHouseContainer.getJdbcUrl());
        System.out.println("Clickhouse username: " + this.clickHouseContainer.getUsername());
        System.out.println("Clickhouse password: " + this.clickHouseContainer.getPassword());

        log.info("Clickhouse container started");
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(360L, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
        this.initializeClickhouseTable();
        this.batchInsertData();
        log.info(JSON.toJSONString(TEST_DATASET));

    }

    private void initConnection()
            throws SQLException, ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        final Properties info = new Properties();
        info.put("user", this.clickHouseContainer.getUsername());
        info.put("password",  this.clickHouseContainer.getPassword());
        this.connection =
                ((Driver) Class.forName(DRIVER_CLASS).newInstance())
                        .connect(this.clickHouseContainer.getJdbcUrl(), info);

    }

    @TestTemplate
    public void testClickHouse(TestContainer container) throws Exception {
        assertHasData(SOURCE_TABLE);
    }

    @TestTemplate
    public void testClickHouseDataTypeSinkTable(TestContainer container) throws Exception {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        Container.ExecResult execResult = container.executeJob("/clickhouse_data_type_sink.yaml");
                        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
                        return execResult;
                    } catch (Exception e) {
                        log.error("Commit task exception:" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        await().atMost(300000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            assertHasData(SINK_TABLE);
                            compareResult();
                        });
    }


    private void assertHasData(String table) {
        String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
        try (Statement statement = connection.createStatement();
                ResultSet source = statement.executeQuery(sql);) {
            Assertions.assertTrue(source.next());
        } catch (SQLException e) {
            throw new RuntimeException("test clickhouse server image error", e);
        }
    }

    private void clearTable(String table) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("truncate table %s.%s", DATABASE, table));
        } catch (SQLException e) {
            throw new RuntimeException("Test clickhouse server image error", e);
        }
    }

    private void compareResult() throws SQLException, IOException {
        String sourceSql = "select * from " + SOURCE_TABLE + " order by id ";
        String sinkSql = "select * from " + SINK_TABLE + " order by id";
        try (Statement sourceStatement = connection.createStatement();
             Statement sinkStatement = connection.createStatement();
             ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
             ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql)) {
            Assertions.assertEquals(
                    sourceResultSet.getMetaData().getColumnCount(),
                    sinkResultSet.getMetaData().getColumnCount());
            String columns = String.join(",", default_columns);
            Assertions.assertTrue(
                    compare(String.format(CONFIG.getString(COMPARE_SQL), columns, columns)));
        }

        sourceSql = "select count(distinct id) as count from " + SOURCE_TABLE;
        sinkSql = "select count(distinct id) as count  from " + SINK_TABLE;

        try (Statement sourceStatement = connection.createStatement();
             Statement sinkStatement = connection.createStatement();
             ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
             ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql)) {
            while (sourceResultSet.next()) {
                if (sinkResultSet.next()) {
                   int sinkUniqueIds = sinkResultSet.getInt("count");
                   int sourceUniqueIds = sourceResultSet.getInt("count");
                   Assertions.assertEquals(sinkUniqueIds, sourceUniqueIds);
                }
            }
        }


    }

    private Boolean compare(String sql) {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);) {
            return !resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException("result compare error", e);
        }
    }


    private void batchInsertData() {
        String sql = CONFIG.getString(INSERT_SQL);
        PreparedStatement preparedStatement = null;
        try {
            this.connection.setAutoCommit(true);
            preparedStatement = this.connection.prepareStatement(sql);
            for (Map<String, Object> row : TEST_DATASET) {
                preparedStatement.setLong(1, (Long) row.get(default_columns[0]));
                preparedStatement.setArray(2, toSqlArray(row.get(default_columns[1])));
                preparedStatement.setArray(3, toSqlArray(row.get(default_columns[2])));
                preparedStatement.setArray(4, toSqlArray(row.get(default_columns[3])));
                preparedStatement.setArray(5, toSqlArray(row.get(default_columns[4])));
                preparedStatement.setArray(6, toSqlArray(row.get(default_columns[5])));
                preparedStatement.setArray(7, toSqlArray(row.get(default_columns[6])));
                preparedStatement.setString(8, (String) row.get(default_columns[7]));
                preparedStatement.setByte(9, (Byte) row.get(default_columns[8]));
                preparedStatement.setShort(10, (Short) row.get(default_columns[9]));
                preparedStatement.setInt(11, (Integer) row.get(default_columns[10]));
                preparedStatement.setLong(12, (Long) row.get(default_columns[11]));
                preparedStatement.setFloat(13, (Float) row.get(default_columns[12]));
                preparedStatement.setDouble(14, (Double) row.get(default_columns[13]));
                preparedStatement.setBigDecimal(15, (BigDecimal) row.get(default_columns[14]));
                preparedStatement.setInt(16, (Integer) row.get(default_columns[15]));
                preparedStatement.setString(17, (String) row.get(default_columns[16]));
                preparedStatement.addBatch();
            }

            preparedStatement.executeBatch();
            preparedStatement.clearBatch();

        } catch (SQLException e) {
            throw new RuntimeException("Batch insert data failed!", e);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException("PreparedStatement close failed!", e);
                }
            }
        }



    }


    private Array toSqlArray(Object value) throws SQLException {
        Object[] elements = null;
        String sqlType = null;
        if (String[].class.equals(value.getClass())) {
            sqlType = "TEXT";
            elements = (String[]) value;
        } else if (Boolean[].class.equals(value.getClass())) {
            sqlType = "BOOLEAN";
            elements = (Boolean[]) value;
        } else if (Byte[].class.equals(value.getClass())) {
            sqlType = "TINYINT";
            elements = (Byte[]) value;
        } else if (Short[].class.equals(value.getClass())) {
            sqlType = "SMALLINT";
            elements = (Short[]) value;
        } else if (Integer[].class.equals(value.getClass())) {
            sqlType = "INTEGER";
            elements = (Integer[]) value;
        } else if (Long[].class.equals(value.getClass())) {
            sqlType = "BIGINT";
            elements = (Long[]) value;
        } else if (Float[].class.equals(value.getClass())) {
            sqlType = "REAL";
            elements = (Float[]) value;
        } else if (Double[].class.equals(value.getClass())) {
            sqlType = "DOUBLE";
            elements = (Double[]) value;
        }
        if (sqlType == null) {
            throw new IllegalArgumentException(
                    "array inject error, not supported data type: " + value.getClass());
        }
        return connection.createArrayOf(sqlType, elements);
    }

    private static List<Map<String, Object>> generateTestDataSet() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            Map<String, Object> row = Maps.newLinkedHashMap();
            row.put(default_columns[0], (long) i);
            row.put(default_columns[1], new String[] {"string"});
            row.put(default_columns[2], new Short[] {Short.parseShort("1")});
            row.put(default_columns[3], new Integer[] {Integer.parseInt("1")});
            row.put(default_columns[4], new Long[] {Long.parseLong("1")});
            row.put(default_columns[5], new Float[] {Float.parseFloat("1.1")});
            row.put(default_columns[6], new Double[] {Double.parseDouble("1.1")});
            row.put(default_columns[7], "string");
            row.put(default_columns[8], Byte.parseByte("1"));
            row.put(default_columns[9], Short.parseShort("1"));
            row.put(default_columns[10], Integer.parseInt("1"));
            row.put(default_columns[11], Long.parseLong("1"));
            row.put(default_columns[12], Float.parseFloat("1.1"));
            row.put(default_columns[13], Double.parseDouble("1.1"));
            row.put(default_columns[14], BigDecimal.valueOf(11L, 1));
            row.put(default_columns[15], i);
            row.put(default_columns[16], "string");
            rows.add(row);
        }
        return rows;
    }



    private void initializeClickhouseTable() {
        try {
            Statement statement = this.connection.createStatement();
            statement.execute(CONFIG.getString(SOURCE_TABLE));
            statement.execute(CONFIG.getString(SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("Initializing Clickhouse table failed!", e);
        }
    }

    private static Config getInitClickhouseConfig() {
        File file = ContainerUtil.getResourcesFile(INIT_CLICKHOUSE_PATH);
        Config config = ConfigFactory.parseFile(file);
        assert config.hasPath(SOURCE_TABLE)
                && config.hasPath(SINK_TABLE)
                && config.hasPath(INSERT_SQL)
                && config.hasPath(COMPARE_SQL);
        return config;
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (this.connection != null) {
            this.connection.close();
        }
        if (this.clickHouseContainer != null) {
            this.clickHouseContainer.stop();
        }

    }
}
