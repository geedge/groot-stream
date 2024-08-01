package com.geedgenetworks.test.e2e.clickhouse;

import com.geedgenetworks.test.common.TestResource;
import com.geedgenetworks.test.common.TestSuiteBase;
import com.geedgenetworks.test.common.container.ContainerUtil;
import com.geedgenetworks.test.common.container.TestContainer;
import com.geedgenetworks.test.common.container.TestContainerId;
import com.geedgenetworks.test.common.junit.DisabledOnContainer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {TestContainerId.FLINK_1_17},
        disabledReason = "Override TestSuiteBase @DisabledOnContainer")
public class ClickHouseIT extends TestSuiteBase implements TestResource {
    private static final String CLICKHOUSE_DOCKER_IMAGE = "clickhouse/clickhouse-server:23.3.19.32";
    private static final String DRIVER_CLASS = "com.clickhouse.jdbc.ClickHouseDriver";
    private static final String INIT_CLICKHOUSE_PATH = "/init/clickhouse_init.conf";
    private static final String DATABASE = "default";
    private static final String SOURCE_TABLE = "source_table";
    private static final String SINK_TABLE = "sink_table";
    private static final String INSERT_SQL = "insert_sql";
    private static final String COMPARE_SQL = "compare_sql";
    private static final String HOST = "clickhouse";
    private static final Config CONFIG = getInitClickhouseConfig();
    private ClickHouseContainer clickHouseContainer;
    private Connection connection;
    private static final List<Object[]>TEST_DATASET =
            generateTestDataSet();
    private static final String[] default_columns = new String[] {
                "id",
                "c_map",
                "c_array_string",
                "c_array_short",
                "c_array_int",
                "c_array_long",
                "c_array_float",
                "c_array_double",
                "c_string",
                "c_boolean",
                "c_int8",
                "c_int16",
                "c_int32",
                "c_int64",
                "c_float32",
                "c_float64",
                "c_decimal",
                "c_date",
                "c_datetime",
                "c_nullable",
                "c_lowcardinality",
                "c_nested.int",
                "c_nested.double",
                "c_nested.string",
                "c_int128",
                "c_uint128",
                "c_int256",
                "c_uint256",
                "c_point",
                "c_ring"
    };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.clickHouseContainer =
                new ClickHouseContainer(CLICKHOUSE_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CLICKHOUSE_DOCKER_IMAGE)));
        Startables.deepStart(Stream.of(this.clickHouseContainer)).join();
        log.info("Clickhouse container started");
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(360L, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
        this.initializeClickhouseTable();
        this.batchInsertData();

    }


    @TestTemplate
    public void testClickhouse(TestContainer container) throws Exception {
        assertHasData(SOURCE_TABLE);

        //assertHasData(SINK_TABLE);
        //compareResult();
        //clearSinkTable();

    }

    private void assertHasData(String table) {
        try (Statement statement = connection.createStatement()) {
            String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (SQLException e) {
            throw new RuntimeException("test clickhouse server image error", e);
        }
    }

    private void clearSinkTable() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("truncate table %s.%s", DATABASE, SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("Test clickhouse server image error", e);
        }
    }

    private void compareResult() throws SQLException, IOException {
        String sourceSql = "select * from " + SOURCE_TABLE + " order by id";
        String sinkSql = "select * from " + SINK_TABLE + " order by id";
        List<String> columnList =
                Arrays.stream(default_columns).collect(Collectors.toList());
        Statement sourceStatement = connection.createStatement();
        Statement sinkStatement = connection.createStatement();
        ResultSet sourceResultSet = sourceStatement.executeQuery(sourceSql);
        ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
        Assertions.assertEquals(
                sourceResultSet.getMetaData().getColumnCount(),
                sinkResultSet.getMetaData().getColumnCount());
        while (sourceResultSet.next()) {
            if (sinkResultSet.next()) {
                for (String column : columnList) {
                    Object source = sourceResultSet.getObject(column);
                    Object sink = sinkResultSet.getObject(column);
                    if (!Objects.deepEquals(source, sink)) {
                        InputStream sourceAsciiStream = sourceResultSet.getBinaryStream(column);
                        InputStream sinkAsciiStream = sinkResultSet.getBinaryStream(column);
                        String sourceValue =
                                IOUtils.toString(sourceAsciiStream, StandardCharsets.UTF_8);
                        String sinkValue =
                                IOUtils.toString(sinkAsciiStream, StandardCharsets.UTF_8);
                        Assertions.assertEquals(sourceValue, sinkValue);
                    }
                    Assertions.assertTrue(true);
                }
            }
        }
        String columns = String.join(",", default_columns);
        Assertions.assertTrue(
                compare(String.format(CONFIG.getString(COMPARE_SQL), columns, columns)));
    }

    private Boolean compare(String sql) {
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
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
            for (Object[] row : TEST_DATASET) {
                preparedStatement.setLong(1, (Long) row[0]);
                preparedStatement.setObject(2, row[1]);
                preparedStatement.setArray(3, toSqlArray(row[2]));
                preparedStatement.setArray(4, toSqlArray(row[3]));
                preparedStatement.setArray(5, toSqlArray(row[4]));
                preparedStatement.setArray(6, toSqlArray(row[5]));
                preparedStatement.setArray(7, toSqlArray(row[6]));
                preparedStatement.setArray(8, toSqlArray(row[7]));
                preparedStatement.setString(9, (String) row[8]);
                preparedStatement.setBoolean(10, (Boolean) row[9]);
                preparedStatement.setByte(11, (Byte) row[10]);
                preparedStatement.setShort(12, (Short) row[11]);
                preparedStatement.setInt(13, (Integer) row[12]);
                preparedStatement.setLong(14, (Long) row[13]);
                preparedStatement.setFloat(15, (Float) row[14]);
                preparedStatement.setDouble(16, (Double) row[15]);
                preparedStatement.setBigDecimal(17, (BigDecimal) row[16]);
                preparedStatement.setDate(18, Date.valueOf((LocalDate) row[17]));
                preparedStatement.setTimestamp(
                        19, Timestamp.valueOf((LocalDateTime) row[18]));
                preparedStatement.setInt(20, (Integer) row[19]);
                    preparedStatement.setString(21, (String) row[20]);
                preparedStatement.setArray(22, toSqlArray(row[21]));
                preparedStatement.setArray(23, toSqlArray(row[22]));
                preparedStatement.setArray(24, toSqlArray(row[23]));
                preparedStatement.setObject(25, row[24]);
                preparedStatement.setObject(26, row[25]);
                preparedStatement.setObject(27, row[26]);
                preparedStatement.setObject(28, row[27]);
                preparedStatement.setObject(29, row[28]);
                preparedStatement.setObject(30, row[29]);
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

    private static List<Object[]> generateTestDataSet() {
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            Object[] row = new Object[] {
                                (long) i,
                                Collections.singletonMap("key", Integer.parseInt("1")),
                                new String[] {"string"},
                                new Short[] {Short.parseShort("1")},
                                new Integer[] {Integer.parseInt("1")},
                                new Long[] {Long.parseLong("1")},
                                new Float[] {Float.parseFloat("1.1")},
                                new Double[] {Double.parseDouble("1.1")},
                                "string",
                                Boolean.FALSE,
                                Byte.parseByte("1"),
                                Short.parseShort("1"),
                                Integer.parseInt("1"),
                                Long.parseLong("1"),
                                Float.parseFloat("1.1"),
                                Double.parseDouble("1.1"),
                                BigDecimal.valueOf(11L, 1),
                                LocalDate.now(),
                                LocalDateTime.now(),
                                i,
                                "string",
                                new Integer[] {Integer.parseInt("1")},
                                new Double[] {Double.parseDouble("1.1")},
                                new String[] {"1"},
                                "170141183460469231731687303715884105727",
                                "340282366920938463463374607431768211455",
                                "57896044618658097711785492504343953926634992332820282019728792003956564819967",
                                "115792089237316195423570985008687907853269984665640564039457584007913129639935",
                                new double[] {1, 2},
                                new double[][] {{2, 3}, {4, 5}}
                            };
            rows.add(row);
        }
        return rows;
    }

    private void initConnection()
            throws SQLException, ClassNotFoundException, InstantiationException,
            IllegalAccessException {
        final Properties info = new Properties();
        info.put("user", this.clickHouseContainer.getUsername());
        info.put("password", this.clickHouseContainer.getPassword());
        this.connection =
                ((Driver) Class.forName(DRIVER_CLASS).newInstance())
                        .connect(this.clickHouseContainer.getJdbcUrl(), info);
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
