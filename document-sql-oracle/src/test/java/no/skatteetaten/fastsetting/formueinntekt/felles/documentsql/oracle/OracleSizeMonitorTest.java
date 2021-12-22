package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.OracleContainer;

@Category(OracleContainer.class)
public class OracleSizeMonitorTest {

    @Rule
    public OracleContainer oracle = new OracleContainer("oracle/database:18.4.0-xe-prebuilt");

    private HikariDataSource dataSource;

    @Before
    public void setUp() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(oracle.getJdbcUrl());
        hikariConfig.setUsername(oracle.getUsername());
        hikariConfig.setPassword(oracle.getPassword());
        dataSource = new HikariDataSource(hikariConfig);
    }

    @After
    public void tearDown() {
        dataSource.close();
    }

    @Test
    public void can_read_size_summary() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            assertThat(stmt.execute("CREATE TABLE FOO_RAW (DUMMY VARCHAR2(100) PRIMARY KEY)")).isFalse();
        }
        Map<String, BigInteger> summary = new OracleSizeMonitor(dataSource, 1).summary("FOO");
        assertThat(summary).containsOnlyKeys("FOO");
        assertThat(summary.get("FOO")).isPositive();
    }

    @Test
    public void can_read_size_summary_missing() throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            assertThat(stmt.execute("CREATE TABLE FOO_RAW (DUMMY VARCHAR2(100) PRIMARY KEY)")).isFalse();
        }
        Map<String, BigInteger> summary = new OracleSizeMonitor(dataSource, 1).summary("FOO", "DOES_NOT_EXIST");
        assertThat(summary).containsOnlyKeys("FOO");
        assertThat(summary.get("FOO")).isPositive();
    }
}
