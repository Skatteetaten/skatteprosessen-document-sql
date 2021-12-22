package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.OracleContainer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.util.Map;

@Category(OracleContainer.class)
public class OracleAuditMonitorTest {

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
    public void can_read_audit_log() {
        Map<String, Long> summary = new OracleAuditMonitor(dataSource).summary(LocalDate.now());
        assertThat(summary).isEmpty();
    }
}
