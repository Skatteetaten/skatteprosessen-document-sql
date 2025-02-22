package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.ZoneOffset;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.OracleContainer;

@Category(OracleContainer.class)
public class OracleAuditMonitorTest {

    @Rule
    public OracleContainer oracle = new OracleContainer("gvenzl/oracle-xe:18-faststart");

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
        OracleAuditMonitor monitor = new OracleAuditMonitor(dataSource, false);
        assertThat(monitor.summary(LocalDate.now(ZoneOffset.UTC))).isEmpty();
    }
}
