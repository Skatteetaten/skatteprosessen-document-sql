package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcher;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.SimpleTableResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.management.sample.SimpleSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle.OracleDispatcherFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;

@Category(OracleContainer.class)
public class SchemaManagerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaManagerTest.class);

    @Rule
    public OracleContainer oracle = new OracleContainer("oracle/database:18.4.0-xe-prebuilt");

    private HikariDataSource dataSource;

    private Map<String, SchemaContext<String>> schemas;
    private SchemaManager<String> schemaManager;

    @Before
    public void setUp() throws Exception {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(oracle.getJdbcUrl());
        hikariConfig.setUsername(oracle.getUsername());
        hikariConfig.setPassword(oracle.getPassword());
        dataSource = new HikariDataSource(hikariConfig);

        try (Connection conn = dataSource.getConnection()) {
            Liquibase liquibase = new Liquibase(SchemaManager.CHANGE_LOG,
                new ClassLoaderResourceAccessor(),
                DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(conn)));
            liquibase.update(new Contexts());
        }

        schemas = new HashMap<>();
        schemaManager = new SchemaManager<>(dataSource, schemas, OracleDispatcherFactory.ofXml());
    }

    @After
    public void tearDown() {
        dataSource.close();
    }

    @Test
    public void can_create_schema() throws Exception {
        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "sample", SimpleTableResolver.ofString()));

        JdbcDispatcher<String> dispatcher = schemaManager.create("foo");
        assertThat(schemaManager.create("foo")).isNotNull();

        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, "<sample><val>bar</val></sample>");
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION "
                + "FROM FOO "
                + "WHERE VAL = 'bar'")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("ID")).isEqualTo("X");
            assertThat(rs.getInt("REVISION")).isEqualTo(1);
            assertThat(rs.next()).isFalse();
        }

        assertThat(schemaManager.drop("foo")).isTrue();
        assertThat(schemaManager.drop("foo")).isFalse();
    }

    @Test
    public void can_create_schema_with_synonym() throws Exception {
        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "sample", SimpleTableResolver.ofString()));

        JdbcDispatcher<String> dispatcher = schemaManager.create("foo");
        assertThat(schemaManager.create("foo")).isNotNull();

        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, "<sample><val>bar</val></sample>");
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION "
                + "FROM FOO "
                + "WHERE VAL = 'bar'")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("ID")).isEqualTo("X");
            assertThat(rs.getInt("REVISION")).isEqualTo(1);
            assertThat(rs.next()).isFalse();
        }

        assertThat(schemaManager.drop("foo")).isTrue();
        assertThat(schemaManager.drop("foo")).isFalse();
    }

    @Test
    public void can_recreate_schema() throws Exception {
        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "sample", SimpleTableResolver.ofString()));

        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE FOO_RAW (DUMMY CHAR(1))");
        }

        JdbcDispatcher<String> dispatcher = schemaManager.create("foo", true, name -> { });
        dispatcher.printTo(lines -> lines.forEach(LOGGER::info));
        assertThat(schemaManager.create("foo")).isNotNull();

        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, "<sample><val>bar</val></sample>");
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION "
                + "FROM FOO "
                + "WHERE VAL = 'bar'")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("ID")).isEqualTo("X");
            assertThat(rs.getInt("REVISION")).isEqualTo(1);
            assertThat(rs.next()).isFalse();
        }

        assertThat(schemaManager.drop("foo")).isTrue();
        assertThat(schemaManager.drop("foo")).isFalse();
    }

    @Test
    public void cannot_delete_inexistant_schema() throws Exception {
        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "sample", SimpleTableResolver.ofString()));
        assertThat(schemaManager.drop("foo")).isFalse();
    }

    @Test
    public void discovers_inconsistent_type() throws Exception {
        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "sample", SimpleTableResolver.ofString()));
        assertThat(schemaManager.create("foo")).isNotNull();

        schemas.put("foo", new SchemaContext<>(Object.class, "sample", SimpleTableResolver.ofString()));
        assertThatThrownBy(() -> schemaManager.create("foo")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void discovers_inconsistent_root() throws Exception {
        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "sample", SimpleTableResolver.ofString()));
        assertThat(schemaManager.create("foo")).isNotNull();

        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "bar", SimpleTableResolver.ofString()));
        assertThatThrownBy(() -> schemaManager.create("foo")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void drops_if_inconsistent_type() throws Exception {
        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "sample", SimpleTableResolver.ofString()));
        assertThat(schemaManager.create("foo")).isNotNull();

        schemas.put("foo", new SchemaContext<>(Object.class, "sample", SimpleTableResolver.ofString()));
        assertThatThrownBy(() -> schemaManager.drop("foo")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void drops_if_inconsistent_root() throws Exception {
        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "sample", SimpleTableResolver.ofString()));
        assertThat(schemaManager.create("foo")).isNotNull();

        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "bar", SimpleTableResolver.ofString()));
        assertThatThrownBy(() -> schemaManager.drop("foo")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void does_not_recreate_schema_if_not_forced() throws Exception {
        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "sample", SimpleTableResolver.ofString()));
        assertThat(schemaManager.create("foo")).isNotNull();

        schemas.put("foo", new SchemaContext<>(
            SimpleSample.class,
            "sample",
            SimpleTableResolver.ofString(),
            (path, property) -> true,
            true
        ));
        assertThatThrownBy(() -> schemaManager.create("foo")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void recreates_schema_if_forced() throws Exception {
        schemas.put("foo", new SchemaContext<>(SimpleSample.class, "sample", SimpleTableResolver.ofString()));
        assertThat(schemaManager.create("foo")).isNotNull();

        schemas.put("foo", new SchemaContext<>(
            SimpleSample.class, "sample",
            SimpleTableResolver.ofString(),
            (path, property) -> true,
            true
        ));
        assertThat(schemaManager.create("foo", true, name -> { })).isNotNull();
    }
}
