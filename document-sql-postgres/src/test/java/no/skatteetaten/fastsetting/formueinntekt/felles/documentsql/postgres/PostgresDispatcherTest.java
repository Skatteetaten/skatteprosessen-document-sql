package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JacksonJsonPathResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JaxbHandler;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JaxbPathResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcher;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.SimpleTableResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.SimpleViewResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.TableResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.container.SampleContainer;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.container.SampleContainerResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample.AbstractSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample.EmptySample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample.MultipleSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample.NestedSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample.NestedViewSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample.NumericSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample.SimpleSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample.ViewSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample.ViewTerminalSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample.ViewWrapperSample;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

@RunWith(Parameterized.class)
@Category(PostgreSQLContainer.class)
public class PostgresDispatcherTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresDispatcherTest.class);

    private final DispatcherFactory factory;

    private final ObjectMapper mapper;

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {{
            XmlMapper.builder()
                .defaultUseWrapper(false)
                .build()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS),
            new DispatcherFactory("XML") {
                @Override
                <T> JdbcDispatcher<T> apply(String name, Class<?> type, TableResolver<T> resolver) {
                    return JdbcDispatcher.of(PostgresDispatcherFactory.ofXml())
                        .withTableResolver(resolver)
                        .withViewResolver(new SimpleViewResolver(new JaxbPathResolver(JaxbHandler.ofJakarta())))
                        .build(name, type, "sample");
                }
            }
        }, {
            new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS),
            new DispatcherFactory("JSON") {
                @Override
                <T> JdbcDispatcher<T> apply(String name, Class<?> type, TableResolver<T> resolver) {
                    return JdbcDispatcher.of(PostgresDispatcherFactory.ofJson())
                        .withTableResolver(resolver)
                        .withViewResolver(new SimpleViewResolver(new JacksonJsonPathResolver()))
                        .build(name, type);
                }
            }
        }});
    }

    public PostgresDispatcherTest(ObjectMapper mapper, DispatcherFactory factory) {
        this.factory = factory;
        this.mapper = mapper;
    }

    @Rule
    public JdbcDatabaseContainer<?> postgres = new PostgreSQLContainer<>("postgres:12");

    private HikariDataSource dataSource;

    @Before
    public void setUp() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(postgres.getJdbcUrl());
        hikariConfig.setUsername(postgres.getUsername());
        hikariConfig.setPassword(postgres.getPassword());
        dataSource = new HikariDataSource(hikariConfig);
    }

    @After
    public void tearDown() {
        dataSource.close();
    }

    @Test
    public void trivial_dispatcher() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", SimpleSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        SimpleSample example = new SimpleSample();
        example.setVal("foo");
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, VAL "
                + "FROM BAR "
                + "WHERE VAL = 'foo'")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void trivial_dispatcher_container() throws Exception {
        JdbcDispatcher<SampleContainer> dispatcher = factory.apply(
            "bar", SimpleSample.class, new SampleContainerResolver()
        );
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        SimpleSample example = new SimpleSample();
        example.setVal("foo");
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, new SampleContainer(mapper.writeValueAsString(example), "qux"));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, VAL, TAG "
                + "FROM BAR "
                + "WHERE VAL = 'foo'")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.getString(4)).isEqualTo("qux");
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void trivial_dispatcher_numeric() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", NumericSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        NumericSample example = new NumericSample();
        example.setVal(42);
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, VAL "
                + "FROM BAR "
                + "WHERE VAL = 42")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getInt(3)).isEqualTo(42);
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void multiple_dispatcher() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", MultipleSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        MultipleSample example = new MultipleSample();
        example.setVal1("foo");
        example.setVal2("bar");
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, VAL1, VAL2 "
                + "FROM BAR "
                + "WHERE VAL1 = 'foo' and VAL2 = 'bar'")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.getString(4)).isEqualTo("bar");
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void nested_dispatcher() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", NestedSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        NestedSample example = new NestedSample();
        SimpleSample inner = new SimpleSample();
        inner.setVal("foo");
        example.setOuter(inner);
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, OUTER_VAL "
                + "FROM BAR "
                + "WHERE OUTER_VAL = 'foo'")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void empty_dispatcher() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", EmptySample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        EmptySample example = new EmptySample();
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION FROM BAR_RAW")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void abstract_dispatcher() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", AbstractSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        SimpleSample example = new SimpleSample();
        example.setVal("foo");
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, VAL "
                + "FROM BAR "
                + "WHERE VAL = 'foo'")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void nested_list_dispatcher() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", ViewSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        ViewSample example1 = new ViewSample(), example2 = new ViewSample();
        SimpleSample first = new SimpleSample();
        first.setVal("foo");
        SimpleSample second = new SimpleSample();
        second.setVal("bar");
        example1.setList(Arrays.asList(first, second));
        example2.setList(Arrays.asList(first, second));
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example1));
            dispatcher.insert(conn, "Y", 1, mapper.writeValueAsString(example2));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, VAL "
                + "FROM BAR_LIST "
                + "WHERE VAL IN ('foo', 'bar') "
                + "ORDER BY ID, VAL")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("bar");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("Y");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("bar");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("Y");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void nested_list_dispatcher_terminal() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", ViewTerminalSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        ViewTerminalSample example = new ViewTerminalSample();
        example.setList(Arrays.asList("foo", "bar"));
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, VALUE "
                + "FROM BAR_LIST "
                + "WHERE VALUE IN ('foo', 'bar') "
                + "ORDER BY ID, VALUE")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("bar");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void nested_list_dispatcher_wrapped() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", ViewWrapperSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        ViewWrapperSample example1 = new ViewWrapperSample(), example2 = new ViewWrapperSample();
        SimpleSample first = new SimpleSample();
        first.setVal("foo");
        SimpleSample second = new SimpleSample();
        second.setVal("bar");
        example1.setList(Arrays.asList(first, second));
        example2.setList(Arrays.asList(first, second));
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example1));
            dispatcher.insert(conn, "Y", 1, mapper.writeValueAsString(example2));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, VAL "
                + "FROM BAR_LIST "
                + "WHERE VAL IN ('foo', 'bar') "
                + "ORDER BY ID, VAL")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("bar");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("Y");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("bar");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("Y");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void multiple_nested_list_dispatcher() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", NestedViewSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        NestedViewSample example1 = new NestedViewSample(), example2 = new NestedViewSample();
        SimpleSample first = new SimpleSample();
        first.setVal("foo");
        SimpleSample second = new SimpleSample();
        second.setVal("bar");
        SimpleSample third = new SimpleSample();
        third.setVal("qux");
        SimpleSample forth = new SimpleSample();
        forth.setVal("baz");
        ViewSample firstView = new ViewSample();
        firstView.setList(Arrays.asList(first, second));
        ViewSample secondView = new ViewSample();
        secondView.setList(Arrays.asList(third, forth));
        example1.setOther(Arrays.asList(firstView, secondView));
        example2.setOther(Arrays.asList(firstView, secondView));
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example1));
            dispatcher.insert(conn, "Y", 1, mapper.writeValueAsString(example2));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, VAL "
                + "FROM BAR_OTHER_LIST "
                //+ "WHERE VAL IN ('foo', 'bar', 'qux', 'baz') "
                + "ORDER BY ID, VAL")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("bar");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("baz");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("qux");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("Y");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("bar");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("Y");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("baz");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("Y");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("foo");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("Y");
            assertThat(rs.getInt(2)).isEqualTo(1);
            assertThat(rs.getString(3)).isEqualTo("qux");
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void nested_list_dispatcher_long_data() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", ViewSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        ViewSample example = new ViewSample();
        example.setList(IntStream.range(0, 1000).mapToObj(index -> {
            SimpleSample inner = new SimpleSample();
            inner.setVal("val" + index);
            return inner;
        }).collect(Collectors.toList()));
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(DISTINCT VAL) "
                + "FROM BAR_LIST")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(1000);
        }

        dispatcher.drop(dataSource);
    }

    @Test
    public void trivial_dispatcher_latest_version() throws SQLException, JsonProcessingException {
        JdbcDispatcher<String> dispatcher = factory.apply("bar", NumericSample.class);
        dispatcher.printToEach(LOGGER::info);

        dispatcher.create(dataSource);

        NumericSample example = new NumericSample();
        example.setVal(42);
        try (Connection conn = dataSource.getConnection()) {
            dispatcher.insert(conn, "X", 1, mapper.writeValueAsString(example));
            example.setVal(84);
            dispatcher.insert(conn, "X", 2, mapper.writeValueAsString(example));
        }

        try (
            Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT ID, REVISION, VAL "
                + "FROM BAR "
                + "NATURAL JOIN BAR_MAX "
                + "WHERE VAL = 84")
        ) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isEqualTo("X");
            assertThat(rs.getInt(2)).isEqualTo(2);
            assertThat(rs.getInt(3)).isEqualTo(84);
            assertThat(rs.next()).isFalse();
        }

        dispatcher.drop(dataSource);
    }

    abstract static class DispatcherFactory {

        private final String description;

        DispatcherFactory(String description) {
            this.description = description;
        }

        JdbcDispatcher<String> apply(String name, Class<?> type) {
            return apply(name, type, SimpleTableResolver.ofString());
        }

        abstract <T> JdbcDispatcher<T> apply(String name, Class<?> type, TableResolver<T> resolver);

        @Override
        public String toString() {
            return description;
        }
    }
}