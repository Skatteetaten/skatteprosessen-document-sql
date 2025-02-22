package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.sql.DataSource;

public interface JdbcDispatcher<T> {

    static Builder<String> of(JdbcDispatcherFactory factory) {
        return new Builder<>(factory, new SimpleViewResolver(), new CapitalizingNameResolver(), SimpleTableResolver.ofString());
    }

    default void printToEach(Consumer<String> consumer) {
        printTo(strings -> strings.forEach(consumer));
    }

    void printTo(Consumer<List<String>> consumer);

    default void create(DataSource dataSource) throws SQLException {
        create(dataSource, sql -> { });
    }

    default void create(DataSource dataSource, Consumer<String> callback) throws SQLException {
        create(dataSource, Collections.emptySet(), callback);
    }

    default void create(DataSource dataSource, Collection<String> users) throws SQLException {
        create(dataSource, users, sql -> { });
    }

    void create(DataSource dataSource, Collection<String> users, Consumer<String> callback) throws SQLException;

    default void drop(DataSource dataSource) throws SQLException {
        drop(dataSource, sql -> { });
    }

    default void drop(DataSource dataSource, Consumer<String> callback) throws SQLException {
        drop(dataSource, callback, exception -> false);
    }

    void drop(DataSource dataSource, Consumer<String> callback, Predicate<SQLException> check) throws SQLException;

    default void insert(Connection conn, String id, T payload) throws SQLException {
        insert(conn, id, 1, payload);
    }

    default void insert(Connection conn, String id, int revision, T payload) throws SQLException {
        insert(conn, id, revision, false, payload);
    }

    default void insert(Connection conn, String id, int revision, boolean deleted, T payload) throws SQLException {
        insert(conn, new RevisionedId(id, revision, deleted), payload);
    }

    default void insert(Connection conn, RevisionedId id, T payload) throws SQLException {
        insert(conn, Collections.singletonMap(id, payload));
    }

    void insert(Connection conn, Map<RevisionedId, T> payloads) throws SQLException;

    default void insert(DataSource dataSource, String id, T payload) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            insert(conn, id, payload);
        }
    }

    default void insert(DataSource dataSource, String id, int revision, T payload) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            insert(conn, id, revision, payload);
        }
    }

    default void insert(DataSource dataSource, String id, int revision, boolean deleted, T payload) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            insert(conn, id, revision, deleted, payload);
        }
    }

    default void insert(DataSource dataSource, RevisionedId id, T payload) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            insert(conn, id, payload);
        }
    }

    default void insert(DataSource dataSource, Map<RevisionedId, T> payloads) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            insert(conn, payloads);
        }
    }

    void truncate(DataSource dataSource) throws SQLException;

    class Builder<T> {

        private final JdbcDispatcherFactory factory;

        private final ViewResolver viewResolver;
        private final NameResolver nameResolver;
        private final TableResolver<T> tableResolver;

        private Builder(JdbcDispatcherFactory factory, ViewResolver viewResolver, NameResolver nameResolver, TableResolver<T> tableResolver) {
            this.factory = factory;
            this.viewResolver = viewResolver;
            this.nameResolver = nameResolver;
            this.tableResolver = tableResolver;
        }

        public Builder<T> withViewResolver(ViewResolver viewResolver) {
            return new Builder<>(factory, viewResolver, nameResolver, tableResolver);
        }

        public Builder<T> withNameResolver(NameResolver nameResolver) {
            return new Builder<>(factory, viewResolver, nameResolver, tableResolver);
        }

        public <S> Builder<S> withTableResolver(TableResolver<S> tableResolver) {
            return new Builder<>(factory, viewResolver, nameResolver, tableResolver);
        }

        public JdbcDispatcher<T> build(String name, Class<?> type) {
            return build(name, type, new PathElement[0]);
        }

        public JdbcDispatcher<T> build(String name, Class<?> type, String... roots) {
            return build(name, type, Arrays.stream(roots).map(PathElement::new).toArray(PathElement[]::new));
        }

        public JdbcDispatcher<T> build(String name, Class<?> type, PathElement... roots) {
            return build(name, type, roots.length == 0 ? Collections.emptyList() : Collections.singletonList(Arrays.asList(roots)));
        }

        public JdbcDispatcher<T> build(String name, Class<?> type, List<List<PathElement>> roots) {
            return factory.create(name, viewResolver.resolve(type, roots), nameResolver, tableResolver);
        }
    }
}
