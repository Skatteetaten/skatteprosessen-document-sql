package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.sql.DataSource;

public class StubJdbcDispatcher<T> implements JdbcDispatcher<T>, JdbcDispatcherFactory {

    private final String baseTable;

    public StubJdbcDispatcher() {
        baseTable = null;
    }

    public StubJdbcDispatcher(String baseTable) {
        this.baseTable = baseTable;
    }

    @Override
    public void printTo(Consumer<List<String>> consumer) {
        consumer.accept(Collections.singletonList("-- stubbed dispatcher"));
    }

    @Override
    public JdbcDispatcherFactory withOnCreation(Function<String, Collection<String>> onCreation) {
        return this;
    }

    @Override
    public JdbcDispatcherFactory withOnDrop(Function<String, Collection<String>> onDrop) {
        return this;
    }

    @Override
    public void create(DataSource dataSource, Collection<String> users, Consumer<String> callback) { }

    @Override
    public void drop(DataSource dataSource, Consumer<String> callback, Predicate<SQLException> check) { }

    @Override
    public void insert(Connection conn, Map<RevisionedId, T> payloads) { }

    @Override
    public void truncate(DataSource dataSource) { }

    @Override
    public <T> JdbcDispatcher<T> create(
        String name,
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views,
        NameResolver nameResolver,
        TableResolver<T> tableResolver
    ) {
        return new StubJdbcDispatcher<>(baseTable);
    }

    @Override
    public Optional<String> getBaseTable() {
        return Optional.ofNullable(baseTable);
    }
}
