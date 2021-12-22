package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcher;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.RevisionedId;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.TableResolver;

class PostgresDispatcher<T> implements JdbcDispatcher<T> {

    private final List<String> create, drop, grant;
    private final String insert, truncate;

    private final TableResolver<T> resolver;

    PostgresDispatcher(
        List<String> create, List<String> drop, List<String> grant,
        String insert, String truncate, TableResolver<T> resolver
    ) {
        this.create = create;
        this.drop = drop;
        this.grant = grant;
        this.insert = insert;
        this.truncate = truncate;
        this.resolver = resolver;
    }

    private static String asExecutableStatement(String sql) {
        return sql + (sql.endsWith(";") ? "" : ";");
    }

    @Override
    public void printTo(Consumer<List<String>> consumer) {
        consumer.accept(Stream.of(
            Stream.of("-- creation"),
            create.stream().map(PostgresDispatcher::asExecutableStatement),
            Stream.of("-- grant"),
            grant.stream().map(statement -> asExecutableStatement(String.format(statement, "[users]"))),
            Stream.of("-- insert"),
            Stream.of(asExecutableStatement(insert)),
            Stream.of("-- truncate"),
            Stream.of(asExecutableStatement(truncate)),
            Stream.of("-- drop"),
            drop.stream().map(PostgresDispatcher::asExecutableStatement)
        ).flatMap(Function.identity()).collect(Collectors.toList()));
    }

    @Override
    public void create(DataSource dataSource, Collection<String> users, Consumer<String> callback) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            for (String sql : create) {
                callback.accept(sql);
                try {
                    stmt.execute(sql);
                } catch (SQLException e) {
                    throw new SQLException("Failed to execute: " + sql, e.getSQLState(), e.getErrorCode(), e);
                }
            }
            if (!users.isEmpty()) {
                String listOfUsers = users.stream()
                    .map(String::toUpperCase)
                    .distinct()
                    .sorted()
                    .collect(Collectors.joining(", "));
                for (String statement : grant) {
                    String sql = String.format(statement, listOfUsers);
                    callback.accept(sql);
                    try {
                        stmt.execute(sql);
                    } catch (SQLException e) {
                        throw new SQLException("Failed to execute: " + sql, e.getSQLState(), e.getErrorCode(), e);
                    }
                }
            }
        }
    }

    @Override
    public void drop(DataSource dataSource, Consumer<String> callback, Predicate<SQLException> check) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            ListIterator<String> it = drop.listIterator(drop.size());
            while (it.hasPrevious()) {
                String sql = it.previous();
                callback.accept(sql);
                try {
                    stmt.execute(sql);
                } catch (SQLException e) {
                    if (!check.test(e)) {
                        throw new SQLException("Failed to execute: " + sql, e.getSQLState(), e.getErrorCode(), e);
                    }
                }
            }
        }
    }

    @Override
    public void insert(Connection conn, Map<RevisionedId, T> payloads) throws SQLException {
        if (payloads.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(insert)) {
            for (Map.Entry<RevisionedId, T> payload : payloads.entrySet()) {
                ps.setString(1, payload.getKey().getId());
                ps.setLong(2, payload.getKey().getRevision());
                ps.setBoolean(3, payload.getKey().isDeleted());
                ps.setString(4, resolver.toPayload(payload.getValue()));
                resolver.registerAdditionalValues(5, ps, payload.getValue());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    @Override
    public void truncate(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(truncate);
        }
    }
}
