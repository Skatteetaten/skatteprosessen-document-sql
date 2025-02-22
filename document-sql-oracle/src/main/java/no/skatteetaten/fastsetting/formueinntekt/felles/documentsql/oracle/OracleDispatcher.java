package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle;

import javax.sql.DataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcher;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.RevisionedId;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.TableResolver;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class OracleDispatcher<T> implements JdbcDispatcher<T> {

    private final List<String> create, drop, grant;
    private final Map<String, String> overrides;
    private final String insert, truncate;

    private final TableResolver<T> resolver;

    OracleDispatcher(
        List<String> create, List<String> drop, List<String> grant,
        Map<String, String> overrides,
        String insert, String truncate, TableResolver<T> resolver
    ) {
        this.create = create;
        this.drop = drop;
        this.grant = grant;
        this.overrides = overrides;
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
            create.stream().map(OracleDispatcher::asExecutableStatement),
            Stream.of("-- grant"),
            grant.stream().map(statement -> asExecutableStatement(String.format(statement, "[users]"))),
            Stream.of("-- insert"),
            Stream.of(asExecutableStatement(insert)),
            Stream.of("-- truncate"),
            Stream.of(asExecutableStatement(truncate)),
            Stream.of("-- drop"),
            drop.stream().map(OracleDispatcher::asExecutableStatement)
        ).flatMap(Function.identity()).collect(Collectors.toList()));
    }

    @Override
    public void create(DataSource dataSource, Collection<String> users, Consumer<String> callback) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            List<String> delayed = new ArrayList<>(overrides.size());
            for (String sql : create) {
                if (!users.isEmpty() && overrides.containsKey(sql)) {
                    if (!sql.startsWith("CREATE")) {
                        throw new IllegalStateException("Override does not create object: " + sql);
                    } else if (!sql.startsWith("CREATE OR REPLACE")) {
                        delayed.add("CREATE OR REPLACE" + sql.substring("CREATE".length()));
                    } else {
                        delayed.add(sql);
                    }
                    sql = overrides.get(sql);
                }
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
            for (String sql : delayed) {
                callback.accept(sql);
                try {
                    stmt.execute(sql);
                } catch (SQLException e) {
                    throw new SQLException("Failed to execute: " + sql, e.getSQLState(), e.getErrorCode(), e);
                }
            }
        }
    }

    @Override
    public void drop(DataSource dataSource, Consumer<String> callback, Predicate<SQLException> check) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            for (String sql : drop) {
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
        List<Clob> clobs = new ArrayList<>(payloads.size());
        try (PreparedStatement ps = conn.prepareStatement(insert)) {
            for (Map.Entry<RevisionedId, T> payload : payloads.entrySet()) {
                ps.setString(1, payload.getKey().getId());
                ps.setLong(2, payload.getKey().getRevision());
                ps.setBoolean(3, payload.getKey().isDeleted());
                String value = resolver.toPayload(payload.getValue());
                if (value == null) {
                    ps.setNull(4, Types.CLOB);
                } else {
                    Clob clob = conn.createClob();
                    clobs.add(clob);
                    clob.setString(1, value);
                    ps.setClob(4, clob);
                }
                resolver.registerAdditionalValues(5, ps, payload.getValue());
                if (payloads.size() == 1) {
                    ps.executeUpdate();
                } else {
                    ps.addBatch();
                }
            }
            if (payloads.size() > 1) {
                ps.executeBatch();
            }
        } finally {
            for (Clob clob : clobs) {
                clob.free();
            }
        }
    }

    @Override
    public void truncate(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(truncate);
        }
    }
}
