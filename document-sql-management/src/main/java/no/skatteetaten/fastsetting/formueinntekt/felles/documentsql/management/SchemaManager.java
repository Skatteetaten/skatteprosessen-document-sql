package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.management;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.sql.DataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.CapitalizingNameResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcher;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcherFactory;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.PathContext;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.PathElement;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.SimplePathResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaManager<T> {

    public static final String CHANGE_LOG = "liquibase/schemamanagementChangeLog.xml";

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaManager.class);

    private final DataSource dataSource;

    private final Function<String, Optional<SchemaContext<T>>> schemas;

    private final JdbcDispatcherFactory factory;

    private final Function<Class<?>, Map<PathElement, PathContext>> pathResolver;

    private final BiFunction<Class<?>, String, List<List<PathElement>>> rootResolver;

    private final int length;

    private final Collection<String> users;

    public SchemaManager(
        DataSource dataSource,
        Map<String, SchemaContext<T>> schemas,
        JdbcDispatcherFactory factory
    ) {
        this.dataSource = dataSource;
        this.schemas = name -> Optional.ofNullable(schemas.get(name));
        this.factory = factory;
        pathResolver = new SimplePathResolver();
        rootResolver = (type, root) -> Collections.singletonList(Collections.singletonList(new PathElement(root)));
        length = CapitalizingNameResolver.ORA_MAX_LENGTH;
        users = Collections.emptySet();
    }

    public SchemaManager(
        DataSource dataSource,
        Function<String, Optional<SchemaContext<T>>> schemas,
        JdbcDispatcherFactory factory
    ) {
        this.dataSource = dataSource;
        this.schemas = schemas;
        this.factory = factory;
        pathResolver = new SimplePathResolver();
        rootResolver = (type, root) -> Collections.singletonList(Collections.singletonList(new PathElement(root)));
        length = CapitalizingNameResolver.ORA_MAX_LENGTH;
        users = Collections.emptySet();
    }

    private SchemaManager(
        DataSource dataSource,
        Function<String, Optional<SchemaContext<T>>> schemas,
        JdbcDispatcherFactory factory,
        Function<Class<?>, Map<PathElement, PathContext>> pathResolver,
        BiFunction<Class<?>, String, List<List<PathElement>>> rootResolver,
        int length,
        Collection<String> users
    ) {
        this.dataSource = dataSource;
        this.schemas = schemas;
        this.factory = factory;
        this.pathResolver = pathResolver;
        this.rootResolver = rootResolver;
        this.length = length;
        this.users = users;
    }

    public SchemaManager<T> withResolvers(
        Function<Class<?>, Map<PathElement, PathContext>> pathResolver,
        BiFunction<Class<?>, String, List<List<PathElement>>> rootResolver
    ) {
        return new SchemaManager<T>(dataSource, schemas, factory, pathResolver, rootResolver, length, users);
    }

    public SchemaManager<T> withLength(int length) {
        return new SchemaManager<T>(dataSource, schemas, factory, pathResolver, rootResolver, length, users);
    }

    public SchemaManager<T> withUsers(Collection<String> users) {
        return new SchemaManager<T>(dataSource, schemas, factory, pathResolver, rootResolver, length, users);
    }

    public JdbcDispatcher<T> create(String name) throws SQLException {
        return create(name, false, forced -> {
            throw new IllegalStateException("Did not expect forcing of " + forced);
        });
    }

    public JdbcDispatcher<T> create(String name, boolean force) throws SQLException {
        return create(name, force, forced -> { });
    }

    public JdbcDispatcher<T> create(String name, boolean force, Consumer<String> onForce) throws SQLException {
        SchemaContext<T> context = schemas.apply(name).orElseThrow(() -> new UnknownSchemaException(name));
        boolean exists;
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO SCHEMA_GENERATION "
                    + "(KEY_NAME, CLASS_NAME, ROOT_NAME, NAME_LENGTH, RESOLVER_ID) "
                    + "SELECT ?, ?, ?, ?, ? "
                    + factory.getBaseTable().map(table -> "FROM " + table + " ").orElse("")
                    + "WHERE NOT EXISTS (SELECT KEY_NAME FROM SCHEMA_GENERATION WHERE KEY_NAME = ?)"
            )) {
                ps.setString(1, name);
                ps.setString(2, context.getType().getName());
                ps.setString(3, context.getRoot().orElse(null));
                ps.setInt(4, length);
                ps.setString(5, context.getResolverId());
                ps.setString(6, name);
                exists = ps.executeUpdate() == 0;
            }
            if (exists) {
                try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT CLASS_NAME, ROOT_NAME, NAME_LENGTH, RESOLVER_ID "
                        + "FROM SCHEMA_GENERATION "
                        + "WHERE KEY_NAME = ?")
                ) {
                    ps.setString(1, name);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            assertConsistency(rs, name, context);
                        } else {
                            throw new IllegalStateException("Found no result for " + name);
                        }
                    }
                } catch (SchemaConsistencyException e) {
                    if (force) {
                        LOGGER.info("Replacing altered schema for {}: {}", name, e.getMessage());
                        context.toDispatcher(name, e.getLength(), factory, pathResolver, rootResolver).drop(
                            dataSource,
                            LOGGER::debug,
                            exception -> factory.checkError(false, exception)
                        );
                        onForce.accept(name);
                        LOGGER.info("Successfully removed outdated schema for {} - starting recreation", name);
                        JdbcDispatcher<T> dispatcher = context.toDispatcher(name, length, factory, pathResolver, rootResolver);
                        dispatcher.create(dataSource, users, LOGGER::debug);
                        try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE SCHEMA_GENERATION "
                                + "SET CLASS_NAME = ?, ROOT_NAME = ?, NAME_LENGTH = ?, RESOLVER_ID = ? "
                                + "WHERE KEY_NAME = ?"
                        )) {
                            ps.setString(1, context.getType().getName());
                            ps.setString(2, context.getRoot().orElse(null));
                            ps.setInt(3, length);
                            ps.setString(4, context.getResolverId());
                            ps.setString(5, name);
                            if (ps.executeUpdate() == 0) {
                                throw new IllegalStateException("Could not update schema context for " + name);
                            }
                        }
                        LOGGER.info("Completed schema updating for {}", name);
                        return dispatcher;
                    } else if (e.isRecreation()) {
                        throw new IllegalStateException("Recreation of " + name + " requires forcing enabled", e);
                    } else {
                        throw new IllegalStateException("Schema for " + name + " has changed in a non-compatible manner", e);
                    }
                }
                LOGGER.info("Found existing schema for {}", name);
                return context.toDispatcher(name, length, factory, pathResolver, rootResolver);
            } else {
                LOGGER.info("Creating schema for {} using {}, {}", name, context.getType(), context.getRoot());
                try {
                    JdbcDispatcher<T> dispatcher = context.toDispatcher(name, length, factory, pathResolver, rootResolver);
                    try {
                        dispatcher.create(dataSource, users, LOGGER::debug);
                    } catch (SQLException e) {
                        if (force && factory.checkError(true, e)) {
                            LOGGER.info("Found conflicting tables for {} - attempting to replace schema", name);
                            try {
                                dispatcher.drop(dataSource, LOGGER::debug, exception -> factory.checkError(false, exception));
                            } catch (SQLException suppressed) {
                                if (!factory.checkError(false, suppressed)) {
                                    e.addSuppressed(suppressed);
                                    throw e;
                                }
                            }
                            onForce.accept(name);
                            LOGGER.info("Successfully removed conflicting schema for {} - starting recreation", name);
                            dispatcher.create(dataSource, users, LOGGER::debug);
                            LOGGER.info("Completed schema recreation for {}", name);
                        } else {
                            throw e;
                        }
                    }
                    return dispatcher;
                } catch (Throwable throwable) {
                    try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE "
                            + "FROM SCHEMA_GENERATION "
                            + "WHERE KEY_NAME = ?"
                    )) {
                        ps.setString(1, name);
                        if (ps.executeUpdate() == 1) {
                            LOGGER.warn("Deleted {} after schema creation error", name, throwable);
                        }
                    }
                    throw new IllegalStateException("Failed schema creation for " + name, throwable);
                }
            }
        }
    }

    public boolean drop(String name) throws SQLException {
        SchemaContext<T> context = schemas.apply(name).orElseThrow(() -> new UnknownSchemaException(name));
        try (Connection conn = dataSource.getConnection()) {
            boolean exists;
            try (PreparedStatement ps = conn.prepareStatement(
                "SELECT CLASS_NAME, ROOT_NAME, NAME_LENGTH, RESOLVER_ID "
                    + "FROM SCHEMA_GENERATION "
                    + "WHERE KEY_NAME = ?"
            )) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        assertConsistency(rs, name, context);
                        exists = true;
                    } else {
                        exists = false;
                    }
                }
            }
            if (exists) {
                LOGGER.info("Dropping schema for {} using {}, {}", name, context.getType(), context.getRoot());
                JdbcDispatcher<?> dispatcher = context.toDispatcher(name, length, factory, pathResolver, rootResolver);
                dispatcher.drop(dataSource, LOGGER::debug, exception -> factory.checkError(false, exception));
                try (PreparedStatement ps = conn.prepareStatement(
                    "DELETE "
                        + "FROM SCHEMA_GENERATION "
                        + "WHERE KEY_NAME = ?"
                )) {
                    ps.setString(1, name);
                    if (ps.executeUpdate() == 0) {
                        throw new IllegalStateException("Schema for " + name + " seems to be deleted already");
                    }
                }
                return true;
            }
            return false;
        }
    }

    private void assertConsistency(ResultSet rs, String name, SchemaContext<?> context) throws SQLException {
        int nameLength = rs.getInt("NAME_LENGTH");
        if (!rs.getString("CLASS_NAME").equals(context.getType().getName())) {
            throw new SchemaConsistencyException(name, "type", context.getType(), nameLength);
        } else if (!Objects.equals(rs.getString("ROOT_NAME"), context.getRoot().orElse(null))) {
            throw new SchemaConsistencyException(name, "root", context.getRoot().orElse(null), nameLength);
        } else if (nameLength != this.length) {
            throw new SchemaConsistencyException(name, "name length", nameLength, nameLength);
        } else if (!rs.getString("RESOLVER_ID").equals(context.getResolverId())) {
            throw new SchemaConsistencyException(name, "resolver", context.getResolverId(), nameLength);
        } else if (context.isRecreate()) {
            throw new SchemaConsistencyException(name, nameLength);
        }
    }
}
