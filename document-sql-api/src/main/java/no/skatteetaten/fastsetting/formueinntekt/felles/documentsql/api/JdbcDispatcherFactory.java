package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public interface JdbcDispatcherFactory {

    String ID = "ID", REVISION = "REVISION", DELETED = "DELETED", PAYLOAD = "PAYLOAD";

    default JdbcDispatcher<String> create(String name, Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views) {
        return create(name, views, new CapitalizingNameResolver(), SimpleTableResolver.ofString());
    }

    JdbcDispatcherFactory withOnCreation(Function<String, Collection<String>> onCreation);

    JdbcDispatcherFactory withOnDrop(Function<String, Collection<String>> onDrop);

    <T> JdbcDispatcher<T> create(
        String name,
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views,
        NameResolver nameResolver,
        TableResolver<T> tableResolver
    );

    default Optional<String> getBaseTable() {
        return Optional.empty();
    }

    default boolean checkError(boolean exists, SQLException exception) {
        return false;
    }
}
