package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface JdbcDispatcherFactory {

    String ID = "ID", REVISION = "REVISION", DELETED = "DELETED", PAYLOAD = "PAYLOAD";

    default JdbcDispatcher<String> create(String name, Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views) {
        return create(name, views, new CapitalizingNameResolver(), SimpleTableResolver.ofString());
    }

    <T> JdbcDispatcher<T> create(
        String name,
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views,
        NameResolver nameResolver,
        TableResolver<T> tableResolver
    );

    default Optional<String> getBaseTable() {
        return Optional.empty();
    }

    default boolean checkError(boolean exists, int code) {
        return false;
    }
}
