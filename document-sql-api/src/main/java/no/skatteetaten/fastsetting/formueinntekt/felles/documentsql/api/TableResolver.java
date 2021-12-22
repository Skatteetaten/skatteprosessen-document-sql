package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.stream.Collectors;

public interface TableResolver<T> {

    String toPayload(T value);

    void registerAdditionalValues(int index, PreparedStatement ps, T value) throws SQLException;

    Map<String, String> getAdditionalColumns();

    default String getId() {
        Deque<Class<?>> types = new ArrayDeque<>();
        Class<?> type = getClass();
        do {
            types.addFirst(type);
            type = type.getDeclaringClass();
        } while (type != null);
        return types.stream().map(Class::getSimpleName).collect(Collectors.joining("."));
    }
}
