package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

public class SimpleTableResolver<T> implements TableResolver<T> {

    private final Function<T, String> transformer;

    public SimpleTableResolver(Function<T, String> transformer) {
        this.transformer = transformer;
    }

    public static TableResolver<String> ofString() {
        return new SimpleTableResolver<>(Function.identity());
    }

    @Override
    public String toPayload(T value) {
        return transformer.apply(value);
    }

    @Override
    public void registerAdditionalValues(int index, PreparedStatement ps, T value) { }

    @Override
    public Map<String, String> getAdditionalColumns() {
        return Collections.emptyMap();
    }
}
