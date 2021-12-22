package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

@FunctionalInterface
public interface PathContext {

    void accept(Consumer<Class<?>> onTerminal, BiConsumer<Class<?>, List<PathElement>> onBranch);

    static PathContext of(Field field, Supplier<List<PathElement>> wrappers) {
        if (Collection.class.isAssignableFrom(field.getType())) {
            Type genericType = field.getGenericType();
            if (!(genericType instanceof ParameterizedType)) {
                throw new IllegalArgumentException("Did not expect non-parameterized type for " + field);
            }
            ParameterizedType parameterizedType = ((ParameterizedType) genericType);
            if (parameterizedType.getActualTypeArguments().length != 1) {
                throw new IllegalArgumentException("Did expect a single generic type for " + field);
            } else if (!(parameterizedType.getActualTypeArguments()[0] instanceof Class<?>)) {
                throw new IllegalArgumentException("Did not expect a nested generic type for " + field);
            }
            return (onTerminal, onBranch) -> onBranch.accept(
                (Class<?>) parameterizedType.getActualTypeArguments()[0], wrappers.get()
            );
        } else {
            return (onTerminal, onBranch) -> onTerminal.accept(field.getType());
        }
    }
}
