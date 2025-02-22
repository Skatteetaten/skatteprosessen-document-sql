package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.management;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.CapitalizingNameResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcher;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcherFactory;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.PathContext;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.PathElement;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.SimpleViewResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.TableResolver;

public class SchemaContext<T> {

    private final Class<?> type;

    private final String root;

    private final TableResolver<T> tableResolver;

    private final BiPredicate<List<String>, Class<?>> filter;

    private final boolean recreate;

    public SchemaContext(
        Class<?> type,
        String root,
        TableResolver<T> tableResolver
    ) {
        this.type = type;
        this.root = root;
        this.tableResolver = tableResolver;
        filter = (path, property) -> true;
        recreate = false;
    }

    public SchemaContext(
        Class<?> type,
        String root,
        TableResolver<T> tableResolver,
        BiPredicate<List<String>, Class<?>> filter,
        boolean recreate
    ) {
        this.type = type;
        this.root = root;
        this.tableResolver = tableResolver;
        this.filter = filter;
        this.recreate = recreate;
    }

    Class<?> getType() {
        return type;
    }

    Optional<String> getRoot() {
        return Optional.ofNullable(root);
    }

    String getResolverId() {
        return tableResolver.getId();
    }

    boolean isRecreate() {
        return recreate;
    }

    JdbcDispatcher<T> toDispatcher(
        String name,
        int length,
        JdbcDispatcherFactory factory,
        Function<Class<?>, Map<PathElement, PathContext>> pathResolver,
        BiFunction<Class<?>, String, List<List<PathElement>>> rootResolver
    ) {
        return JdbcDispatcher.of(factory)
            .withViewResolver(new SimpleViewResolver(pathResolver, filter))
            .withNameResolver(new CapitalizingNameResolver(length))
            .withTableResolver(tableResolver)
            .build(name, type, root == null ? Collections.emptyList() : rootResolver.apply(type, root));
    }
}
