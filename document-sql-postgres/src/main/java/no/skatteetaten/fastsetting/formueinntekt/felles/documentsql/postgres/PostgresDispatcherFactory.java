package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcher;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcherFactory;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.NameResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.PathElement;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.SyntheticNamespacePrefixResolver;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.TableResolver;

public class PostgresDispatcherFactory implements JdbcDispatcherFactory {

    private static final int MAX_COLUMNS_VIEW = 200;

    private static final int // TODO: Exists etc.
        VIEW_OR_TABLE_EXISTS = 955,
        MATERIALIZED_VIEW_EXISTS = 12006,
        PARAMETER_EXISTS = 44740,
        VIEW_OR_TABLE_NOT_EXISTS = 942,
        MATERIALIZED_VIEW_NOT_EXISTS = 12003,
        PUBLIC_SYNONYM_NOT_EXISTS = 1432;

    private final PostgresSqlEmitter emitter;

    private final Function<Class<?>, String> typeResolver;

    private final Function<Set<String>, Map<String, String>> namespacePrefixResolver;

    private final boolean meta, synonym;

    private final Function<String, List<String>> onCreation, onDrop;

    private PostgresDispatcherFactory(
        PostgresSqlEmitter emitter,
        Function<Set<String>, Map<String, String>> namespacePrefixResolver,
        Function<Class<?>, String> typeResolver,
        boolean meta, boolean synonym,
        Function<String, List<String>> onCreation, Function<String, List<String>> onDrop
    ) {
        this.emitter = emitter;
        this.namespacePrefixResolver = namespacePrefixResolver;
        this.typeResolver = typeResolver;
        this.meta = meta;
        this.synonym = synonym;
        this.onCreation = onCreation;
        this.onDrop = onDrop;
    }

    public static PostgresDispatcherFactory ofXml() {
        return of(PostgresSqlEmitter.XML, new SyntheticNamespacePrefixResolver(true, false));
    }

    public static PostgresDispatcherFactory ofXml(Function<Set<String>, Map<String, String>> namespacePrefixResolver) {
        return of(PostgresSqlEmitter.XML, namespacePrefixResolver);
    }

    public static PostgresDispatcherFactory ofJson() {
        return of(PostgresSqlEmitter.JSON, namespace -> {
            throw new IllegalStateException("Unexpected resolution of namespace " + namespace + " during JSON processing");
        });
    }

    private static PostgresDispatcherFactory of(PostgresSqlEmitter emitter, Function<Set<String>, Map<String, String>> namespacePrefixResolver) {
        return new PostgresDispatcherFactory(
            emitter, namespacePrefixResolver, new PostgresTypeResolver(true),
            true, false,
            base -> Collections.emptyList(), base -> Collections.emptyList()
        );
    }

    public PostgresDispatcherFactory withTypeResolver(Function<Class<?>, String> typeResolver) {
        return new PostgresDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, meta, synonym, onCreation, onDrop);
    }

    public PostgresDispatcherFactory withMeta(boolean meta) {
        return new PostgresDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, meta, synonym, onCreation, onDrop);
    }

    public PostgresDispatcherFactory withSynonym(boolean synonym) {
        return new PostgresDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, meta, synonym, onCreation, onDrop);
    }

    @Override
    public PostgresDispatcherFactory withOnCreation(Function<String, Collection<String>> onCreation) {
        return new PostgresDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, meta, synonym, base -> Stream.concat(
            this.onCreation.apply(base).stream(),
            onCreation.apply(base).stream()
        ).collect(Collectors.toList()), onDrop);
    }

    @Override
    public PostgresDispatcherFactory withOnDrop(Function<String, Collection<String>> onDrop) {
        return new PostgresDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, meta, synonym, onCreation, base -> Stream.concat(
            this.onDrop.apply(base).stream(),
            onDrop.apply(base).stream()
        ).collect(Collectors.toList()));
    }

    @Override
    public <T> JdbcDispatcher<T> create(
        String name,
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views,
        NameResolver nameResolver,
        TableResolver<T> tableResolver
    ) {
        String base = nameResolver.resolve(Collections.singletonList(name));
        Map<String, String> objects = new LinkedHashMap<>();
        List<String> ddl = new ArrayList<>();
        ddl.add("CREATE TABLE " + base + "_RAW ("
            + ID + " VARCHAR(250) NOT NULL, "
            + REVISION + " BIGINT NOT NULL, "
            + DELETED + " BOOLEAN NOT NULL, "
            + PAYLOAD + " " + emitter.getPayloadType() + ", "
            + tableResolver.getAdditionalColumns().entrySet().stream()
            .map(entry -> entry.getKey() + " " + entry.getValue() + ", ")
            .collect(Collectors.joining())
            + "CONSTRAINT " + base + "_PK PRIMARY KEY (" + ID + ", " + REVISION + "))");
        objects.put(base + "_RAW", "TABLE");
        Set<String> indices = new HashSet<>(Collections.singleton(base + "_IDX"));
        for (String column : tableResolver.getAdditionalColumns().keySet()) {
            String index = nameResolver.resolve(Arrays.asList(base, column), indices::contains);
            if (!indices.add(index)) {
                throw new IllegalStateException("Index name already in use: " + index);
            }
            ddl.add("CREATE INDEX " + index + "_IDX ON " + base + "_RAW (" + column + ")");
        }
        for (String suffix : Arrays.asList("MIN", "MAX")) {
            ddl.add("CREATE VIEW " + base + "_" + suffix + " AS "
                + "SELECT " + ID + ", " + suffix + "(" + REVISION + ") " + REVISION + " "
                + "FROM " + base + "_RAW "
                + "GROUP BY " + ID);
            objects.put(base + "_" + suffix, "VIEW");
        }
        ddl.add("CREATE VIEW " + base + "_NOW AS "
            + "SELECT " + ID + ", MAX(" + REVISION + ") " + REVISION + " "
            + "FROM " + base + "_RAW "
            + "GROUP BY " + ID + " "
            + "INTERSECT "
            + "SELECT " + ID + ", " + REVISION + " "
            + "FROM " + base + "_RAW "
            + "WHERE " + DELETED + " = false");
        objects.put(base + "_NOW", "VIEW");
        List<String> directColumns = Stream.concat(
            Stream.of(ID, REVISION, DELETED, PAYLOAD),
            tableResolver.getAdditionalColumns().keySet().stream()
        ).collect(Collectors.toList());
        Map<String, Map<String, String>> viewMeta = new LinkedHashMap<>();
        views.forEach((paths, properties) -> {
            String view = nameResolver.resolve(Stream.concat(
                Stream.of(base),
                paths.stream().flatMap(path -> PathElement.dense(path).stream()).skip(emitter.getRoots())
            ).collect(Collectors.toList()), objects.keySet()::contains);
            if (objects.containsKey(view)) {
                throw new IllegalStateException("View name already in use: " + view);
            }
            Set<String> reserved = new HashSet<>(directColumns);
            Map<List<PathElement>, String> columns = properties.keySet().stream().collect(Collectors.toMap(
                Function.identity(), path -> {
                    String column = nameResolver.resolve(PathElement.dense(path), reserved::contains);
                    if (!reserved.add(column)) {
                        throw new IllegalStateException("Column name " + column + " already assigned for " + view);
                    }
                    return column;
                }
            ));
            if (columns.size() > MAX_COLUMNS_VIEW) {
                Iterator<Map.Entry<List<PathElement>, Class<?>>> iterator = properties.entrySet().stream().sorted(
                    Comparator.comparing(entry -> columns.get(entry.getKey()))
                ).iterator();
                Map<List<PathElement>, Class<?>> current = new HashMap<>();
                int partition = 0;
                while (iterator.hasNext()) {
                    Map.Entry<List<PathElement>, Class<?>> entry = iterator.next();
                    current.put(entry.getKey(), entry.getValue());
                    if (current.size() == MAX_COLUMNS_VIEW || !iterator.hasNext()) {
                        String alias;
                        do {
                            alias = view + "_" + partition++;
                        } while (objects.containsKey(alias));
                        emitter.makeView(
                            base, alias,
                            paths, directColumns, current, columns,
                            ddl, viewMeta, objects,
                            namespacePrefixResolver,
                            typeResolver
                        );
                        current.clear();
                    }
                }
            } else {
                emitter.makeView(
                    base, view,
                    paths, directColumns, properties, columns,
                    ddl, viewMeta, objects,
                    namespacePrefixResolver,
                    typeResolver
                );
            }
        });
        if (meta) {
            ddl.add("CREATE VIEW " + base + "_MTA AS " + Stream.concat(
                Stream.of("SELECT NULL AS PATH, NULL AS OBJECT, NULL AS NAME WHERE 0 = 1"),
                viewMeta.entrySet().stream().flatMap(entry -> entry.getValue().entrySet().stream().map(location -> "SELECT '"
                    + location.getKey() + "' AS PATH, '"
                    + entry.getKey() + "' AS OBJECT, '"
                    + location.getValue() + "' AS NAME"))
            ).collect(Collectors.joining(" UNION ALL ")));
            objects.put(base + "_MTA", "VIEW");
        }
        if (synonym) {
            objects.keySet().forEach(object -> ddl.add("CREATE PUBLIC SYNONYM " + object + " FOR " + object));
        }
        ddl.addAll(onCreation.apply(base + "_RAW"));
        return new PostgresDispatcher<>(
            ddl,
            Stream.of(
                objects.entrySet().stream().map(entry -> "DROP " + entry.getValue() + " " + entry.getKey()),
                synonym ? objects.keySet().stream().<String>map(object -> "DROP PUBLIC SYNONYM " + object) : Stream.<String>empty(),
                onDrop.apply(base + "_RAW").stream()
            ).flatMap(Function.identity()).collect(Collectors.toList()),
            objects.keySet().stream()
                .map(object -> "GRANT SELECT ON " + object + " TO %s")
                .collect(Collectors.toList()),
            "INSERT INTO " + base + "_RAW (" +
                Stream.concat(
                    Stream.of(ID, REVISION, DELETED, PAYLOAD),
                    tableResolver.getAdditionalColumns().keySet().stream()
                ).collect(Collectors.joining(", "))
                + ") VALUES (" +
                Stream.concat(
                    Stream.of("?", "?", "?", emitter.getValueVariable()),
                    Collections.nCopies(tableResolver.getAdditionalColumns().size(), "?").stream()
                ).collect(Collectors.joining(", "))
                + ")",
            "TRUNCATE TABLE " + base + "_RAW",
            tableResolver
        );
    }

    @Override
    public boolean checkError(boolean exists, int code) {
        if (exists) {
            return code == VIEW_OR_TABLE_EXISTS || code == MATERIALIZED_VIEW_EXISTS || code == PARAMETER_EXISTS;
        } else {
            return code == VIEW_OR_TABLE_NOT_EXISTS || code == MATERIALIZED_VIEW_NOT_EXISTS || code == PUBLIC_SYNONYM_NOT_EXISTS;
        }
    }
}
