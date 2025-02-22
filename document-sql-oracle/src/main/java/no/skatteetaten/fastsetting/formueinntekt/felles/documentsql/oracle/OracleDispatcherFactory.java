package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle;

import java.sql.SQLException;
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
import java.util.Optional;
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

public class OracleDispatcherFactory implements JdbcDispatcherFactory {

    private static final int MAX_COLUMNS_VIEW = 200;

    private static final int
        VIEW_OR_TABLE_EXISTS = 955,
        MATERIALIZED_VIEW_EXISTS = 12006,
        PARAMETER_EXISTS = 44740,
        VIEW_OR_TABLE_NOT_EXISTS = 942,
        MATERIALIZED_VIEW_NOT_EXISTS = 12003,
        PUBLIC_SYNONYM_NOT_EXISTS = 1432;

    private final OracleSqlEmitter emitter;
    private final Function<Class<?>, String> typeResolver;
    private final Function<Set<String>, Map<String, String>> namespacePrefixResolver;

    private final boolean audit, meta, synonym, grantViewOnDummy;

    private final Function<String, List<String>> onCreation, onDrop;

    private OracleDispatcherFactory(
        OracleSqlEmitter emitter,
        Function<Set<String>, Map<String, String>> namespacePrefixResolver,
        Function<Class<?>, String> typeResolver,
        boolean audit, boolean meta, boolean synonym, boolean grantViewOnDummy,
        Function<String, List<String>> onCreation, Function<String, List<String>> onDrop
    ) {
        this.emitter = emitter;
        this.namespacePrefixResolver = namespacePrefixResolver;
        this.typeResolver = typeResolver;
        this.onCreation = onCreation;
        this.onDrop = onDrop;
        this.audit = audit;
        this.meta = meta;
        this.synonym = synonym;
        this.grantViewOnDummy = grantViewOnDummy;
    }

    public static OracleDispatcherFactory ofXml() {
        return of(OracleSqlEmitter.XML, new SyntheticNamespacePrefixResolver());
    }

    public static OracleDispatcherFactory ofXml(Function<Set<String>, Map<String, String>> namespacePrefixResolver) {
        return of(OracleSqlEmitter.XML, namespacePrefixResolver);
    }

    public static OracleDispatcherFactory ofJson() {
        return of(OracleSqlEmitter.JSON, namespace -> {
            throw new IllegalStateException("Unexpected resolution of namespace " + namespace + " during JSON processing");
        });
    }

    private static OracleDispatcherFactory of(OracleSqlEmitter emitter, Function<Set<String>, Map<String, String>> namespacePrefixResolver) {
        return new OracleDispatcherFactory(
            emitter, namespacePrefixResolver, new OracleTypeResolver(true),
            false, true, false, false,
            base -> Collections.emptyList(), base -> Collections.emptyList()
        );
    }

    public OracleDispatcherFactory withTypeResolver(Function<Class<?>, String> typeResolver) {
        return new OracleDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, audit, meta, synonym, grantViewOnDummy, onCreation, onDrop);
    }

    public OracleDispatcherFactory withAudit(boolean audit) {
        return new OracleDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, audit, meta, synonym, grantViewOnDummy, onCreation, onDrop);
    }

    public OracleDispatcherFactory withMeta(boolean meta) {
        return new OracleDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, audit, meta, synonym, grantViewOnDummy, onCreation, onDrop);
    }

    public OracleDispatcherFactory withSynonym(boolean synonym) {
        return new OracleDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, audit, meta, synonym, grantViewOnDummy, onCreation, onDrop);
    }

    public OracleDispatcherFactory withGrantViewOnDummy(boolean grantViewOnDummy) {
        return new OracleDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, audit, meta, synonym, grantViewOnDummy, onCreation, onDrop);
    }

    @Override
    public OracleDispatcherFactory withOnCreation(Function<String, Collection<String>> onCreation) {
        return new OracleDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, audit, meta, synonym, grantViewOnDummy, base -> Stream.concat(
            this.onCreation.apply(base).stream(),
            onCreation.apply(base).stream()
        ).collect(Collectors.toList()), onDrop);
    }

    @Override
    public OracleDispatcherFactory withOnDrop(Function<String, Collection<String>> onDrop) {
        return new OracleDispatcherFactory(emitter, namespacePrefixResolver, typeResolver, audit, meta, synonym, grantViewOnDummy, onCreation, base -> Stream.concat(
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
        Map<String, Integer> viewIndices = new HashMap<>();
        List<String> ddl = new ArrayList<>();
        ddl.add("CREATE TABLE " + base + "_RAW ("
            + ID + " VARCHAR2(250) NOT NULL, "
            + REVISION + " NUMBER(19) NOT NULL, "
            + DELETED + " NUMBER(1) NOT NULL, "
            + PAYLOAD + " " + emitter.getPayloadType() + ", "
            + tableResolver.getAdditionalColumns().entrySet().stream()
            .map(entry -> entry.getKey() + " " + entry.getValue() + ", ")
            .collect(Collectors.joining())
            + "CONSTRAINT " + base + "_PK PRIMARY KEY (" + ID + ", " + REVISION + "))");
        objects.put(base + "_RAW", "TABLE");
        ddl.addAll(emitter.afterCreateTable(base));
        Set<String> indices = new HashSet<>(Collections.singleton(base + "_IDX"));
        for (String column : tableResolver.getAdditionalColumns().keySet()) {
            String index = nameResolver.resolve(Arrays.asList(base, column), indices::contains);
            if (!indices.add(index)) {
                throw new IllegalStateException("Index name already in use: " + index);
            }
            ddl.add("CREATE INDEX " + index + "_IDX ON " + base + "_RAW (" + column + ")");
        }
        for (String suffix : Arrays.asList("MIN", "MAX")) {
            viewIndices.put(base + "_" + suffix, ddl.size());
            ddl.add("CREATE VIEW " + base + "_" + suffix + " AS "
                + "SELECT " + ID + ", " + suffix + "(" + REVISION + ") " + REVISION + " "
                + "FROM " + base + "_RAW "
                + "GROUP BY " + ID);
            objects.put(base + "_" + suffix, "VIEW");
        }
        viewIndices.put(base + "_NOW", ddl.size());
        ddl.add("CREATE VIEW " + base + "_NOW AS "
            + "SELECT " + ID + ", MAX(" + REVISION + ") " + REVISION + " "
            + "FROM " + base + "_RAW "
            + "GROUP BY " + ID + " "
            + "INTERSECT "
            + "SELECT " + ID + ", " + REVISION + " "
            + "FROM " + base + "_RAW "
            + "WHERE " + DELETED + " = 0");
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
                            ddl, viewIndices, viewMeta, objects,
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
                    ddl, viewIndices, viewMeta, objects,
                    namespacePrefixResolver,
                    typeResolver
                );
            }
        });
        if (audit) {
            ddl.add("AUDIT SELECT ON " + base + "_RAW BY ACCESS");
            objects.entrySet().stream()
                .filter(entry -> entry.getValue().equals("MATERIALIZED VIEW"))
                .forEach(entry -> ddl.add("AUDIT SELECT ON " + entry.getKey() + " BY ACCESS"));
        }
        if (meta) {
            ddl.add("CREATE VIEW " + base + "_MTA AS " + Stream.concat(
                Stream.of("SELECT NULL AS PATH, NULL AS OBJECT, NULL AS NAME FROM DUAL WHERE 0 = 1"),
                viewMeta.entrySet().stream().flatMap(entry -> entry.getValue().entrySet().stream().map(location -> "SELECT '"
                    + location.getKey() + "' AS PATH, '"
                    + entry.getKey() + "' AS OBJECT, '"
                    + location.getValue() + "' AS NAME "
                    + "FROM DUAL"))
            ).collect(Collectors.joining(" UNION ALL ")));
            objects.put(base + "_MTA", "VIEW");
        }
        if (synonym) {
            objects.keySet().forEach(object -> ddl.add("CREATE PUBLIC SYNONYM " + object + " FOR " + object));
        }
        ddl.addAll(onCreation.apply(base + "_RAW"));
        return new OracleDispatcher<>(
            ddl,
            Stream.of(
                objects.entrySet().stream().map(entry -> "DROP " + entry.getValue() + " " + entry.getKey()),
                synonym ? objects.keySet().stream().<String>map(object -> "DROP PUBLIC SYNONYM " + object) : Stream.<String>empty(),
                onDrop.apply(base + "_RAW").stream()
            ).flatMap(Function.identity()).collect(Collectors.toList()),
            objects.keySet().stream()
                .map(object -> "GRANT SELECT ON " + object + " TO %s")
                .collect(Collectors.toList()),
            grantViewOnDummy ? viewIndices.entrySet().stream().collect(Collectors.toMap(
                entry -> ddl.get(entry.getValue()),
                entry -> "CREATE VIEW " + entry.getKey() + " AS SELECT DUMMY FROM DUAL"
            )) : Collections.emptyMap(),
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
    public Optional<String> getBaseTable() {
        return Optional.of("DUAL");
    }

    @Override
    public boolean checkError(boolean exists, SQLException exception) {
        if (exists) {
            return exception.getErrorCode() == VIEW_OR_TABLE_EXISTS
                    || exception.getErrorCode() == MATERIALIZED_VIEW_EXISTS
                    || exception.getErrorCode() == PARAMETER_EXISTS;
        } else {
            return exception.getErrorCode() == VIEW_OR_TABLE_NOT_EXISTS
                    || exception.getErrorCode() == MATERIALIZED_VIEW_NOT_EXISTS
                    || exception.getErrorCode() == PUBLIC_SYNONYM_NOT_EXISTS;
        }
    }
}
