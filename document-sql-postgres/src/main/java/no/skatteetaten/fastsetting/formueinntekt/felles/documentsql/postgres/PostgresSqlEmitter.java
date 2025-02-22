package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.PathElement;

import static no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcherFactory.*;
import static no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcherFactory.DELETED;

enum PostgresSqlEmitter {

    XML("XML NOT NULL", "XMLPARSE(CONTENT ?)", 1) {
        @Override
        void makeView(
            String base,
            String name,
            List<List<PathElement>> paths,
            List<String> directColumns,
            Map<List<PathElement>, Class<?>> properties,
            Map<List<PathElement>, String> columns,
            List<String> ddl,
            Map<String, Map<String, String>> viewMeta,
            Map<String, String> objects,
            Function<Set<String>, Map<String, String>> namespacePrefixResolver,
            Function<Class<?>, String> typeResolver
        ) {
            Map<String, String> namespaces = namespacePrefixResolver.apply(Stream.concat(paths.stream(), properties.keySet().stream())
                .flatMap(Collection::stream)
                .flatMap(element -> element.getNamespace().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new)));
            String root = "/" + PathElement.full(
                "/",
                Function.identity(),
                paths.stream().flatMap(Collection::stream).collect(Collectors.toList()),
                namespaces::get
            );
            String sqlColumns = Stream.concat(
                directColumns.stream(),
                properties.keySet().stream().map(columns::get)
            ).collect(Collectors.joining(", "));
            String xmlColumns = properties.entrySet().stream().map(entry ->
                columns.get(entry.getKey()) + " "
                    + typeResolver.apply(entry.getValue()) + " "
                    + "PATH '" + PathElement.full(".", "/", Function.identity(), entry.getKey(), namespaces::get) + "'"
            ).collect(Collectors.joining(", "));
            String namespace;
            if (namespaces.isEmpty()) {
                namespace = "";
            } else {
                namespace = namespaces.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue())
                    .map(entry -> {
                        if (entry.getValue().isEmpty()) {
                            throw new IllegalArgumentException("Postgres does not support a default namespace for " + entry.getKey());
                        } else {
                            return "'" + entry.getKey() + "' AS \"" + entry.getValue() + "\"";
                        }
                    }).collect(Collectors.joining(", ", "XMLNAMESPACES(", "), "));
            }
            ddl.add("CREATE FUNCTION " + name + "_RFS() "
                    + "RETURNS TRIGGER LANGUAGE plpgsql "
                    + "AS $$ "
                    + "BEGIN "
                    + "REFRESH MATERIALIZED VIEW " + name + "; "
                    + "RETURN NULL; "
                    + "END $$;");
            objects.put(name + "_RFS", "FUNCTION");
            ddl.add("CREATE MATERIALIZED VIEW " + name + " AS "
                    + "SELECT " + sqlColumns + " "
                    + "FROM " + base + "_RAW, "
                    + "XMLTABLE("
                    + namespace + "'" + root + "' "
                    + "PASSING " + PAYLOAD + " "
                    + "COLUMNS " + xmlColumns + ")");
            objects.put(name, "MATERIALIZED VIEW");
            int index = 0;
            for (String column : Stream.of(
                    Stream.of(ID + ", " + REVISION),
                    directColumns.stream().filter(column -> Stream.of(ID, REVISION, DELETED, PAYLOAD).noneMatch(column::equals)),
                    properties.keySet().stream().map(columns::get).sorted()
            ).flatMap(Function.identity()).collect(Collectors.toList())) {
                ddl.add("CREATE INDEX " + name + "_IDX" + (index++) + " "
                        + "ON " + name + " "
                        + "(" + column + ")");
            }
            ddl.add("CREATE TRIGGER " + name + "_TRG "
                    + "AFTER UPDATE OR INSERT OR DELETE "
                    + "ON " + base + "_RAW "
                    + "FOR EACH STATEMENT "
                    + "EXECUTE PROCEDURE " + name + "_RFS()");
            objects.put(name + "_TRG ON " + base + "_RAW", "TRIGGER");
            viewMeta.put(name, properties.keySet().stream().collect(Collectors.toMap(path -> PathElement.full(
                "/",
                "/",
                Function.identity(),
                Stream.concat(
                    paths.stream().flatMap(Collection::stream),
                    path.stream()
                ).collect(Collectors.toList()),
                namespaces::get
            ), columns::get)));
        }

        @Override
        void makeIndex(String base, List<String> ddl) {
        }
    },

    JSON("JSONB", "CAST(? AS JSONB)",0) {
        @Override
        void makeView(
            String base,
            String name,
            List<List<PathElement>> paths,
            List<String> directColumns,
            Map<List<PathElement>, Class<?>> properties,
            Map<List<PathElement>, String> columns,
            List<String> ddl,
            Map<String, Map<String, String>> viewMeta,
            Map<String, String> objects,
            Function<Set<String>, Map<String, String>> namespacePrefixResolver,
            Function<Class<?>, String> typeResolver
        ) {
            String root = "$" + (paths.isEmpty() ? "" : ("." + paths.stream().map(path ->
                PathElement.full(".", Function.identity(), path, namespace -> "") + "[*]"
            ).collect(Collectors.joining("."))));
            Map<List<PathElement>, String> types = properties.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> typeResolver.apply(entry.getValue())
            ));
            String allColumns = Stream.concat(
                directColumns.stream(),
                properties.keySet().stream().map(path -> "CAST("
                    + PathElement.full("EXPLODED.VALUE", "->", element -> "'" + element + "'", path, namespace -> "") + "->>0 "
                    + "AS " + types.get(path) + ") "
                    + "AS " + columns.get(path))
            ).collect(Collectors.joining(", "));
            ddl.add("CREATE VIEW " + name + " "
                + "AS "
                + "SELECT " + allColumns + " "
                + "FROM " + base + "_RAW, "
                + "JSONB_PATH_QUERY(" + PAYLOAD + ", "
                + "'" + root + "') AS EXPLODED(VALUE)");
            objects.put(name, "VIEW");
            viewMeta.put(name, properties.keySet().stream().collect(Collectors.toMap(path -> PathElement.full(
                "$",
                ".",
                Function.identity(),
                Stream.concat(paths.stream().flatMap(Collection::stream), path.stream()).collect(Collectors.toList()),
                namespace -> ""
            ),columns::get)));
        }

        @Override
        void makeIndex(String base, List<String> ddl) {
            ddl.add("CREATE INDEX " + base + "_IDX ON " + base + "_RAW USING GIN (PAYLOAD)");
        }
    };

    private final String payloadType, valueVariable;

    private final int roots;

    PostgresSqlEmitter(String payloadType, String valueVariable, int roots) {
        this.payloadType = payloadType;
        this.valueVariable = valueVariable;
        this.roots = roots;
    }

    String getPayloadType() {
        return payloadType;
    }

    String getValueVariable() {
        return valueVariable;
    }

    int getRoots() {
        return roots;
    }

    abstract void makeView(
        String base,
        String name,
        List<List<PathElement>> paths,
        List<String> directColumns,
        Map<List<PathElement>, Class<?>> properties,
        Map<List<PathElement>, String> columns,
        List<String> ddl,
        Map<String, Map<String, String>> viewMeta,
        Map<String, String> objects,
        Function<Set<String>, Map<String, String>> namespaceResolver,
        Function<Class<?>, String> typeResolver
    );

    abstract void makeIndex(String base, List<String> ddl);
}
