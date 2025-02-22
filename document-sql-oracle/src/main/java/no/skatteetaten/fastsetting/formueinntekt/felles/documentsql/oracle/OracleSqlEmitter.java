package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle;

import static no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcherFactory.DELETED;
import static no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcherFactory.ID;
import static no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcherFactory.PAYLOAD;
import static no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.JdbcDispatcherFactory.REVISION;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.PathElement;

enum OracleSqlEmitter {

    XML("XMLTYPE NOT NULL", "XMLTYPE(?)", 1) {
        @Override
        List<String> afterCreateTable(String base) {
            return Collections.singletonList("CREATE INDEX " + base + "_IDX "
                + "ON " + base + "_RAW (" + PAYLOAD + ") "
                + "INDEXTYPE IS XDB.XMLINDEX "
                + "PARAMETERS ('PATH TABLE " + base + "_PTL')");
        }

        @Override
        void makeView(
            String base,
            String name,
            List<List<PathElement>> paths,
            List<String> directColumns,
            Map<List<PathElement>, Class<?>> properties,
            Map<List<PathElement>, String> columns,
            List<String> ddl,
            Map<String, Integer> viewIndices,
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
                            return "DEFAULT '" + entry.getKey() + "'";
                        } else {
                            return "'" + entry.getKey() + "' AS \"" + entry.getValue() + "\"";
                        }
                    }).collect(Collectors.joining(", ", "XMLNAMESPACES(", "), "));
            }
            viewIndices.put(name, ddl.size());
            ddl.add("CREATE VIEW " + name + " AS "
                + "SELECT " + sqlColumns + " "
                + "FROM " + base + "_RAW, "
                + "XMLTABLE("
                + namespace + "'" + root + "' "
                + "PASSING " + PAYLOAD + " "
                + "COLUMNS " + xmlColumns + ")");
            ddl.add(safeParameterRegistration(name + "_IDP", "ADD_GROUP GROUP " + name + "_GRP "
                + "XMLTABLE " + name + "_GPI "
                + namespace.replace("'", "''")
                + "''" + root + "'' "
                + "COLUMNS " + xmlColumns.replace("'", "''")));
            ddl.add("ALTER INDEX " + base + "_IDX "
                + "PARAMETERS ('PARAM " + name + "_IDP')");
            ddl.add("BEGIN "
                + "DBMS_XMLINDEX.DROPPARAMETER("
                + "'" + name + "_IDP'"
                + "); "
                + "END;");
            int index = 0;
            for (String column : properties.keySet().stream().map(columns::get).sorted().collect(Collectors.toList())) {
                ddl.add("CREATE INDEX " + name + "_IDX" + (index++) + " "
                    + "ON " + name + "_GPI "
                    + "(" + column + ")");
            }
            objects.put(name, "VIEW");
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

        private static final int MAX_STRING_LENGTH = 32_767 - 1;

        private String safeParameterRegistration(String name, String value) {
            String registration, declaration;
            if (value.length() > MAX_STRING_LENGTH) {
                declaration = "DECLARE INDEX_PARAM CLOB; ";
                registration = IntStream.rangeClosed(0, value.length() / MAX_STRING_LENGTH)
                    .mapToObj(index -> value.substring(
                        index * MAX_STRING_LENGTH,
                        Math.min(value.length(), (index + 1) * MAX_STRING_LENGTH)
                    ))
                    .map(segment -> { // Repairs SQL character escapes which must not be split over two lines.
                        int pre = 0;
                        while (pre < segment.length() && segment.charAt(pre) == '\'') {
                            pre += 1;
                        }
                        if (pre % 2 == 1) {
                            segment = segment.substring(1);
                        }
                        int post = 0;
                        while (post < segment.length() && segment.charAt(segment.length() - post - 1) == '\'') {
                            post += 1;
                        }
                        if (post % 2 == 1) {
                            segment = segment + "'";
                        }
                        return segment;
                    })
                    .filter(segment -> !segment.isEmpty())
                    .map(segment -> "INDEX_PARAM := INDEX_PARAM || '" + segment + "'; ")
                    .collect(Collectors.joining(
                        "",
                        "INDEX_PARAM := NULL; ",
                        "DBMS_XMLINDEX.REGISTERPARAMETER("
                            + "'" + name + "', "
                            + "INDEX_PARAM"
                            + "); "
                    ));
            } else {
                declaration = "";
                registration = "DBMS_XMLINDEX.REGISTERPARAMETER("
                    + "'" + name + "', "
                    + "'" + value + "'"
                    + "); ";
            }
            return declaration
                + "BEGIN "
                + registration
                + "EXCEPTION WHEN OTHERS THEN "
                + "DBMS_XMLINDEX.DROPPARAMETER('" + name + "'); "
                + registration
                + "END;";
        }
    },

    JSON("CLOB NOT NULL CHECK (" + PAYLOAD + " IS JSON)", "?",0) {
        @Override
        List<String> afterCreateTable(String base) {
            return Collections.singletonList("CREATE MATERIALIZED VIEW LOG ON " + base + "_RAW WITH PRIMARY KEY");
        }

        @Override
        void makeView(
            String base,
            String name,
            List<List<PathElement>> paths,
            List<String> directColumns,
            Map<List<PathElement>, Class<?>> properties,
            Map<List<PathElement>, String> columns,
            List<String> ddl,
            Map<String, Integer> viewIndices,
            Map<String, Map<String, String>> viewMeta,
            Map<String, String> objects,
            Function<Set<String>, Map<String, String>> namespacePrefixResolver,
            Function<Class<?>, String> typeResolver
        ) {
            String root = "$" + (paths.isEmpty() ? "" : ("." + paths.stream().map(path ->
                PathElement.full(".", new OracleJsonStrictSyntaxTransformer(), path, namespace -> "") + "[*]"
            ).collect(Collectors.joining("."))));
            String allColumns = Stream.concat(
                directColumns.stream(),
                properties.keySet().stream().map(columns::get)
            ).collect(Collectors.joining(", "));
            Map<List<PathElement>, String> types = properties.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> typeResolver.apply(entry.getValue())
            ));
            String jsonColumns = properties.keySet().stream().map(path ->
                columns.get(path) + " "
                    + types.get(path) + " "
                    + "PATH '" + PathElement.full("$", ".", new OracleJsonStrictSyntaxTransformer(), path, namespace -> "") + "' "
                    + "NULL ON EMPTY ERROR ON ERROR"
            ).collect(Collectors.joining(", "));
            ddl.add("CREATE MATERIALIZED VIEW " + name + " "
                + "BUILD IMMEDIATE "
                + "REFRESH FAST ON STATEMENT WITH PRIMARY KEY "
                + "AS "
                + "SELECT " + allColumns + " "
                + "FROM " + base + "_RAW, "
                + "JSON_TABLE(" + PAYLOAD + ", "
                + "'" + root + "' "
                + "COLUMNS (" + jsonColumns + "))");
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
            objects.put(name, "MATERIALIZED VIEW");
            viewMeta.put(name, properties.keySet().stream().collect(Collectors.toMap(path -> PathElement.full(
                "$",
                ".",
                new OracleJsonStrictSyntaxTransformer(),
                Stream.concat(paths.stream().flatMap(Collection::stream), path.stream()).collect(Collectors.toList()),
                namespace -> ""
            ),columns::get)));
        }
    };

    private final String payloadType, valueVariable;

    private final int roots;

    OracleSqlEmitter(String payloadType, String valueVariable, int roots) {
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

    abstract List<String> afterCreateTable(String base);

    abstract void makeView(
        String base,
        String name,
        List<List<PathElement>> paths,
        List<String> directColumns,
        Map<List<PathElement>, Class<?>> properties,
        Map<List<PathElement>, String> columns,
        List<String> ddl,
        Map<String, Integer> viewIndices,
        Map<String, Map<String, String>> viewMeta,
        Map<String, String> objects,
        Function<Set<String>, Map<String, String>> namespaceResolver,
        Function<Class<?>, String> typeResolver
    );
}
