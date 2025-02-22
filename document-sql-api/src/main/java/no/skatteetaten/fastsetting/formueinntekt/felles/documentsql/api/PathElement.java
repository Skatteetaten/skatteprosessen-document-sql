package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PathElement {

    private final String name, namespace;
    private final boolean manifest;

    public PathElement(String name) {
        this.name = name;
        namespace = null;
        manifest = true;
    }

    public PathElement(String name, String namespace) {
        this.name = name;
        this.namespace = namespace;
        manifest = true;
    }

    public PathElement(String name, String namespace, boolean manifest) {
        this.name = name;
        this.namespace = namespace;
        this.manifest = manifest;
    }

    public String getName() {
        return name;
    }

    public Optional<String> getNamespace() {
        return Optional.ofNullable(namespace);
    }

    public boolean isManifest() {
        return manifest;
    }

    static List<PathElement> merge(
        List<PathElement> key,
        PathElement... elements
    ) {
        return Stream.concat(
            key.stream(),
            Stream.of(elements)
        ).collect(Collectors.toList());
    }

    @SafeVarargs
    static List<List<PathElement>> merge(
        List<List<PathElement>> prefix,
        List<PathElement>... elements
    ) {
        return Stream.concat(
            prefix.stream(),
            Stream.of(Stream.of(elements).flatMap(Collection::stream).collect(Collectors.toList()))
        ).collect(Collectors.toList());
    }

    public static List<String> dense(List<PathElement> path) {
        return path.stream()
            .filter(element -> element.manifest)
            .map(element -> element.name)
            .collect(Collectors.toList());
    }

    private static List<String> full(
        Function<String, String> transformer,
        List<PathElement> path,
        Function<String, String> toNamespacePrefix
    ) {
        return path.stream().map(element -> {
            if (element.namespace == null) {
                return transformer.apply(element.name);
            } else {
                String prefix = toNamespacePrefix.apply(element.namespace);
                if (prefix.isEmpty()) {
                    return transformer.apply(element.name);
                } else {
                    return prefix + ":" + transformer.apply(element.name);
                }
            }
        }).collect(Collectors.toList());
    }

    public static String full(
        String infix,
        Function<String, String> transformer,
        List<PathElement> path,
        Function<String, String> toNamespacePrefix
    ) {
        if (path.isEmpty()) {
            return "";
        } else {
            return String.join(infix, full(transformer, path, toNamespacePrefix));
        }
    }

    public static String full(
        String prefix,
        String infix,
        Function<String, String> transformer,
        List<PathElement> path,
        Function<String, String> toNamespacePrefix
    ) {
        if (path.isEmpty()) {
            return prefix;
        } else {
            return prefix + infix + String.join(infix, full(transformer, path, toNamespacePrefix));
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        PathElement that = (PathElement) object;
        if (manifest != that.manifest) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        return namespace != null ? namespace.equals(that.namespace) : that.namespace == null;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (namespace != null ? namespace.hashCode() : 0);
        result = 31 * result + (manifest ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return name;
    }
}
