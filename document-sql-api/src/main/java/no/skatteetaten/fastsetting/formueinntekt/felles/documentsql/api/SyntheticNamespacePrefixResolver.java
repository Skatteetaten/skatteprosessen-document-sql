package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class SyntheticNamespacePrefixResolver implements Function<Set<String>, Map<String, String>> {

    private final boolean emptyToEmpty, firstToEmpty;

    public SyntheticNamespacePrefixResolver() {
        emptyToEmpty = true;
        firstToEmpty = true;
    }

    public SyntheticNamespacePrefixResolver(boolean emptyToEmpty, boolean firstToEmpty) {
        this.emptyToEmpty = emptyToEmpty;
        this.firstToEmpty = firstToEmpty;
    }

    @Override
    public Map<String, String> apply(Set<String> namespaces) {
        Map<String, String> prefixes = new HashMap<>();
        boolean firstToEmpty;
        if (emptyToEmpty && namespaces.contains("")) {
            prefixes.put("", "");
            firstToEmpty = false;
        } else {
            firstToEmpty = this.firstToEmpty;
        }
        int index = 0;
        for (String namespace : namespaces) {
            if (!(emptyToEmpty && namespace.isEmpty())) {
                if (firstToEmpty) {
                    prefixes.put(namespace, "");
                    firstToEmpty = false;
                } else {
                    prefixes.put(namespace, "ns" + ++index);
                }
            }
        }
        return prefixes;
    }
}
