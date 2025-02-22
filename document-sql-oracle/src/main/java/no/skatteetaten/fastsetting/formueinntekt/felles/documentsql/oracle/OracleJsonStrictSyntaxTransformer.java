package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

class OracleJsonStrictSyntaxTransformer implements Function<String, String> {

    private final Map<String, String> escape = new LinkedHashMap<>();
    private final Set<String> quote = new HashSet<>();

    OracleJsonStrictSyntaxTransformer() {
        escape.put("\\", "\\\\");
        escape.put("\t", "\\t");
        escape.put("\r", "\\r");
        escape.put("\b", "\\b");
        escape.put("\f", "\\f");
        escape.put("\n", "\\n");
        escape.put("\"", "\\\"");
        escape.put("/", "\\/");
        quote.add(".");
        quote.add("$");
        quote.add("@");
    }

    @Override
    public String apply(String element) {
        if (escape.keySet().stream().anyMatch(element::contains) || quote.stream().anyMatch(element::contains)) {
            for (Map.Entry<String, String> entry : escape.entrySet()) {
                element = element.replace(entry.getKey(), entry.getValue());
            }
            return "\"" + element + "\"";
        } else {
            return element;
        }
    }
}
