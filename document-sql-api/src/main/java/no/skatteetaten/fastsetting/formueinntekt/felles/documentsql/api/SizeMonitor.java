package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.math.BigInteger;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface SizeMonitor {

    default Map<String, BigInteger> summary(String... groups) {
        return summary(Set.of(groups));
    }

    default Map<String, BigInteger> summary(Set<String> groups) {
        return summary(groups.stream().collect(Collectors.toMap(Function.identity(), Function.identity())));
    }

    Map<String, BigInteger> summary(Map<String, String> groups);
}
