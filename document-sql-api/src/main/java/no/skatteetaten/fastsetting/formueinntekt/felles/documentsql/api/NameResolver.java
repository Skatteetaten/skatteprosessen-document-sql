package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

@FunctionalInterface
public interface NameResolver {

    default String resolve(String... elements) {
        return resolve(Arrays.asList(elements));
    }

    default String resolve(List<String> elements) {
        return resolve(elements, name -> false);
    }

    String resolve(List<String> path, Predicate<String> isReserved);
}
