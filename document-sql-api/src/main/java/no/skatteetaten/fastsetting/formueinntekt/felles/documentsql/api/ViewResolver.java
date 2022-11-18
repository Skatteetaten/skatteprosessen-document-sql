package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface ViewResolver {

    default Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> resolve(Class<?> type) {
        return resolve(type, Collections.emptyList());
    }

    default Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> resolve(Class<?> type, String... roots) {
        return resolve(type, Arrays.stream(roots).map(PathElement::new).toArray(PathElement[]::new));
    }

    default Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> resolve(Class<?> type, PathElement... roots) {
        return resolve(type, roots.length == 0 ? Collections.emptyList() : Collections.singletonList(Arrays.asList(roots)));
    }

    Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> resolve(Class<?> type, List<List<PathElement>> roots);
}
