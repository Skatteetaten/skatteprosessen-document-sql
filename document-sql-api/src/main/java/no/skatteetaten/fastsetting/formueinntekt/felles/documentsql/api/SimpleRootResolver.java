package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

public class SimpleRootResolver implements BiFunction<Class<?>, String, List<List<PathElement>>>  {

    @Override
    public List<List<PathElement>> apply(Class<?> type, String root) {
        return Collections.singletonList(Collections.singletonList(new PathElement(root)));
    }
}
