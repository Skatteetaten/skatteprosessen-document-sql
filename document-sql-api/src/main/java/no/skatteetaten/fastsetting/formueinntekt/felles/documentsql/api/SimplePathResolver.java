package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SimplePathResolver implements Function<Class<?>, Map<PathElement, PathContext>> {

    @Override
    public Map<PathElement, PathContext> apply(Class<?> type) {
        List<Field> fields = new ArrayList<>(Arrays.asList(type.getDeclaredFields()));
        Class<?> superClass = type.getSuperclass();
        while (superClass != null && superClass != Object.class) {
            fields.addAll(Arrays.asList(superClass.getDeclaredFields()));
            superClass = superClass.getSuperclass();
        }
        Map<PathElement, PathContext> elements = new LinkedHashMap<>();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
                continue;
            }
            elements.put(new PathElement(field.getName()), PathContext.of(field, Collections::emptyList));
        }
        return elements;
    }
}
