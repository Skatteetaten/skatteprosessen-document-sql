package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class JaxbPathResolver implements Function<Class<?>, Map<PathElement, PathContext>> {

    private final JaxbHandler handler;

    public JaxbPathResolver(JaxbHandler handler) {
        this.handler = handler;
    }

    @Override
    public Map<PathElement, PathContext> apply(Class<?> type) {
        List<Field> fields = handler.toFields(type);
        Class<?> superClass = type.getSuperclass();
        while (superClass != null && handler.isXmlType(superClass)) {
            fields.addAll(Arrays.asList(superClass.getDeclaredFields()));
            superClass = superClass.getSuperclass();
        }
        Map<PathElement, PathContext> elements = new LinkedHashMap<>();
        for (Field field : fields) {
            if (handler.isXmlTransient(field) || Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            PathElement pathElement = handler.toElement(field).orElseGet(() -> new PathElement(
                field.getName(),
                handler.toNamespace(field.getDeclaringClass()).orElse(null)
            ));
            elements.put(pathElement, PathContext.of(field, () -> handler.toWrapper(field, pathElement)
                .map(Collections::singletonList)
                .orElseGet(Collections::emptyList)));
        }
        return elements;
    }
}
