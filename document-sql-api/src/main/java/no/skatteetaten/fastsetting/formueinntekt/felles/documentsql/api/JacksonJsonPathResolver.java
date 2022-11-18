package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;

public class JacksonJsonPathResolver implements Function<Class<?>, Map<PathElement, PathContext>> {

    @Override
    public Map<PathElement, PathContext> apply(Class<?> type) {
        List<Field> fields = toFields(type);
        Class<?> superClass = type.getSuperclass();
        while (superClass != null && superClass != Object.class) {
            fields.addAll(Arrays.asList(superClass.getDeclaredFields()));
            superClass = superClass.getSuperclass();
        }
        Map<PathElement, PathContext> elements = new LinkedHashMap<>();
        for (Field field : fields) {
            if (isTransient(field)) {
                continue;
            }
            elements.put(new PathElement(resolve(field)), PathContext.of(field, Collections::emptyList));
        }
        return elements;
    }

    private static boolean isTransient(Field property) {
        if (property.isAnnotationPresent(JsonIgnore.class)) {
            return true;
        } else if (property.getType().isAnnotationPresent(JsonIgnoreType.class)) {
            return true;
        } else if (property.getType().isAnnotationPresent(JsonIgnoreProperties.class)) {
            return Arrays.asList(property.getDeclaringClass()
                .getAnnotation(JsonIgnoreProperties.class)
                .value()).contains(resolve(property));
        } else {
            return false;
        }
    }

    private static String resolve(Field property) {
        JsonProperty element = property.getAnnotation(JsonProperty.class);
        if (element != null) {
            return element.value().isEmpty() ? property.getName() : element.value();
        } else {
            return property.getName();
        }
    }

    private static List<Field> toFields(Class<?> type) {
        List<Field> fields = new ArrayList<>(Arrays.asList(type.getDeclaredFields()));
        JsonSubTypes subTypes = type.getAnnotation(JsonSubTypes.class);
        if (subTypes != null) {
            for (JsonSubTypes.Type subType : subTypes.value()) {
                fields.addAll(toFields(subType.value()));
            }
        }
        return fields;
    }
}
