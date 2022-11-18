package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class JaxbHandler {

    private static final String DEFAULT = "##default";

    private final Class<? extends Annotation> xmlType, xmlSchema, xmlElement, xmlElementWrapper, xmlTransient, xmlSeeAlso;
    private final MethodHandle xmlTypeNamespace, xmlSchemaNamespace, xmlElementName, xmlElementNamespace, xmlElementWrapperName, xmlElementWrapperNamespace, xmlSeeAlsoValue;

    @SuppressWarnings("unchecked")
    private JaxbHandler(ClassLoader classLoader, String namespace) {
        try {
            xmlType = (Class<? extends Annotation>) Class.forName(namespace + ".XmlType", true, classLoader);
            xmlTypeNamespace = MethodHandles.publicLookup().findVirtual(xmlType, "namespace", MethodType.methodType(String.class));
            xmlSchema = (Class<? extends Annotation>) Class.forName(namespace + ".XmlSchema", true, classLoader);
            xmlSchemaNamespace = MethodHandles.publicLookup().findVirtual(xmlSchema, "namespace", MethodType.methodType(String.class));
            xmlElement = (Class<? extends Annotation>) Class.forName(namespace + ".XmlElement", true, classLoader);
            xmlElementName = MethodHandles.publicLookup().findVirtual(xmlElement, "name", MethodType.methodType(String.class));
            xmlElementNamespace = MethodHandles.publicLookup().findVirtual(xmlElement, "namespace", MethodType.methodType(String.class));
            xmlElementWrapper = (Class<? extends Annotation>) Class.forName(namespace + ".XmlElementWrapper", true, classLoader);
            xmlElementWrapperName = MethodHandles.publicLookup().findVirtual(xmlElementWrapper, "name", MethodType.methodType(String.class));
            xmlElementWrapperNamespace = MethodHandles.publicLookup().findVirtual(xmlElementWrapper, "namespace", MethodType.methodType(String.class));
            xmlTransient = (Class<? extends Annotation>) Class.forName(namespace + ".XmlTransient", true, classLoader);
            xmlSeeAlso = (Class<? extends Annotation>) Class.forName(namespace + ".XmlSeeAlso", true, classLoader);
            xmlSeeAlsoValue = MethodHandles.publicLookup().findVirtual(xmlSeeAlso, "value", MethodType.methodType(Class[].class));
        } catch (Exception e) {
            throw new IllegalStateException("Could not resolve JAXB for namespace " + namespace + " in " + classLoader, e);
        }
    }

    public static JaxbHandler ofJavax() {
        return ofJavax(JaxbHandler.class.getClassLoader());
    }

    public static JaxbHandler ofJavax(ClassLoader classLoader) {
        return new JaxbHandler(classLoader, "javax.xml.bind.annotation");
    }

    public static JaxbHandler ofJakarta() {
        return ofJakarta(JaxbHandler.class.getClassLoader());
    }

    public static JaxbHandler ofJakarta(ClassLoader classLoader) {
        return new JaxbHandler(classLoader, "jakarta.xml.bind.annotation");
    }

    boolean isXmlType(Class<?> type) {
        return type.isAnnotationPresent(xmlType);
    }

    boolean isXmlTransient(Field field) {
        return field.isAnnotationPresent(xmlTransient);
    }

    Optional<String> toNamespace(Class<?> property) {
        try {
            Annotation type = property.getAnnotation(xmlType);
            if (type != null && !xmlTypeNamespace.invoke(type).equals(DEFAULT)) {
                return Optional.of((String) xmlTypeNamespace.invoke(type)).filter(namespace -> !namespace.isEmpty());
            } else if (property.getPackage() == null) {
                return Optional.empty();
            }
            Annotation schema = property.getPackage().getAnnotation(xmlSchema);
            return schema == null || ((String) xmlSchemaNamespace.invoke(schema)).isEmpty()
                ? Optional.empty()
                : Optional.of((String) xmlSchemaNamespace.invoke(schema));
        } catch (Throwable t) {
            throw new IllegalStateException(t);
        }
    }

    Optional<PathElement> toElement(Field field) {
        return Optional.ofNullable(field.getAnnotation(xmlElement)).map(element -> {
            try {
                return new PathElement(
                    xmlElementName.invoke(element).equals(DEFAULT)
                        ? field.getName()
                        : (String) xmlElementName.invoke(element),
                    xmlElementNamespace.invoke(element).equals(DEFAULT) || ((String) xmlElementNamespace.invoke(element)).isEmpty()
                        ? toNamespace(field.getDeclaringClass()).orElse(null)
                        : (String) xmlElementNamespace.invoke(element)
                );
            } catch (Throwable t) {
                throw new IllegalStateException(t);
            }
        });
    }

    Optional<PathElement> toWrapper(Field field, PathElement pathElement) {
        return Optional.ofNullable(field.getAnnotation(xmlElementWrapper)).map(elementWrapper -> {
            try {
                return new PathElement(
                    xmlElementWrapperName.invoke(elementWrapper).equals(DEFAULT)
                        ? field.getName()
                        : (String) xmlElementWrapperName.invoke(elementWrapper),
                    xmlElementWrapperNamespace.invoke(elementWrapper).equals(DEFAULT) || ((String) xmlElementWrapperNamespace.invoke(elementWrapper)).isEmpty()
                        ? toNamespace(field.getDeclaringClass()).orElse(null)
                        : (String) xmlElementWrapperNamespace.invoke(elementWrapper),
                    false
                );
            } catch (Throwable t) {
                throw new IllegalStateException(t);
            }
        });
    }

    List<Field> toFields(Class<?> type) {
        List<Field> fields = new ArrayList<>(Arrays.asList(type.getDeclaredFields()));
        Annotation seeAlso = type.getAnnotation(xmlSeeAlso);
        if (seeAlso != null) {
            Class<?>[] values;
            try {
                values = (Class<?>[]) xmlSeeAlsoValue.invoke(seeAlso);
            } catch (Throwable t) {
                throw new IllegalStateException(t);
            }
            for (Class<?> specialization : values) {
                fields.addAll(toFields(specialization));
            }
        }
        return fields;
    }
}
