package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SimpleViewResolver implements ViewResolver {

    private static final String JAXB_XML_CALENDAR = "javax.xml.datatype.XMLGregorianCalendar";
    private static final String JAKARTA_XML_CALENDAR = "jakarta.xml.datatype.XMLGregorianCalendar";

    private static final Set<Class<?>> TERMINAL_TYPES = Set.of(
        boolean.class,
        Boolean.class,
        byte.class,
        Byte.class,
        short.class,
        Short.class,
        char.class,
        Character.class,
        int.class,
        Integer.class,
        long.class,
        Long.class,
        float.class,
        Float.class,
        double.class,
        Double.class,
        Number.class,
        BigDecimal.class,
        BigInteger.class,
        Year.class,
        Object.class,
        String.class,
        Date.class,
        Duration.class,
        MonthDay.class,
        YearMonth.class,
        LocalTime.class,
        LocalDate.class,
        LocalDateTime.class,
        OffsetTime.class,
        OffsetDateTime.class,
        ZonedDateTime.class
    );

    private final Function<Class<?>, Map<PathElement, PathContext>> resolver;
    private final Predicate<Class<?>> isTerminal;
    private final BiPredicate<List<List<PathElement>>, Class<?>> filter;

    public SimpleViewResolver() {
        resolver = new SimplePathResolver();
        isTerminal = type -> TERMINAL_TYPES.contains(type)
            || type.getName().equals(JAXB_XML_CALENDAR)
            || type.getName().equals(JAKARTA_XML_CALENDAR);
        filter = (elements, type) -> type != Object.class;
    }

    public SimpleViewResolver(Function<Class<?>, Map<PathElement, PathContext>> resolver) {
        this.resolver = resolver;
        isTerminal = type -> TERMINAL_TYPES.contains(type)
            || type.getName().equals(JAXB_XML_CALENDAR)
            || type.getName().equals(JAKARTA_XML_CALENDAR);
        filter = (elements, type) -> type != Object.class;
    }

    public SimpleViewResolver(BiPredicate<List<String>, Class<?>> filter) {
        resolver = new SimplePathResolver();
        isTerminal = type -> TERMINAL_TYPES.contains(type)
            || type.getName().equals(JAXB_XML_CALENDAR)
            || type.getName().equals(JAKARTA_XML_CALENDAR);
        this.filter = (elements, type) -> type != Object.class && filter.test(
            PathElement.dense(elements.stream().flatMap(Collection::stream).collect(Collectors.toList())),
            type
        );
    }

    public SimpleViewResolver(Function<Class<?>, Map<PathElement, PathContext>> resolver, BiPredicate<List<String>, Class<?>> filter) {
        this.resolver = resolver;
        isTerminal = type -> TERMINAL_TYPES.contains(type)
            || type.getName().equals(JAXB_XML_CALENDAR)
            || type.getName().equals(JAKARTA_XML_CALENDAR);
        this.filter = (elements, type) -> type != Object.class && filter.test(
            PathElement.dense(elements.stream().flatMap(Collection::stream).collect(Collectors.toList())),
            type
        );
    }

    public SimpleViewResolver(
        Function<Class<?>, Map<PathElement, PathContext>> resolver,
        Predicate<Class<?>> isTerminal,
        BiPredicate<List<List<PathElement>>, Class<?>> filter
    ) {
        this.resolver = resolver;
        this.isTerminal = isTerminal;
        this.filter = filter;
    }

    @Override
    public Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> resolve(Class<?> type, List<List<PathElement>> roots) {
        if (!filter.test(roots, type)) {
            return Collections.emptyMap();
        }
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new LinkedHashMap<>();
        Deque<Map.Entry<List<List<PathElement>>, ChainedValue<Class<?>>>> work = new ArrayDeque<>();
        work.add(Map.entry(roots, new ChainedValue<>(type, type)));
        do {
            Map.Entry<List<List<PathElement>>, ChainedValue<Class<?>>> current = work.removeFirst();
            if (current.getValue().getValue().isEnum() || isTerminal.test(current.getValue().getValue())) {
                views.put(current.getKey(), Collections.singletonMap(Collections.emptyList(), current.getValue().getValue()));
                continue;
            }
            Map<List<PathElement>, Class<?>> properties = new LinkedHashMap<>();
            Deque<Map.Entry<List<PathElement>, ChainedValue<PathContext>>> unresolved = new ArrayDeque<>();
            resolver.apply(current.getValue().getValue()).forEach((element, context) -> unresolved.addLast(Map.entry(
                Collections.singletonList(element),
                new ChainedValue<>(context)
            )));
            while (!unresolved.isEmpty()) {
                Map.Entry<List<PathElement>, ChainedValue<PathContext>> entry = unresolved.removeFirst();
                entry.getValue().getValue().accept(property -> {
                    if (filter.test(PathElement.merge(current.getKey(), entry.getKey()), property)) {
                        if (property.isEnum() || isTerminal.test(property)) {
                            properties.put(entry.getKey(), property);
                        } else {
                            resolver.apply(property).forEach((element, context) -> unresolved.addLast(Map.entry(
                                PathElement.merge(entry.getKey(), element),
                                entry.getValue().chain(context, property)
                            )));
                        }
                    }
                }, (property, wrappers) -> {
                    List<List<PathElement>> paths = PathElement.merge(current.getKey(), wrappers, entry.getKey());
                    if (filter.test(paths, property)) {
                        work.addLast(Map.entry(paths, current.getValue().chain(property, property)));
                    }
                });
            }
            if (!properties.isEmpty()) {
                views.put(current.getKey(), properties);
            }
        } while (!work.isEmpty());
        return views;
    }

    static class ChainedValue<T> {

        private final T value;

        private final Set<Class<?>> previous;

        ChainedValue(T value) {
            this.value = value;
            previous = Collections.emptySet();
        }

        ChainedValue(T value, Class<?> type) {
            this.value = value;
            previous = Collections.singleton(type);
        }

        private ChainedValue(T value, Set<Class<?>> previous) {
            this.value = value;
            this.previous = previous;
        }

        T getValue() {
            return value;
        }

        ChainedValue<T> chain(T value, Class<?> type) {
            Set<Class<?>> previous = new LinkedHashSet<>(this.previous);
            if (previous.add(type)) {
                return new ChainedValue<>(value, previous);
            } else {
                throw new IllegalArgumentException(previous.stream().map(Class::getTypeName).collect(Collectors.joining(
                    " -> ",
                    "Cannot resolve chain with recursive occurrence of " + type + " via ",
                    ""
                )));
            }
        }
    }
}
