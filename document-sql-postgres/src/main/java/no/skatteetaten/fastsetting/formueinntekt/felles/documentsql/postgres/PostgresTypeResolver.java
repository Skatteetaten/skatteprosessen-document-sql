package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Year;
import java.util.Set;
import java.util.function.Function;

public class PostgresTypeResolver implements Function<Class<?>, String> {

    private static final Set<Class<?>> NUMERIC_TYPES = Set.of(
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
        BigInteger.class,
        Year.class
    );

    private static final Set<Class<?>> DECIMAL_TYPES = Set.of(
        float.class,
        Float.class,
        double.class,
        Double.class,
        Number.class,
        BigDecimal.class
    );

    private final boolean decimalsAsNumber;

    public PostgresTypeResolver(boolean decimalsAsNumber) {
        this.decimalsAsNumber = decimalsAsNumber;
    }

    @Override
    public String apply(Class<?> type) {
        if (NUMERIC_TYPES.contains(type)) {
            return "BIGINT";
        } else if (DECIMAL_TYPES.contains(type)) {
            return decimalsAsNumber ? "DECIMAL" : "VARCHAR(500)";
        } else {
            return "TEXT";
        }
    }
}
