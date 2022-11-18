package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CapitalizingNameResolver implements NameResolver {

    private static final String NO_NAME_COLUMN = "VALUE";

    public static final int ORA_MAX_LENGTH = 25; // five characters are reserved for suffixing.

    private final int maxLength;

    public CapitalizingNameResolver() {
        this(ORA_MAX_LENGTH);
    }

    public CapitalizingNameResolver(int maxLength) {
        if (maxLength < NO_NAME_COLUMN.length()) {
            throw new IllegalArgumentException("Length must be at least: " + NO_NAME_COLUMN.length());
        }
        this.maxLength = maxLength;
    }

    @Override
    public String resolve(List<String> elements, Predicate<String> isReserved) {
        List<String> identifiers = elements.stream()
            .map(element -> element.replaceAll("([a-z])([A-Z]+)", "$1_$2").toUpperCase(Locale.US))
            .map(element -> element.replaceAll("[^a-zA-Z0-9_]", "_"))
            .map(element -> element.startsWith("_") && element.length() > 1 ? element.substring(1) : element)
            .collect(Collectors.toList());
        List<String> retained = new ArrayList<>();
        for (int length = 0, index = identifiers.size() - 1; index >= 0; index--) {
            String identifier = identifiers.get(index);
            if (length + identifier.length() < maxLength) {
                retained.add(0, identifier);
                length += identifier.length() + 1;
            } else {
                String abbreviation = toAbbreviation(elements.get(index));
                if (length + abbreviation.length() < maxLength) {
                    retained.add(0, abbreviation);
                    length += abbreviation.length() + 1;
                } else {
                    if (index > 0) {
                        abbreviation = toAbbreviation(elements.get(0));
                    }
                    do {
                        length -= retained.remove(0).length() + 1;
                    } while (length + abbreviation.length() > maxLength - 1);
                    if (retained.isEmpty()) {
                        if (identifiers.get(identifiers.size() - 1).length() < maxLength) {
                            retained.add(identifiers.get(identifiers.size() - 1));
                        } else {
                            throw new IllegalArgumentException("Cannot generate unique name for " + elements);
                        }
                    } else if (length + identifiers.get(0).length() < maxLength) {
                        retained.add(0, identifiers.get(0));
                    } else {
                        retained.add(0, abbreviation);
                    }
                    break;
                }
            }
        }
        String name;
        if (retained.isEmpty()) {
            if (identifiers.isEmpty()) {
                name = NO_NAME_COLUMN;
            } else {
                String identifier = identifiers.get(identifiers.size() - 1);
                name = identifier.substring(0, Math.min(identifier.length(), maxLength));
            }
        } else {
            name = String.join("_", retained);
        }
        int increment = 0;
        String adjusted = name;
        while (isReserved.test(adjusted)) {
            adjusted = (name.length() > maxLength - 3
                ? name.substring(0, name.length() - 3)
                : name) + "_" + increment++;
        }
        return adjusted;
    }

    static String toAbbreviation(String value) {
        StringBuilder abbreviation = new StringBuilder().append(Character.toUpperCase(value.charAt(0)));
        boolean lastUpper = true;
        for (int index = 1; index < value.length(); index++) {
            if (Character.isDigit(value.charAt(index))) {
                abbreviation.append(value.charAt(index));
            } else if (!lastUpper && Character.isUpperCase(value.charAt(index))) {
                abbreviation.append(value.charAt(index));
                lastUpper = true;
            } else if (value.charAt(index) == '_' || Character.isLowerCase(value.charAt(index))) {
                lastUpper = false;
            }
        }
        return abbreviation.toString();
    }
}
