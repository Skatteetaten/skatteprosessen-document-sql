package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.management;

public class UnknownSchemaException extends IllegalArgumentException {

    UnknownSchemaException(String schema) {
        super("Unknown schema: " + schema);
    }
}
