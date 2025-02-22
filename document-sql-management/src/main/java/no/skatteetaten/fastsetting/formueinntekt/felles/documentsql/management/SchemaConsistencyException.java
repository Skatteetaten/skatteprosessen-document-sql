package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.management;

class SchemaConsistencyException extends RuntimeException {

    private final int length;
    private final boolean recreation;

    SchemaConsistencyException(String name, int length) {
        super("Forced recreation for " + name);
        this.length = length;
        recreation = true;
    }

    SchemaConsistencyException(String name, String sort, Object value, int length) {
        super("Inconsistent " + sort + " '" + value + "' for " + name);
        this.length = length;
        recreation = false;
    }

    int getLength() {
        return length;
    }

    boolean isRecreation() {
        return recreation;
    }
}
