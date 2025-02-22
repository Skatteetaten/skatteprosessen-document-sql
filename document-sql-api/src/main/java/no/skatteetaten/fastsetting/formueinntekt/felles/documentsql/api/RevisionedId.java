package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

public class RevisionedId {

    private final String id;
    private final long revision;
    private final boolean deleted;

    public RevisionedId(String id, long revision, boolean deleted) {
        this.id = id;
        this.revision = revision;
        this.deleted = deleted;
    }

    public String getId() {
        return id;
    }

    public long getRevision() {
        return revision;
    }

    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        RevisionedId that = (RevisionedId) object;
        if (revision != that.revision) {
            return false;
        }
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (int) (revision ^ (revision >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return id + "@" + revision;
    }
}
