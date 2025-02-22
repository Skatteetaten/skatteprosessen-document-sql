package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.container;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.TableResolver;

public class SampleContainerResolver implements TableResolver<SampleContainer> {

    @Override
    public String toPayload(SampleContainer value) {
        return value.payload;
    }

    @Override
    public void registerAdditionalValues(int index, PreparedStatement ps, SampleContainer value) throws SQLException {
        ps.setString(index, value.tag);
    }

    @Override
    public Map<String, String> getAdditionalColumns() {
        return Collections.singletonMap("TAG", "VARCHAR(50) NOT NULL");
    }
}
