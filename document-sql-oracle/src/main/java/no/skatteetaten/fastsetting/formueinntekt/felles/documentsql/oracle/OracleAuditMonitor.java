package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.AuditMonitor;

public class OracleAuditMonitor implements AuditMonitor {

    private final DataSource dataSource;

    private final boolean selfExcluded;

    public OracleAuditMonitor(DataSource dataSource) {
        this.dataSource = dataSource;
        selfExcluded = true;
    }

    public OracleAuditMonitor(DataSource dataSource, boolean selfExcluded) {
        this.dataSource = dataSource;
        this.selfExcluded = selfExcluded;
    }

    @Override
    public Map<String, Long> summary(OffsetDateTime from, OffsetDateTime to) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "SELECT USERNAME, COUNT(*) COUNT "
                + "FROM USER_AUDIT_TRAIL "
                + (selfExcluded ? "WHERE USERNAME != USER AND " : "WHERE ")
                + "TIMESTAMP BETWEEN CAST((FROM_TZ(?, 'UTC') AT TIME ZONE DBTIMEZONE) AS DATE) AND CAST((FROM_TZ(?, 'UTC') AT TIME ZONE DBTIMEZONE) AS DATE) "
                + "GROUP BY USERNAME"
        )) {
            ps.setTimestamp(1, Timestamp.valueOf(from.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()));
            ps.setTimestamp(2, Timestamp.valueOf(to.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()));
            Map<String, Long> results = new HashMap<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.put(rs.getString("USERNAME"), rs.getLong("COUNT"));
                }
            }
            return results;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read audit summary", e);
        }
    }
}
