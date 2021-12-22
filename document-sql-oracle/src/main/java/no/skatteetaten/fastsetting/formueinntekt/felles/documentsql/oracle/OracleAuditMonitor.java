package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.AuditMonitor;

public class OracleAuditMonitor implements AuditMonitor  {

    private final DataSource dataSource;

    public OracleAuditMonitor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Map<String, Long> summary(LocalDateTime from, LocalDateTime to) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "SELECT USERNAME, COUNT(*) COUNT "
                    + "FROM USER_AUDIT_TRAIL "
                    + "WHERE USERNAME != USER "
                    + "AND TIMESTAMP BETWEEN ? AND ? "
                    + "GROUP BY USERNAME"
        )) {
            ps.setTimestamp(1, Timestamp.from(from.toInstant(ZoneOffset.UTC)));
            ps.setTimestamp(2, Timestamp.from(to.toInstant(ZoneOffset.UTC)));
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
