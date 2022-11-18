package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle;

import javax.sql.DataSource;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.SizeMonitor;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OracleSizeMonitor implements SizeMonitor {

    private final DataSource dataSource;

    private final int bulk;

    public OracleSizeMonitor(DataSource dataSource, int bulk) {
        this.dataSource = dataSource;
        this.bulk = bulk;
    }

    @Override
    public Map<String, BigInteger> summary(Map<String, String> groups) {
        if (groups.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, BigInteger> result = new HashMap<>();
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
            "WITH "
                + "RELEVANT_GROUPS (OBJECT_GROUP) AS ("
                + String.join(" UNION ALL ", Collections.nCopies(bulk, "SELECT ? FROM DUAL"))
                + "), "
                + "RELEVANT_TABLES (OBJECT_NAME, OBJECT_GROUP) AS ("
                + "SELECT OBJECT_GROUP || '_RAW', OBJECT_GROUP FROM RELEVANT_GROUPS WHERE OBJECT_GROUP IS NOT NULL "
                + "UNION ALL "
                + "SELECT OBJECT_GROUP || '_PTL', OBJECT_GROUP FROM RELEVANT_GROUPS WHERE OBJECT_GROUP IS NOT NULL "
                + "UNION ALL "
                + "SELECT NAME || '_GPI', OBJECT_GROUP FROM RELEVANT_GROUPS INNER JOIN USER_DEPENDENCIES ON REFERENCED_NAME = OBJECT_GROUP || '_IDX' WHERE OBJECT_GROUP IS NOT NULL"
                + "), "
                + "RELEVANT_OBJECTS (OBJECT_NAME, OBJECT_GROUP) AS ("
                + "SELECT OBJECT_NAME, OBJECT_GROUP FROM RELEVANT_TABLES "
                + "UNION ALL "
                + "SELECT INDEX_NAME, OBJECT_GROUP FROM RELEVANT_TABLES INNER JOIN USER_INDEXES ON TABLE_NAME = RELEVANT_TABLES.OBJECT_NAME"
                + ") "
                + "SELECT OBJECT_GROUP, SUM(BYTES) GROUP_SIZE "
                + "FROM RELEVANT_OBJECTS "
                + "INNER JOIN USER_SEGMENTS ON SEGMENT_NAME = OBJECT_NAME "
                + "GROUP BY OBJECT_GROUP"
        )) {
            int count = 0;
            for (String group : groups.keySet()) {
                if (count == bulk) {
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            result.merge(
                                groups.get(rs.getString("OBJECT_GROUP")),
                                rs.getBigDecimal("GROUP_SIZE").toBigInteger(),
                                BigInteger::add
                            );
                        }
                    }
                    count = 0;
                }
                ps.setString(++count, group);
            }
            if (count > 0) {
                while (count < bulk) {
                    ps.setNull(++count, Types.VARCHAR);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.merge(
                            groups.get(rs.getString("OBJECT_GROUP")),
                            rs.getBigDecimal("GROUP_SIZE").toBigInteger(),
                            BigInteger::add
                        );
                    }
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not create size summary", e);
        }
        return result;
    }
}
