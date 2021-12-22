package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

public interface AuditMonitor {

    default Map<String, Long> summary(LocalDate date) {
        return summary(date, 0);
    }

    default Map<String, Long> summary(LocalDate date, int range) {
        return summary(date.minusDays(range), date.plusDays(range));
    }

    default Map<String, Long> summary(LocalDate from, LocalDate to) {
        return summary(from.atStartOfDay(), to.plusDays(1).atStartOfDay());
    }

    Map<String, Long> summary(LocalDateTime from, LocalDateTime to);
}
