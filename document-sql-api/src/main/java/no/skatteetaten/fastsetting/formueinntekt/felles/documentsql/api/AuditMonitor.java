package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;

public interface AuditMonitor {

    default Map<String, Long> summary(LocalDate date) {
        return summary(date, ZoneOffset.UTC);
    }

    default Map<String, Long> summary(LocalDate date, ZoneOffset offset) {
        return summary(date, offset, 0);
    }

    default Map<String, Long> summary(LocalDate date, int range) {
        return summary(date, ZoneOffset.UTC, range);
    }

    default Map<String, Long> summary(LocalDate date, ZoneOffset offset, int range) {
        return summary(date.minusDays(range), date.plusDays(range), offset);
    }

    default Map<String, Long> summary(LocalDate from, LocalDate to) {
        return summary(from, to, ZoneOffset.UTC);
    }

    default Map<String, Long> summary(LocalDate from, LocalDate to, ZoneOffset offset) {
        return summary(OffsetDateTime.of(from.atStartOfDay(), offset), OffsetDateTime.of(to.plusDays(1).atStartOfDay(), offset));
    }

    Map<String, Long> summary(OffsetDateTime from, OffsetDateTime to);
}
