package trash_maker.domain.retention;

import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Pure retention rules for monthly partitions named notification_pYYYY_MM. */
public final class RetentionPolicy {
    private static final Pattern PART = Pattern.compile("notification_p(\\d{4})_(\\d{2})");

    private RetentionPolicy() {}

    /** True when the partition ends before the cutoff (it may then be dropped if it holds no active row). */
    public static boolean expired(String partition, Instant cutoff) {
        Matcher m = PART.matcher(partition);
        if (!m.matches()) return false;
        YearMonth ym = YearMonth.of(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
        return ym.plusMonths(1).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC).isBefore(cutoff);
    }

    public static String historyPartition(String notificationPartition) {
        return notificationPartition.replace("notification_p", "notification_history_p");
    }
}
