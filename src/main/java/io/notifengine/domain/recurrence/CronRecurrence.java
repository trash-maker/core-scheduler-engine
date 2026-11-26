package io.notifengine.domain.recurrence;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.zone.ZoneOffsetTransition;
import java.util.BitSet;
import java.util.Locale;
import java.util.Map;

/**
 * Standard 5-field cron: minute hour day-of-month month day-of-week.
 * Supports '*', lists, ranges, steps, month/day names, 'L' (last day of month) in day-of-month,
 * and 0/7 = Sunday. When both day fields are restricted a day matches if either matches (POSIX).
 */
public final class CronRecurrence implements Recurrence {
    private static final int HORIZON_DAYS = 366 * 5;
    private static final Map<String, Integer> MONTHS = Map.ofEntries(
            Map.entry("JAN", 1), Map.entry("FEB", 2), Map.entry("MAR", 3), Map.entry("APR", 4), Map.entry("MAY", 5),
            Map.entry("JUN", 6), Map.entry("JUL", 7), Map.entry("AUG", 8), Map.entry("SEP", 9), Map.entry("OCT", 10),
            Map.entry("NOV", 11), Map.entry("DEC", 12));
    private static final Map<String, Integer> DAYS = Map.of(
            "SUN", 0, "MON", 1, "TUE", 2, "WED", 3, "THU", 4, "FRI", 5, "SAT", 6);

    private final String expression;
    private final ZoneId zone;
    private final BitSet minutes;
    private final BitSet hours;
    private final BitSet doms;
    private final BitSet months;
    private final BitSet dows;
    private final boolean lastDom;
    private final boolean domStar;
    private final boolean dowStar;
    private final int[] minuteList;
    private final int[] hourList;

    public CronRecurrence(String expression, ZoneId zone) {
        this.expression = expression;
        this.zone = zone;
        String[] f = expression.trim().split("\\s+");
        if (f.length != 5) throw new IllegalArgumentException("cron must have 5 fields (minute hour day-of-month month day-of-week)");
        minutes = parseField(f[0], 0, 59, null, "minute");
        hours = parseField(f[1], 0, 23, null, "hour");
        String dom = f[2].toUpperCase(Locale.ROOT);
        boolean l = false;
        if (dom.equals("L")) { l = true; doms = new BitSet(); }
        else if (dom.contains("L")) {
            String rest = dom.replace(",L", "").replace("L,", "");
            if (rest.contains("L")) throw new IllegalArgumentException("day-of-month: 'L' must be alone or a list item");
            l = true;
            doms = parseField(rest, 1, 31, null, "day-of-month");
        } else doms = parseField(dom, 1, 31, null, "day-of-month");
        lastDom = l;
        months = parseField(f[3], 1, 12, MONTHS, "month");
        BitSet d = parseField(f[4], 0, 7, DAYS, "day-of-week");
        if (d.get(7)) { d.set(0); d.clear(7); }
        dows = d;
        domStar = f[2].equals("*") || f[2].equals("?");
        dowStar = f[4].equals("*") || f[4].equals("?");
        minuteList = minutes.stream().toArray();
        hourList = hours.stream().toArray();
        if (minuteList.length == 0 || hourList.length == 0 || months.isEmpty() || (doms.isEmpty() && !lastDom && !domStar) )
            throw new IllegalArgumentException("cron matches nothing");
    }

    public String expression() { return expression; }

    private static BitSet parseField(String field, int min, int max, Map<String, Integer> names, String label) {
        BitSet bits = new BitSet();
        for (String part : field.toUpperCase(Locale.ROOT).split(",")) {
            if (part.isEmpty()) throw new IllegalArgumentException(label + ": empty list item");
            int step = 1;
            String range = part;
            int slash = part.indexOf('/');
            if (slash >= 0) {
                step = parseInt(part.substring(slash + 1), label);
                if (step <= 0) throw new IllegalArgumentException(label + ": step must be > 0");
                range = part.substring(0, slash);
            }
            int lo, hi;
            if (range.equals("*") || range.equals("?")) { lo = min; hi = max; }
            else if (range.contains("-")) {
                String[] r = range.split("-", 2);
                lo = value(r[0], names, label); hi = value(r[1], names, label);
            } else {
                lo = value(range, names, label);
                hi = slash >= 0 ? max : lo;
            }
            if (lo < min || hi > max || lo > hi) throw new IllegalArgumentException(label + ": value out of range " + min + "-" + max + " in '" + part + "'");
            for (int i = lo; i <= hi; i += step) bits.set(i);
        }
        return bits;
    }

    private static int value(String s, Map<String, Integer> names, String label) {
        if (names != null && names.containsKey(s)) return names.get(s);
        return parseInt(s, label);
    }

    private static int parseInt(String s, String label) {
        try { return Integer.parseInt(s); } catch (NumberFormatException e) { throw new IllegalArgumentException(label + ": not a number '" + s + "'"); }
    }

    boolean matchesDate(LocalDate d) {
        if (!months.get(d.getMonthValue())) return false;
        boolean domMatch = doms.get(d.getDayOfMonth()) || (lastDom && d.getDayOfMonth() == d.lengthOfMonth());
        int dow = d.getDayOfWeek() == DayOfWeek.SUNDAY ? 0 : d.getDayOfWeek().getValue();
        boolean dowMatch = dows.get(dow);
        if (domStar && dowStar) return true;
        if (domStar) return dowMatch;
        if (dowStar) return domMatch;
        return domMatch || dowMatch;
    }

    private ZonedDateTime resolve(LocalDateTime local) {
        var rules = zone.getRules();
        if (rules.getValidOffsets(local).isEmpty()) {
            ZoneOffsetTransition tr = rules.getTransition(local);
            return tr.getInstant().atZone(zone);
        }
        return ZonedDateTime.ofLocal(local, zone, null);
    }

    @Override
    public ZonedDateTime next(ZonedDateTime after) {
        ZonedDateTime a = after.withZoneSameInstant(zone);
        LocalDateTime startLocal = a.toLocalDateTime().truncatedTo(ChronoUnit.MINUTES);
        LocalDate day = startLocal.toLocalDate().minusDays(1); // gap/overlap safety
        for (int i = 0; i < HORIZON_DAYS; i++, day = day.plusDays(1)) {
            if (!matchesDate(day)) continue;
            for (int h : hourList) {
                for (int m : minuteList) {
                    LocalDateTime cand = day.atTime(h, m);
                    if (cand.isBefore(startLocal.minusHours(3))) continue;
                    ZonedDateTime z = resolve(cand);
                    if (z.toInstant().isAfter(after.toInstant())) return z;
                }
            }
        }
        return null;
    }

    @Override
    public ZonedDateTime previousOrSame(ZonedDateTime at) {
        ZonedDateTime a = at.withZoneSameInstant(zone);
        LocalDateTime startLocal = a.toLocalDateTime();
        LocalDate day = startLocal.toLocalDate().plusDays(1);
        for (int i = 0; i < HORIZON_DAYS; i++, day = day.minusDays(1)) {
            if (!matchesDate(day)) continue;
            for (int hi = hourList.length - 1; hi >= 0; hi--) {
                for (int mi = minuteList.length - 1; mi >= 0; mi--) {
                    LocalDateTime cand = day.atTime(hourList[hi], minuteList[mi]);
                    if (cand.isAfter(startLocal.plusHours(3))) continue;
                    ZonedDateTime z = resolve(cand);
                    if (!z.toInstant().isAfter(at.toInstant())) return z;
                }
            }
        }
        return null;
    }

    @Override
    public String toString() { return "cron(" + expression + ", " + zone + ")"; }
}
