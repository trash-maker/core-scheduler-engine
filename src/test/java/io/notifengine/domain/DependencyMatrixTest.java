package io.notifengine.domain;

import io.notifengine.domain.notification.Dependencies;
import io.notifengine.domain.notification.Dependencies.ChildAction;
import io.notifengine.domain.notification.Dependencies.SeriesAction;
import io.notifengine.domain.notification.Dependencies.SeriesState;
import io.notifengine.domain.rule.Enums.ParentTrigger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static io.notifengine.domain.notification.NotificationStatus.*;
import static org.assertj.core.api.Assertions.assertThat;

class DependencyMatrixTest {

    /** trigger | statuses entered by the parent transition | parent final status | expected child action */
    @ParameterizedTest(name = "{0} on {1} -> {3}")
    @CsvSource(delimiter = '|', value = {
            "ON_FAILED|FAILED RETRY_SCHEDULED|RETRY_SCHEDULED|ACTIVATE",
            "ON_FAILED|FAILED RETRIES_EXHAUSTED|RETRIES_EXHAUSTED|ACTIVATE",
            "ON_FAILED|SUCCEEDED|SUCCEEDED|SKIP",
            "ON_FAILED|PUBLISHED|PUBLISHED|NONE",
            "ON_RETRIES_EXHAUSTED|FAILED RETRY_SCHEDULED|RETRY_SCHEDULED|NONE",
            "ON_RETRIES_EXHAUSTED|FAILED RETRIES_EXHAUSTED|RETRIES_EXHAUSTED|ACTIVATE",
            "ON_RETRIES_EXHAUSTED|SUCCEEDED|SUCCEEDED|SKIP",
            "ON_RETRIES_EXHAUSTED|CANCELLED|CANCELLED|SKIP",
            "ON_SUCCEEDED|SUCCEEDED|SUCCEEDED|ACTIVATE",
            "ON_SUCCEEDED|FAILED RETRIES_EXHAUSTED|RETRIES_EXHAUSTED|SKIP",
            "ON_PUBLISHED|PUBLISHED|PUBLISHED|ACTIVATE",
            "ON_PUBLISHED|SCHEDULED|SCHEDULED|NONE",
            "ON_PUBLISHED|EXPIRED|EXPIRED|SKIP",
            "ON_CANCELLED|CANCELLED|CANCELLED|ACTIVATE",
            "ON_CANCELLED|SUCCEEDED|SUCCEEDED|SKIP",
            "ON_EXPIRED|EXPIRED|EXPIRED|ACTIVATE",
            "ON_EXPIRED|SKIPPED|SKIPPED|SKIP"})
    void childMatrix(ParentTrigger t, String entered, String finalStatus, ChildAction expected) {
        List<io.notifengine.domain.notification.NotificationStatus> e = Arrays.stream(entered.split(" "))
                .map(io.notifengine.domain.notification.NotificationStatus::valueOf).toList();
        assertThat(Dependencies.child(t, e, io.notifengine.domain.notification.NotificationStatus.valueOf(finalStatus))).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0}: {1} -> {2} -> {4}")
    @CsvSource(delimiter = '|', value = {
            "WAITING|PUBLISHED|RETRY_SCHEDULED|false|START",
            "WAITING|PUBLISHED|SUCCEEDED|false|CLOSE_TERMINATED",
            "WAITING|SCHEDULED|PUBLISHED|false|NONE",
            "ACTIVE|RETRY_SCHEDULED|PUBLISHED|false|CLOSE_RECOVERED",
            "ACTIVE|RETRY_SCHEDULED|SUCCEEDED|false|CLOSE_RECOVERED",
            "ACTIVE|RETRY_SCHEDULED|CANCELLED|false|CLOSE_TERMINATED",
            "ACTIVE|RETRY_SCHEDULED|RETRIES_EXHAUSTED|false|NONE",
            "CLOSED|PUBLISHED|RETRY_SCHEDULED|false|START",
            "CLOSED|PUBLISHED|RETRY_SCHEDULED|true|NONE",
            "CLOSED|RETRY_SCHEDULED|RETRIES_EXHAUSTED|false|NONE"})
    void whileInState(SeriesState st, String before, String after, boolean once, SeriesAction expected) {
        var states = EnumSet.of(FAILED, RETRY_SCHEDULED, RETRIES_EXHAUSTED);
        assertThat(Dependencies.series(states, io.notifengine.domain.notification.NotificationStatus.valueOf(before),
                io.notifengine.domain.notification.NotificationStatus.valueOf(after), st, once)).isEqualTo(expected);
    }
}
