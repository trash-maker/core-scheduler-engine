package trash_maker.domain;

import trash_maker.domain.notification.Effect;
import trash_maker.domain.notification.Event;
import trash_maker.domain.notification.IllegalTransitionException;
import trash_maker.domain.notification.NotificationState;
import trash_maker.domain.notification.NotificationStatus;
import trash_maker.domain.notification.OutcomeType;
import trash_maker.domain.notification.StateMachine;
import trash_maker.domain.notification.TimerKind;
import trash_maker.domain.notification.Transition;
import trash_maker.domain.rule.Enums;
import trash_maker.domain.rule.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static trash_maker.domain.notification.NotificationStatus.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StateMachineTest {
    static final Instant T0 = Instant.parse("2024-05-01T10:00:00Z");
    static final RetryPolicy RETRY3 = new RetryPolicy(3, Enums.BackoffKind.EXPONENTIAL, Duration.ofMinutes(1), 2.0, Duration.ofMinutes(30), 0, Duration.ofMinutes(2));
    final StateMachine sm = new StateMachine(() -> 0.5);

    static NotificationState state(NotificationStatus s, int attempt, int cycle, RetryPolicy p, Instant due, Instant expires) {
        return new NotificationState(UUID.randomUUID(), s, null, attempt, cycle, p, due, expires, null, false, null, null);
    }

    @Test
    void immediatePublishesOnStart() {
        Transition t = sm.apply(state(CREATED, 0, 0, RETRY3, T0, null), new Event.Start(), T0);
        assertThat(t.state().status()).isEqualTo(PUBLISHED);
        assertThat(t.state().attempt()).isEqualTo(1);
        assertThat(t.effects()).containsExactly(new Effect.Emit(1, Duration.ofMinutes(2)));
    }

    @Test
    void scheduledWaitsForTimer() {
        Transition t = sm.apply(state(CREATED, 0, 0, RETRY3, T0.plusSeconds(60), T0.plusSeconds(3600)), new Event.Start(), T0);
        assertThat(t.state().status()).isEqualTo(SCHEDULED);
        assertThat(t.effects()).containsExactlyInAnyOrder(new Effect.ScheduleTimer(TimerKind.PUBLISH, T0.plusSeconds(60)),
                new Effect.ScheduleTimer(TimerKind.EXPIRE, T0.plusSeconds(3600)));
    }

    @Test
    void retryThenExhaustion() {
        NotificationState s = sm.apply(state(CREATED, 0, 0, RETRY3, T0, null), new Event.Start(), T0).state();
        Transition f1 = sm.apply(s, new Event.Outcome(1, OutcomeType.FAILURE, "smtp 451", T0), T0);
        assertThat(f1.entered()).containsExactly(FAILED, RETRY_SCHEDULED);
        assertThat(f1.effects()).contains(new Effect.ScheduleTimer(TimerKind.PUBLISH, T0.plus(Duration.ofMinutes(1))));
        NotificationState r1 = sm.apply(f1.state(), new Event.PublishDue(), T0.plusSeconds(60)).state();
        assertThat(r1.attempt()).isEqualTo(2);
        Transition f2 = sm.apply(r1, new Event.AttemptTimeout(2), T0.plusSeconds(180));
        assertThat(f2.state().lastFailure().reason()).isEqualTo("TIMEOUT");
        assertThat(f2.effects()).contains(new Effect.ScheduleTimer(TimerKind.PUBLISH, T0.plusSeconds(180).plus(Duration.ofMinutes(2))));
        NotificationState r2 = sm.apply(f2.state(), new Event.PublishDue(), T0.plusSeconds(300)).state();
        Transition f3 = sm.apply(r2, new Event.Outcome(3, OutcomeType.FAILURE, "x", T0), T0.plusSeconds(301));
        assertThat(f3.entered()).containsExactly(FAILED, RETRIES_EXHAUSTED);
    }

    @Test
    void permanentFailureSkipsRetries() {
        NotificationState s = sm.apply(state(CREATED, 0, 0, RETRY3, T0, null), new Event.Start(), T0).state();
        assertThat(sm.apply(s, new Event.Outcome(1, OutcomeType.PERMANENT_FAILURE, "bad address", T0), T0).state().status()).isEqualTo(RETRIES_EXHAUSTED);
    }

    @Test
    void lateSuccessAcceptedLateFailureIgnored() {
        NotificationState s = sm.apply(state(CREATED, 0, 0, RETRY3, T0, null), new Event.Start(), T0).state();
        NotificationState retry = sm.apply(s, new Event.AttemptTimeout(1), T0.plusSeconds(120)).state();
        assertThat(retry.status()).isEqualTo(RETRY_SCHEDULED);
        // late failure for attempt 1: ignored
        assertThat(sm.apply(retry, new Event.Outcome(1, OutcomeType.FAILURE, "x", T0), T0.plusSeconds(121)).isIgnored()).isTrue();
        // late success for attempt 1: accepted, retry timer cancelled
        Transition ok = sm.apply(retry, new Event.Outcome(1, OutcomeType.SUCCESS, null, T0), T0.plusSeconds(122));
        assertThat(ok.state().status()).isEqualTo(SUCCEEDED);
        assertThat(ok.effects()).anyMatch(e -> e instanceof Effect.CancelTimers);
        // duplicate success ignored
        assertThat(sm.apply(ok.state(), new Event.Outcome(1, OutcomeType.SUCCESS, null, T0), T0).isIgnored()).isTrue();
        // unknown attempt ignored
        assertThat(sm.apply(s, new Event.Outcome(5, OutcomeType.SUCCESS, null, T0), T0).isIgnored()).isTrue();
    }

    @Test
    void noRetryPastExpiry() {
        NotificationState s = sm.apply(state(CREATED, 0, 0, RETRY3, T0, T0.plusSeconds(30)), new Event.Start(), T0).state();
        Transition f = sm.apply(s, new Event.Outcome(1, OutcomeType.FAILURE, "x", T0), T0.plusSeconds(5));
        assertThat(f.state().status()).isEqualTo(RETRIES_EXHAUSTED);
    }

    @Test
    void expiryAndStaleTimers() {
        NotificationState sch = sm.apply(state(CREATED, 0, 0, RETRY3, T0.plusSeconds(600), T0.plusSeconds(300)), new Event.Start(), T0).state();
        assertThat(sm.apply(sch, new Event.Expire(), T0.plusSeconds(300)).state().status()).isEqualTo(EXPIRED);
        assertThat(sm.apply(sch, new Event.Expire(), T0.plusSeconds(10)).isIgnored()).isTrue();
        NotificationState pub = sm.apply(state(CREATED, 0, 0, RETRY3, T0, null), new Event.Start(), T0).state();
        assertThat(sm.apply(pub, new Event.PublishDue(), T0).isIgnored()).isTrue();
        assertThat(sm.apply(pub, new Event.AttemptTimeout(0), T0).isIgnored()).isTrue();
    }

    @Test
    void retriggerResetsCycleButKeepsMonotonicAttempt() {
        NotificationState s = sm.apply(state(CREATED, 0, 0, RetryPolicy.DEFAULT, T0, null), new Event.Start(), T0).state();
        NotificationState ex = sm.apply(s, new Event.Outcome(1, OutcomeType.FAILURE, "x", T0), T0).state();
        assertThat(ex.status()).isEqualTo(RETRIES_EXHAUSTED);
        NotificationState again = sm.apply(ex, new Event.Retrigger("ops"), T0.plusSeconds(10)).state();
        assertThat(again.status()).isEqualTo(PUBLISHED);
        assertThat(again.attempt()).isEqualTo(2);
        assertThat(again.cycleAttempts()).isEqualTo(1);
        assertThatThrownBy(() -> sm.apply(again, new Event.Retrigger("ops"), T0)).isInstanceOf(IllegalTransitionException.class);
    }

    @ParameterizedTest
    @EnumSource(value = NotificationStatus.class, names = {"SUCCEEDED", "RETRIES_EXHAUSTED", "CANCELLED", "EXPIRED", "SKIPPED"})
    void cancelRejectedInTerminalStates(NotificationStatus s) {
        assertThatThrownBy(() -> sm.apply(state(s, 1, 1, RETRY3, T0, null), new Event.Cancel("x"), T0)).isInstanceOf(IllegalTransitionException.class);
    }

    @Test
    void legalTransitionTable() {
        assertThat(StateMachine.isLegal(SUCCEEDED, PUBLISHED)).isFalse();
        assertThat(StateMachine.isLegal(SKIPPED, CREATED)).isFalse();
        assertThat(StateMachine.isLegal(WAITING_ON_PARENT, PUBLISHED)).isFalse();
        assertThat(StateMachine.isLegal(PUBLISHED, RETRY_SCHEDULED)).isFalse(); // must go through FAILED
        assertThat(StateMachine.isLegal(RETRIES_EXHAUSTED, SUCCEEDED)).isTrue();
    }

    @Test
    void backoff() {
        RetryPolicy p = new RetryPolicy(10, Enums.BackoffKind.EXPONENTIAL, Duration.ofSeconds(10), 3.0, Duration.ofSeconds(100), 0, null);
        assertThat(p.delay(1, 0.5)).isEqualTo(Duration.ofSeconds(10));
        assertThat(p.delay(2, 0.5)).isEqualTo(Duration.ofSeconds(30));
        assertThat(p.delay(3, 0.5)).isEqualTo(Duration.ofSeconds(90));
        assertThat(p.delay(4, 0.5)).isEqualTo(Duration.ofSeconds(100));
        RetryPolicy j = new RetryPolicy(10, Enums.BackoffKind.FIXED, Duration.ofSeconds(10), 1, Duration.ofSeconds(100), 0.2, null);
        assertThat(j.delay(5, 0.0)).isEqualTo(Duration.ofSeconds(8));
        assertThat(j.delay(5, 1.0)).isEqualTo(Duration.ofSeconds(12));
    }
}
