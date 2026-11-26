package trash_maker.domain.rule;

import trash_maker.common.IsoAmount;

/** End of a series: either an absolute {@link TimeSpec} or an amount after the series start. */
public record UntilSpec(TimeSpec at, IsoAmount afterStart) {}
