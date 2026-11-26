package trash_maker.domain.expr;

/**
 * How a leaf with a MISSING signal (never received / stale) evaluates.
 * FALSE (default): the leaf is false. TRUE: the leaf is true.
 * UNKNOWN: Kleene three-valued logic up the tree; an unknown overall result counts as false.
 */
public enum OnMissing { FALSE, TRUE, UNKNOWN }
