package ca.on.oicr.gsi.cerberus;

/**
 * Start joining on a particular value on the left-hand side of a join
 *
 * @param <L> the left-hand type
 * @param <R> the right hand type
 */
public interface JoinSinkCreator<L, R> {

  /**
   * Begin consumption of right hand records for a particular right-hand value
   *
   * @param left the left-hand value being consumed
   * @return a consumer of the right-hand values
   */
  JoinSink<R> create(L left);
}
