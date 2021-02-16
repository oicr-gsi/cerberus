package ca.on.oicr.gsi.cerberus;

import java.util.List;

/**
 * The result of an incremental fetch operation
 *
 * @param <T> the type of data being fetch
 */
public abstract class UpdateResult<T> {

  /**
   * Transform the update operation into a value
   *
   * @param <T> the type of records being fetch
   * @param <R> the type of the resulting value
   */
  public interface Visitor<T, R> {

    /**
     * Handle a successful incremental fetch
     *
     * @param items the additional items fetched
     */
    R incremental(List<T> items);

    /**
     * Handle a fetch that indicates all historic values should be removed
     *
     * @param items the new items to replace the cache's content
     */
    R restart(List<T> items);
  }

  /**
   * Handle a successful incremental fetch
   *
   * @param items the additional items fetched
   */
  public static <T> UpdateResult<T> incremental(List<T> items) {
    return new UpdateResult<>() {

      @Override
      public <R> R visit(Visitor<T, R> visitor) {
        return visitor.incremental(items);
      }
    };
  }

  /**
   * Handle a fetch that indicates all historic values should be removed
   *
   * @param items the new items to replace the cache's content
   */
  public static <T> UpdateResult<T> restart(List<T> items) {
    return new UpdateResult<T>() {

      @Override
      public <R> R visit(Visitor<T, R> visitor) {
        return visitor.restart(items);
      }
    };
  }

  private UpdateResult() {}

  /**
   * Convert the incremental operation to a new value
   *
   * @param visitor the converter to use
   * @param <R> the result type of the conversion
   * @return the converted result
   */
  public abstract <R> R visit(Visitor<T, R> visitor);
}
