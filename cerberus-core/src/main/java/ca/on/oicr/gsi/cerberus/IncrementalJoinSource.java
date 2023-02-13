package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.cerberus.UpdateResult.Visitor;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A join sink that can be reused to incrementally download data from a remote source while keeping
 * previously returned records
 *
 * @param <T> the type of records produced
 */
public interface IncrementalJoinSource<T> {

  /**
   * Create a join source that can collect records with no replacement
   *
   * @param source the incremental join source that provides records
   * @param <T> the type of records being provided
   * @return a join source that will attempt an incremental fetch when it is polled
   */
  static <T> JoinSource<T> accumulating(IncrementalJoinSource<T> source) {
    return new JoinSource<>() {
      private List<T> state = List.of();

      @Override
      public Stream<T> fetch() throws Exception {
        state =
            source
                .update()
                .visit(
                    new Visitor<>() {
                      @Override
                      public List<T> incremental(List<T> items) {
                        return Stream.concat(state.stream(), items.stream())
                            .collect(Collectors.toList());
                      }

                      @Override
                      public List<T> restart(List<T> items) {
                        return items;
                      }
                    });
        return state.stream();
      }
    };
  }

  /**
   * An incremental join source that can remove "duplicate" old records
   *
   * <p>All old records with a key found in the new records will be evicted from the historic set.
   * If the same key is used multiple times, all records with that key in the historic set will be
   * evicted, but all incrementally fetch records will be kept.
   *
   * @param source the incremental join source that provides records
   * @param key a function to determine a key for each record
   * @param <T> the type of records being provided
   * @param <K> the type of the key
   * @return a join source that will attempt an incremental fetch and expunge when it is polled
   */
  static <T, K> JoinSource<T> evicting(IncrementalJoinSource<T> source, Function<T, K> key) {
    return new JoinSource<>() {
      private List<T> state = List.of();

      @Override
      public Stream<T> fetch() throws Exception {
        state =
            source
                .update()
                .visit(
                    new Visitor<>() {
                      @Override
                      public List<T> incremental(List<T> items) {
                        final var evicted = items.stream().map(key).collect(Collectors.toSet());
                        return Stream.concat(
                                state.stream().filter(item -> !evicted.contains(key.apply(item))),
                                items.stream())
                            .collect(Collectors.toList());
                      }

                      @Override
                      public List<T> restart(List<T> items) {
                        return items;
                      }
                    });
        return state.stream();
      }
    };
  }

  /**
   * Incremental fetch records with the ability to remove duplicates by picking the best records
   *
   * <p>Selects based on the natural order of the values
   *
   * @param source the incremental join source that provides records
   * @param key a function to determine a key for each record
   * @param <T> the type of records being provided
   * @param <K> the type of the key
   * @return a join source that will attempt an incremental fetch and expunge when it is polled
   */
  static <T extends Comparable<T>, K> JoinSource<T> evictingWithComparison(
      IncrementalJoinSource<T> source, Function<T, K> key) {
    return evictingWithComparison(source, key, Comparator.naturalOrder());
  }

  /**
   * Incremental fetch records with the ability to remove duplicates by picking the best records
   *
   * @param source the incremental join source that provides records
   * @param key a function to determine a key for each record
   * @param comparator a comparator to decide which record should be selected during comparison
   * @param <T> the type of records being provided
   * @param <K> the type of the key
   * @return a join source that will attempt an incremental fetch and expunge when it is polled
   */
  static <T, K> JoinSource<T> evictingWithComparison(
      IncrementalJoinSource<T> source, Function<T, K> key, Comparator<T> comparator) {
    return new JoinSource<>() {
      private List<T> state = List.of();

      @Override
      public Stream<T> fetch() throws Exception {
        state =
            source
                .update()
                .visit(
                    new Visitor<>() {
                      @Override
                      public List<T> incremental(List<T> items) {
                        return Stream.concat(state.stream(), items.stream())
                            .collect(Collectors.groupingBy(key, Collectors.maxBy(comparator)))
                            .values()
                            .stream()
                            .flatMap(Optional::stream)
                            .collect(Collectors.toList());
                      }

                      @Override
                      public List<T> restart(List<T> items) {
                        return items;
                      }
                    });
        return state.stream();
      }
    };
  }

  /**
   * Perform an incremental fetch
   *
   * @return the resulting data
   */
  UpdateResult<T> update() throws Exception;
}
