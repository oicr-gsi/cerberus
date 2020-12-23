package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.cerberus.UpdateResult.Visitor;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface IncrementalJoinSource<T> {
  static <T> JoinSource<T> accumulating(IncrementalJoinSource<T> source) {
    return new JoinSource<>() {
      private List<T> state = List.of();

      @Override
      public Stream<T> fetch() {
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

  static <T, K> JoinSource<T> evicting(IncrementalJoinSource<T> source, Function<T, K> key) {
    return new JoinSource<>() {
      private List<T> state = List.of();

      @Override
      public Stream<T> fetch() {
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

  static <T extends Comparable<T>, K> JoinSource<T> evictingWithComparison(
      IncrementalJoinSource<T> source, Function<T, K> key) {
    return evictingWithComparison(source, key, Comparator.naturalOrder());
  }

  static <T, K> JoinSource<T> evictingWithComparison(
      IncrementalJoinSource<T> source, Function<T, K> key, Comparator<T> comparator) {
    return new JoinSource<>() {
      private List<T> state = List.of();

      @Override
      public Stream<T> fetch() {
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

  UpdateResult<T> update();
}
