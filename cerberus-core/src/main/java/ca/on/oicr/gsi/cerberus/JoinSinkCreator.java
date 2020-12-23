package ca.on.oicr.gsi.cerberus;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface JoinSinkCreator<L, R> {
  static <L, R, T> JoinSinkCreator<L, R> expandLeft(
      JoinSinkCreator<T, R> sinkCreator, Function<? super L, Stream<? extends T>> children) {
    return left ->
        new JoinSink<>() {
          private final List<JoinSink<R>> sinks =
              children.apply(left).map(sinkCreator::create).collect(Collectors.toList());

          @Override
          public void accept(R item) {
            for (final var sink : sinks) {
              sink.accept(item);
            }
          }

          @Override
          public void finished() {
            for (final var sink : sinks) {
              sink.finished();
            }
          }
        };
  }

  JoinSink<R> create(L left);
}
