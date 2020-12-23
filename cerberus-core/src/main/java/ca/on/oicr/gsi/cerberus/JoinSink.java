package ca.on.oicr.gsi.cerberus;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public interface JoinSink<T> {
  @SafeVarargs
  static <T> JoinSink<T> all(JoinSink<? super T>... sinks) {
    return new JoinSink<>() {
      @Override
      public void accept(T item) {
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

  static <T, A, R> JoinSink<T> of(Collector<T, A, R> collector, Consumer<? super R> consumer) {
    return new JoinSink<>() {
      private final BiConsumer<A, T> accumulator = collector.accumulator();
      private final Function<A, R> finisher = collector.finisher();
      private final A state = collector.supplier().get();

      @Override
      public void accept(T item) {
        accumulator.accept(state, item);
      }

      @Override
      public void finished() {
        consumer.accept(finisher.apply(state));
      }
    };
  }

  static <T, R> JoinSink<T> map(JoinSink<R> sink, Function<? super T, ? extends R> transformation) {
    return new JoinSink<>() {

      @Override
      public void accept(T item) {
        sink.accept(transformation.apply(item));
      }

      @Override
      public void finished() {
        sink.finished();
      }
    };
  }

  void accept(T item);

  void finished();
}
