package ca.on.oicr.gsi.cerberus;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

/**
 * Consumes joined records
 *
 * @param <T> the type of the resulting records
 */
public interface JoinSink<T> {

  /**
   * Dispatch a records to multiple sinks
   *
   * @param sinks the sinks to combine
   * @param <T> the type of the record that can be consumed by all the sinks
   * @return a sink that will dispatch to all the provided sinks
   */
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

  /**
   * Create a sink using the Java collector interface
   *
   * @param collector the collector to use
   * @param consumer the consumer of the final value
   * @param <T> the type of the joined record
   * @param <A> an intermediate collector type
   * @param <R> the result type from the collector
   * @return a sink that will process records using the collector and send the final value to the
   *     consumer provided
   */
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

  /**
   * Transform a value as it is being consumed
   *
   * @param sink the consumer of the transformed value
   * @param transformation the transformation to apply to each value
   * @param <T> the input type
   * @param <R> the transformed type
   * @return a sink that will forward transformed values to the provied sink
   */
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

  /**
   * Consume a record
   *
   * @param item the record to be consumed
   */
  void accept(T item);
  /** No more records are available */
  void finished();
}
