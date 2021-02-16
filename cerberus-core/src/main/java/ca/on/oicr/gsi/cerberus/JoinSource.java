package ca.on.oicr.gsi.cerberus;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Read records as required for joining
 *
 * @param <T> the type of record being consumed
 */
public interface JoinSource<T> {

  /**
   * Combine multiple sources of information
   *
   * @param sources the sources to combine
   * @param <T> the resulting type that can be produced by all the sources
   * @return a source that aggregates the results from all the provided sources
   */
  @SafeVarargs
  static <T> JoinSource<T> all(JoinSource<? extends T>... sources) {
    return () -> Stream.of(sources).flatMap(JoinSource::fetch);
  }

  /**
   * Combine multiple sources of information
   *
   * @param sources the sources to combine
   * @param <T> the resulting type that can be produced by all the sources
   * @return a source that aggregates the results from all the provided sources
   */
  static <T> JoinSource<T> all(Stream<JoinSource<? extends T>> sources) {
    final var s = sources.collect(Collectors.toList());
    return () -> s.stream().flatMap(JoinSource::fetch);
  }

  /**
   * A join source that produces no records
   *
   * @param <T> the type of record being produced
   */
  static <T> JoinSource<T> empty() {
    return Stream::empty;
  }

  /**
   * A join source that masks exceptions to a limited degree
   *
   * <p>The source will serve the previous result if an error is throw up to a maximum failure limit
   *
   * @param source the join source to draw data from
   * @param maxConsecutiveFailures the maximum number of consecutive errors the cache will tolerate
   * @param maxInterval the maximum length of time the cache can be considered fresh
   * @param <T> the type of record being produced
   */
  static <T> JoinSource<T> errorResilient(
      JoinSource<T> source, OptionalInt maxConsecutiveFailures, Optional<Duration> maxInterval) {
    return new JoinSource<T>() {
      private List<T> cache = List.of();
      private Instant lastSuccess;
      private int remainingFailures = maxConsecutiveFailures.orElse(Integer.MAX_VALUE);

      @Override
      public Stream<T> fetch() {
        try {
          cache = source.fetch().collect(Collectors.toList());
          remainingFailures = maxConsecutiveFailures.orElse(Integer.MAX_VALUE);
          lastSuccess = Instant.now();
          return cache.stream();
        } catch (Exception e) {
          if (--remainingFailures <= 0
              || maxInterval
                  .map(interval -> lastSuccess.plus(interval).isBefore(Instant.now()))
                  .orElse(false)) {
            throw new IllegalStateException("Too many failures for error-resilient join source", e);
          }
          return cache.stream();
        }
      }
    };
  }

  /**
   * Join two data sources
   *
   * @param left the source of left-handed values
   * @param right the source of right-handed values
   * @param leftKey a function to determine the multiple keys associated a left-handed value
   * @param rightKey a function to determine the single key associated with a right-handled value
   * @param sinkCreator a processor to handle the joined result
   * @param <L> the type on the left side of the join
   * @param <R> the type on the right side of the join
   * @param <K> the type of the join key
   */
  static <L, R, K> void join(
      JoinSource<L> left,
      JoinSource<R> right,
      Function<L, Stream<K>> leftKey,
      Function<R, K> rightKey,
      JoinSinkCreator<L, R> sinkCreator) {
    final var rightMap =
        right.fetch().collect(Collectors.groupingBy(rightKey, Collectors.toList()));
    left.fetch()
        .forEach(
            leftValue -> {
              final var sink = sinkCreator.create(leftValue);
              leftKey
                  .apply(leftValue)
                  .flatMap(leftKeyValue -> rightMap.getOrDefault(leftKeyValue, List.of()).stream())
                  .forEach(sink::accept);
              sink.finished();
            });
  }

  /**
   * Read data fixed data from a JSON file
   *
   * @param input the input JSON data
   * @param mapper a Jackson mapper to decode the data
   * @param type the class of the records being decoded
   * @param <T> the type of records being loaded
   * @return a fixed source that will always produce the same data read from the JSON file
   */
  static <T> JoinSource<T> jsonFile(InputStream input, ObjectMapper mapper, Class<T> type)
      throws IOException {
    @SuppressWarnings("unchecked")
    final var output =
        (List<T>)
            mapper.readValue(
                input, mapper.getTypeFactory().constructCollectionLikeType(List.class, type));
    return output::stream;
  }

  /**
   * Read data fixed data from a JSON file
   *
   * @param input the input JSON data
   * @param mapper a Jackson mapper to decode the data
   * @param type the class of the records being decoded
   * @param <T> the type of records being loaded
   * @return a fixed source that will always produce the same data read from the JSON file
   */
  static <T> JoinSource<T> jsonFile(InputStream input, ObjectMapper mapper, TypeReference<T> type)
      throws IOException {
    @SuppressWarnings("unchecked")
    final var output =
        (List<T>)
            mapper.readValue(
                input,
                mapper
                    .getTypeFactory()
                    .constructCollectionLikeType(
                        List.class, mapper.getTypeFactory().constructType(type)));
    return output::stream;
  }

  /**
   * A source that applies a transformation
   *
   * @param source the original data type to read
   * @param mapper a transformation to apply
   * @param <T> the type of the input data
   * @param <R> the type of the transformed data
   */
  static <T, R> JoinSource<R> map(JoinSource<T> source, Function<? super T, ? extends R> mapper) {
    return () -> source.fetch().map(mapper);
  }

  /** Provided the stored dataa */
  Stream<T> fetch();
}
