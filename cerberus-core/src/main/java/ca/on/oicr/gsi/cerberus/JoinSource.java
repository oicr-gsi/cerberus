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

public interface JoinSource<T> {
  @SafeVarargs
  static <T> JoinSource<T> all(JoinSource<? extends T>... sources) {
    return () -> Stream.of(sources).flatMap(JoinSource::fetch);
  }

  static <T> JoinSource<T> all(Stream<JoinSource<? extends T>> sources) {
    final var s = sources.collect(Collectors.toList());
    return () -> s.stream().flatMap(JoinSource::fetch);
  }

  static <T> JoinSource<T> empty() {
    return Stream::empty;
  }

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

  static <T> JoinSource<T> jsonFile(InputStream input, ObjectMapper mapper, Class<T> type)
      throws IOException {
    @SuppressWarnings("unchecked")
    final var output =
        (List<T>)
            mapper.readValue(
                input, mapper.getTypeFactory().constructCollectionLikeType(List.class, type));
    return output::stream;
  }

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

  static <T, R> JoinSource<R> map(JoinSource<T> source, Function<? super T, ? extends R> mapper) {
    return () -> source.fetch().map(mapper);
  }

  Stream<T> fetch();
}
