package ca.on.oicr.gsi.cerberus.pinery;

import ca.on.oicr.gsi.cerberus.JoinSource;
import ca.on.oicr.gsi.prometheus.LatencyHistogram;
import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import ca.on.oicr.gsi.vidarr.JsonBodyHandler;
import ca.on.oicr.ws.dto.LaneProvenanceDto;
import ca.on.oicr.ws.dto.SampleProvenanceDto;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** Perform a fetch from Pinery for a particular LIMS value type */
public final class PineryProvenanceSource<T extends LimsProvenance>
    implements JoinSource<LimsProvenanceInfo> {
  private static final HttpClient CLIENT =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .followRedirects(HttpClient.Redirect.NORMAL)
          .connectTimeout(Duration.ofSeconds(20))
          .build();
  private static final LatencyHistogram FETCH_TIME =
      new LatencyHistogram(
          "cerberus_pinery_client_fetch_time",
          "The time required to fetch libraries from Pinery.",
          "kind",
          "target");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.registerModule(new JavaTimeModule());
  }

  /**
   * Get lane provenance
   *
   * @param provider the Pinery provider name to use
   * @param baseUrl the Pinery server URL
   * @param versions the format versions to access
   */
  public static JoinSource<LimsProvenanceInfo> lanes(
      String provider, String baseUrl, Set<Integer> versions) {
    return new PineryProvenanceSource<>(
        new TypeReference<List<LaneProvenanceDto>>() {}, "lane", provider, baseUrl, versions);
  }

  /**
   * Get sample provenance
   *
   * @param provider the Pinery provider name to use
   * @param baseUrl the Pinery server URL
   * @param versions the format versions to access
   */
  public static JoinSource<LimsProvenanceInfo> samples(
      String provider, String baseUrl, Set<Integer> versions) {
    return new PineryProvenanceSource<>(
        new TypeReference<List<SampleProvenanceDto>>() {}, "sample", provider, baseUrl, versions);
  }

  private final String baseUrl;
  private final String kind;
  private final String provider;
  private final TypeReference<List<T>> typeReference;
  private final Set<Integer> versions;

  private PineryProvenanceSource(
      TypeReference<List<T>> typeReference,
      String kind,
      String provider,
      String baseUrl,
      Set<Integer> versions) {
    this.typeReference = typeReference;
    this.kind = kind;
    this.baseUrl = baseUrl;
    this.versions = versions;
    this.provider = provider;
  }

  @Override
  public final Stream<LimsProvenanceInfo> fetch() {
    try (final var ignored = FETCH_TIME.start(kind, baseUrl)) {
      return versions.stream()
          .flatMap(
              v -> {
                try {
                  final var response =
                      CLIENT.send(
                          HttpRequest.newBuilder()
                              .uri(
                                  URI.create(
                                      String.format(
                                          "%s/provenance/v%d/%s-provenance", baseUrl, v, kind)))
                              .timeout(Duration.ofMinutes(10))
                              .header("Content-Type", "application/json")
                              .GET()
                              .build(),
                          new JsonBodyHandler<>(MAPPER, typeReference));
                  if (response.statusCode() == 200) {
                    return response.body().get().stream()
                        .map(l -> new LimsProvenanceInfo(provider, v, l));
                  } else {
                    throw new IllegalStateException(
                        "Response from Pinery: " + response.statusCode());
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
