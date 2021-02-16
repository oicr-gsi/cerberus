package ca.on.oicr.gsi.cerberus.pinery;

import ca.on.oicr.gsi.cerberus.JoinSource;
import ca.on.oicr.gsi.prometheus.LatencyHistogram;
import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import ca.on.oicr.pinery.client.HttpResponseException;
import ca.on.oicr.pinery.client.PineryClient;
import java.util.Set;
import java.util.stream.Stream;

/** Perform a fetch from Pinery for a particular LIMS value type */
public abstract class BaseProvenanceSource implements JoinSource<LimsProvenanceInfo> {
  private static final LatencyHistogram FETCH_TIME =
      new LatencyHistogram(
          "cerberus_pinery_client_fetch_time",
          "The time required to fetch libraries from Pinery.",
          "kind",
          "target");
  private final String baseUrl;
  private final PineryClient client;
  private final String kind;
  private final String provider;
  private final Set<Integer> versions;

  public BaseProvenanceSource(String kind, String provider, String baseUrl, Set<Integer> versions) {
    this.kind = kind;
    client = new PineryClient(baseUrl);
    this.baseUrl = baseUrl;
    this.versions = versions;
    this.provider = provider;
  }

  /**
   * Fetch the real data
   *
   * @param client the Pinery client to use
   * @param version the Pinery version to use
   * @return the data fetched
   */
  protected abstract Stream<? extends LimsProvenance> fetch(PineryClient client, String version)
      throws HttpResponseException;

  @Override
  public final Stream<LimsProvenanceInfo> fetch() {
    try (final var ignored = FETCH_TIME.start(kind, baseUrl)) {
      return versions.stream()
          .flatMap(
              v -> {
                try {
                  return fetch(client, "v" + v).map(l -> new LimsProvenanceInfo(provider, v, l));
                } catch (HttpResponseException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
