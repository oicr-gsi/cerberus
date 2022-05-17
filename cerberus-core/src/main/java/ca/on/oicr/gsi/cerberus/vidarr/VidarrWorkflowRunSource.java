package ca.on.oicr.gsi.cerberus.vidarr;

import ca.on.oicr.gsi.cerberus.IncrementalJoinSource;
import ca.on.oicr.gsi.cerberus.JoinSource;
import ca.on.oicr.gsi.cerberus.UpdateResult;
import ca.on.oicr.gsi.prometheus.LatencyHistogram;
import ca.on.oicr.gsi.vidarr.JsonBodyHandler;
import ca.on.oicr.gsi.vidarr.api.AnalysisOutputType;
import ca.on.oicr.gsi.vidarr.api.AnalysisProvenanceRequest;
import ca.on.oicr.gsi.vidarr.api.AnalysisProvenanceResponse;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ProvenanceWorkflowRun;
import ca.on.oicr.gsi.vidarr.api.VersionPolicy;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.prometheus.client.Gauge;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** Incrementally fetch Vidarr workflow run data */
public final class VidarrWorkflowRunSource
    implements IncrementalJoinSource<ProvenanceWorkflowRun<ExternalKey>> {
  private static final HttpClient CLIENT =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .followRedirects(HttpClient.Redirect.NORMAL)
          .connectTimeout(Duration.ofSeconds(20))
          .build();
  private static final Gauge EPOCH =
      Gauge.build("cerberus_vidarr_client_epoch", "The last epoch seen from the Vidarr server.")
          .labelNames("target")
          .register();
  private static final Gauge ERROR =
      Gauge.build(
              "cerberus_vidarr_client_error", "Whether the last request succeeded Vidarr server.")
          .labelNames("target")
          .register();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final LatencyHistogram REQUEST_TIME =
      new LatencyHistogram(
          "cerberus_vidarr_client_request_time", "Time to fetch data from Vidarr.", "target");
  private static final Gauge TIMESTAMP =
      Gauge.build(
              "cerberus_vidarr_client_timestamp", "The last timestamp seen from the Vidarr server.")
          .labelNames("target")
          .register();

  static {
    MAPPER.registerModule(new JavaTimeModule());
  }

  public static Stream<ExternalId> key(ProvenanceWorkflowRun<? extends ExternalId> workflow) {
    return workflow.getExternalKeys().stream().map(k -> new ExternalId(k.getProvider(), k.getId()));
  }

  public static JoinSource<ProvenanceWorkflowRun<ExternalKey>> of(
      String instanceName, String baseUrl, Set<String> versionTypes) {
    return IncrementalJoinSource.accumulating(
        new VidarrWorkflowRunSource(instanceName, baseUrl, versionTypes));
  }

  private final String baseUrl;
  private long epoch;
  private final String instanceName;
  private long lastTime;
  private final Set<String> versionTypes;

  public VidarrWorkflowRunSource(String instanceName, String baseUrl, Set<String> versionTypes) {
    this.instanceName = instanceName;
    this.baseUrl = baseUrl;
    this.versionTypes = versionTypes;
  }

  @Override
  public UpdateResult<ProvenanceWorkflowRun<ExternalKey>> update() {
    try (final var ignored = REQUEST_TIME.start(baseUrl)) {
      final var request = new AnalysisProvenanceRequest();
      request.setAnalysisTypes(EnumSet.of(AnalysisOutputType.FILE));
      request.setEpoch(epoch);
      request.setTimestamp(lastTime);
      request.setVersionPolicy(VersionPolicy.LATEST);
      request.setVersionTypes(versionTypes);
      final var response =
          CLIENT.send(
              HttpRequest.newBuilder()
                  .uri(URI.create(String.format("%s/api/provenance", baseUrl)))
                  .timeout(Duration.ofMinutes(10))
                  .header("Content-Type", "application/json")
                  .POST(BodyPublishers.ofString(MAPPER.writeValueAsString(request)))
                  .build(),
              new JsonBodyHandler<>(
                  MAPPER, new TypeReference<AnalysisProvenanceResponse<ExternalKey>>() {}));
      if (response.statusCode() != 200) {
        ERROR.labels(baseUrl).set(1);
        return UpdateResult.incremental(List.of());
      }
      final var body = response.body().get();
      EPOCH.labels(baseUrl).set(body.getEpoch());
      TIMESTAMP.labels(baseUrl).set(body.getTimestamp());
      ERROR.labels(baseUrl).set(0);
      for (final var workflowRun : body.getResults()) {
        workflowRun.setInstanceName(instanceName);
      }
      if (body.getEpoch() == epoch) {
        lastTime = body.getTimestamp();
        return UpdateResult.incremental(body.getResults());
      } else {
        epoch = body.getEpoch();
        lastTime = body.getTimestamp();
        return UpdateResult.restart(body.getResults());
      }
    } catch (Exception e) {
      ERROR.labels(baseUrl).set(1);
      e.printStackTrace();
      return UpdateResult.incremental(List.of());
    }
  }
}
