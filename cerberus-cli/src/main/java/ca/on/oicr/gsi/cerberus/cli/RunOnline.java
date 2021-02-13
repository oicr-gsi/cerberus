package ca.on.oicr.gsi.cerberus.cli;

import ca.on.oicr.gsi.cerberus.JoinSource;
import ca.on.oicr.gsi.cerberus.fileprovenance.FileProvenanceConsumer;
import ca.on.oicr.gsi.cerberus.pinery.LaneProvenanceSource;
import ca.on.oicr.gsi.cerberus.pinery.LimsProvenanceInfo;
import ca.on.oicr.gsi.cerberus.pinery.SampleProvenanceSource;
import ca.on.oicr.gsi.cerberus.vidarr.VidarrWorkflowRunSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import picocli.CommandLine;

@CommandLine.Command(
    name = "online",
    description = "Build the report using Pinery and Vidarr servers")
public final class RunOnline implements Callable<Integer> {

  @CommandLine.Option(
      names = {"-c", "--config", "--configuration"},
      required = true,
      description = "The target configuration to run against")
  private String configurationFileName;

  @CommandLine.Option(
      names = {"-o", "--out", "--output"},
      required = true,
      description = "The location for the TSV output")
  private String outputFileName;

  @Override
  public Integer call() throws Exception {
    final var configuration =
        new ObjectMapper().readValue(new File(configurationFileName), Configuration.class);

    try (final var output = new TabReportGenerator(this.outputFileName)) {
      final var versions =
          configuration.getPinery().values().stream()
              .flatMap(pinery -> pinery.getVersions().stream())
              .map(v -> "pinery-hash-" + v)
              .collect(Collectors.toSet());
      JoinSource.join(
          JoinSource.all(
              configuration.getVidarr().entrySet().stream()
                  .map(e -> VidarrWorkflowRunSource.of(e.getKey(), e.getValue(), versions))),
          JoinSource.all(
              configuration.getPinery().entrySet().stream()
                  .flatMap(
                      e ->
                          Stream.of(
                              new LaneProvenanceSource(
                                  e.getKey(), e.getValue().getUrl(), e.getValue().getVersions()),
                              new SampleProvenanceSource(
                                  e.getKey(), e.getValue().getUrl(), e.getValue().getVersions())))),
          VidarrWorkflowRunSource::key,
          LimsProvenanceInfo::key,
          FileProvenanceConsumer.of(output));
    }

    return 0;
  }
}
