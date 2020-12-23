package ca.on.oicr.gsi.cerberus.cli;

import ca.on.oicr.gsi.cerberus.JoinSource;
import ca.on.oicr.gsi.cerberus.fileprovenance.FileProvenanceConsumer;
import ca.on.oicr.gsi.cerberus.fileprovenance.ProvenanceRecord;
import ca.on.oicr.gsi.cerberus.fileprovenance.ProvenanceRecord.Mapper;
import ca.on.oicr.gsi.cerberus.pinery.LaneProvenanceSource;
import ca.on.oicr.gsi.cerberus.pinery.LimsProvenanceInfo;
import ca.on.oicr.gsi.cerberus.pinery.SampleProvenanceSource;
import ca.on.oicr.gsi.cerberus.vidarr.VidarrWorkflowRunSource;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ProvenanceWorkflowRun;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import picocli.CommandLine;

/** Main entry point from the command line */
@CommandLine.Command(
    name = "cerberus",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "TSV file provenance generator")
public final class Main implements Callable<Integer> {

  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  public static final ZoneId TORONTO_TZ = ZoneId.of("America/Toronto");

  public static void main(String[] args) {
    final var cmd = new CommandLine(new Main());
    cmd.setExecutionStrategy(new CommandLine.RunLast());
    System.exit(cmd.execute(args));
  }

  public static <K, V> String transform(
      Function<K, String> keyTransformer,
      Function<V, String> valueTransformer,
      Map<K, ? extends Set<V>> map) {
    return map.entrySet().stream()
        .map(
            entry ->
                keyTransformer.apply(entry.getKey())
                    + "="
                    + entry.getValue().stream()
                        .map(valueTransformer)
                        .collect(Collectors.joining("&")))
        .collect(Collectors.joining(";"));
  }

  public static <K, V> String transform(
      Function<K, String> keyTransformer,
      Function<V, String> valueTransformer,
      SortedMap<K, ? extends Set<V>> map) {
    return map.entrySet().stream()
        .map(
            entry ->
                keyTransformer.apply(entry.getKey())
                    + "="
                    + entry.getValue().stream()
                        .map(valueTransformer)
                        .collect(Collectors.joining("&")))
        .collect(Collectors.joining(";"));
  }

  public static <K, V> String transformSimple(
      Function<K, String> keyTransformer, Function<V, String> valueTransformer, Map<K, V> map) {
    return map.entrySet().stream()
        .map(
            entry ->
                keyTransformer.apply(entry.getKey())
                    + "="
                    + valueTransformer.apply(entry.getValue()))
        .collect(Collectors.joining(";"));
  }

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
    final var nullString = "";
    final var cf =
        CSVFormat.newFormat('\t')
            .withNullString(nullString)
            .withRecordSeparator('\n')
            .withHeader(
                "Last Modified",
                "Study Title",
                "Study SWID",
                "Study Attributes",
                "Experiment Name",
                "Experiment SWID",
                "Experiment Attributes",
                "Root Sample Name",
                "Root Sample SWID",
                "Parent Sample Name",
                "Parent Sample SWID",
                "Parent Sample Organism IDs",
                "Parent Sample Attributes",
                "Sample Name",
                "Sample SWID",
                "Sample Organism ID",
                "Sample Organism Code",
                "Sample Attributes",
                "Sequencer Run Name",
                "Sequencer Run SWID",
                "Sequencer Run Attributes",
                "Sequencer Run Platform ID",
                "Sequencer Run Platform Name",
                "Lane Name",
                "Lane Number",
                "Lane SWID",
                "Lane Attributes",
                "IUS Tag",
                "IUS SWID",
                "IUS Attributes",
                "Workflow Name",
                "Workflow Version",
                "Workflow SWID",
                "Workflow Attributes",
                "Workflow Run Name",
                "Workflow Run Status",
                "Workflow Run SWID",
                "Workflow Run Attributes",
                "Workflow Run Input File SWAs",
                "Processing Algorithm",
                "Processing SWID",
                "Processing Attributes",
                "Processing Status",
                "File Meta-Type",
                "File SWID",
                "File Attributes",
                "File Path",
                "File Md5sum",
                "File Size",
                "File Description",
                "Path Skip",
                "Skip",
                "Status",
                "Status Reason",
                "LIMS IUS SWID",
                "LIMS Provider",
                "LIMS ID",
                "LIMS Version",
                "LIMS Last Modified");
    final var stringSanitizer =
        new StringSanitizerBuilder()
            .add("\t", "\u2300")
            .add(";", "\u2300")
            .add("=", "\u2300")
            .add("&", "\u2300")
            .add(" ", "_")
            .build();

    final var ssForAttributes =
        new StringSanitizerBuilder()
            .add("\t", " ")
            .add(";", "\u2300")
            .add("=", "\u2300")
            .add("&", "\u2300")
            .build();

    final var delimiter = ";";
    Function<Collection<String>, String> join =
        c ->
            c.stream()
                .filter(Objects::nonNull)
                .map(ssForAttributes)
                .collect(Collectors.joining(delimiter));

    try (final var output =
        new CSVPrinter(
            new PrintWriter(
                new GZIPOutputStream(
                    Files.newOutputStream(
                        Path.of(this.outputFileName), StandardOpenOption.CREATE_NEW))),
            cf)) {
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
          FileProvenanceConsumer.of(
              new FileProvenanceConsumer() {
                @Override
                public void error(
                    ProvenanceWorkflowRun<ExternalKey> workflow,
                    Stream<LimsProvenanceInfo> availableLimsInformation) {
                  // Do nothing with error records
                  availableLimsInformation.close();
                }

                @Override
                public void file(boolean stale, ProvenanceRecord<LimsProvenance> record) {

                  final var cs = new ArrayList<String>();
                  final var sampleAttributes =
                      transform(
                          ssForAttributes,
                          ssForAttributes,
                          record
                              .apply(
                                  new Mapper<
                                      SortedMap<String, SortedSet<String>>, SampleProvenance>(
                                      SampleProvenance.class) {
                                    @Override
                                    protected SortedMap<String, SortedSet<String>> apply(
                                        SampleProvenance lims) {
                                      return lims.getSampleAttributes();
                                    }
                                  })
                              .orElse(Collections.emptySortedMap()));

                  cs.add(
                      record
                          .workflow()
                          .getModified()
                          .withZoneSameInstant(TORONTO_TZ)
                          .format(DATE_TIME_FORMATTER));

                  cs.add(
                      record
                          .apply(
                              new Mapper<String, SampleProvenance>(SampleProvenance.class) {
                                @Override
                                protected String apply(SampleProvenance lims) {
                                  return stringSanitizer.apply(lims.getStudyTitle());
                                }
                              })
                          .orElse(""));
                  cs.add(nullString); // Study swids not available
                  cs.add(
                      transform(
                          ssForAttributes,
                          ssForAttributes,
                          record
                              .apply(
                                  new Mapper<
                                      SortedMap<String, SortedSet<String>>, SampleProvenance>(
                                      SampleProvenance.class) {
                                    @Override
                                    protected SortedMap<String, SortedSet<String>> apply(
                                        SampleProvenance lims) {
                                      return lims.getStudyAttributes();
                                    }
                                  })
                              .orElse(Collections.emptySortedMap())));

                  cs.add(nullString);
                  cs.add(nullString); // Experiment swids not available
                  cs.add(nullString);

                  cs.add(
                      record
                          .apply(
                              new Mapper<String, SampleProvenance>(SampleProvenance.class) {
                                @Override
                                protected String apply(SampleProvenance lims) {
                                  return stringSanitizer.apply(lims.getRootSampleName());
                                }
                              })
                          .orElse(nullString));
                  cs.add(nullString); // Root sample swids not available

                  cs.add(
                      record
                          .apply(
                              new Mapper<String, SampleProvenance>(SampleProvenance.class) {
                                @Override
                                protected String apply(SampleProvenance lims) {
                                  return stringSanitizer.apply(lims.getParentSampleName());
                                }
                              })
                          .orElse(nullString));
                  cs.add(nullString); // Parent sample swids not available
                  cs.add(nullString); // Parent sample organism ID
                  cs.add(sampleAttributes);

                  cs.add(
                      record
                          .apply(
                              new Mapper<String, SampleProvenance>(SampleProvenance.class) {
                                @Override
                                protected String apply(SampleProvenance lims) {
                                  return stringSanitizer.apply(lims.getSampleName());
                                }
                              })
                          .orElse(nullString));
                  cs.add(nullString); // sample swids not available
                  cs.add(nullString); // Organism IDs
                  cs.add(null); // Organism codes

                  cs.add(sampleAttributes);

                  cs.add(
                      stringSanitizer.apply(
                          record
                              .<String>apply(
                                  new Mapper<>(SampleProvenance.class) {
                                    @Override
                                    protected String apply(SampleProvenance lims) {
                                      return lims.getSequencerRunName();
                                    }
                                  },
                                  new Mapper<>(LaneProvenance.class) {
                                    @Override
                                    protected String apply(LaneProvenance lims) {
                                      return lims.getSequencerRunName();
                                    }
                                  })
                              .orElse(nullString)));
                  cs.add(nullString); // sequencer run swids not available
                  cs.add(
                      transform(
                          ssForAttributes,
                          ssForAttributes,
                          record
                              .<SortedMap<String, SortedSet<String>>>apply(
                                  new Mapper<>(SampleProvenance.class) {
                                    @Override
                                    protected SortedMap<String, SortedSet<String>> apply(
                                        SampleProvenance lims) {
                                      return lims.getSequencerRunAttributes();
                                    }
                                  },
                                  new Mapper<>(LaneProvenance.class) {
                                    @Override
                                    protected SortedMap<String, SortedSet<String>> apply(
                                        LaneProvenance lims) {
                                      return lims.getSequencerRunAttributes();
                                    }
                                  })
                              .orElse(Collections.emptySortedMap())));
                  cs.add(nullString); // Platform IDs
                  cs.add(
                      record
                          .<String>apply(
                              new Mapper<>(SampleProvenance.class) {
                                @Override
                                protected String apply(SampleProvenance lims) {
                                  return stringSanitizer.apply(lims.getSequencerRunPlatformModel());
                                }
                              },
                              new Mapper<>(LaneProvenance.class) {
                                @Override
                                protected String apply(LaneProvenance lims) {
                                  return stringSanitizer.apply(lims.getSequencerRunPlatformModel());
                                }
                              })
                          .orElse(nullString));

                  cs.add(
                      record
                          .<String>apply(
                              new Mapper<>(SampleProvenance.class) {
                                @Override
                                protected String apply(SampleProvenance lims) {
                                  return stringSanitizer.apply(lims.getSequencerRunName())
                                      + "_lane_"
                                      + lims.getLaneNumber();
                                }
                              },
                              new Mapper<>(LaneProvenance.class) {
                                @Override
                                protected String apply(LaneProvenance lims) {
                                  return stringSanitizer.apply(lims.getSequencerRunName())
                                      + "_lane_"
                                      + lims.getLaneNumber();
                                }
                              })
                          .orElse(nullString));
                  cs.add(
                      record
                          .<String>apply(
                              new Mapper<>(SampleProvenance.class) {
                                @Override
                                protected String apply(SampleProvenance lims) {
                                  return lims.getLaneNumber();
                                }
                              },
                              new Mapper<>(LaneProvenance.class) {
                                @Override
                                protected String apply(LaneProvenance lims) {
                                  return lims.getLaneNumber();
                                }
                              })
                          .orElse(nullString));
                  cs.add(nullString); // lane swids not available
                  cs.add(
                      transform(
                          ssForAttributes,
                          ssForAttributes,
                          record
                              .<SortedMap<String, SortedSet<String>>>apply(
                                  new Mapper<>(SampleProvenance.class) {
                                    @Override
                                    protected SortedMap<String, SortedSet<String>> apply(
                                        SampleProvenance lims) {
                                      return lims.getLaneAttributes();
                                    }
                                  },
                                  new Mapper<>(LaneProvenance.class) {
                                    @Override
                                    protected SortedMap<String, SortedSet<String>> apply(
                                        LaneProvenance lims) {
                                      return lims.getLaneAttributes();
                                    }
                                  })
                              .orElse(Collections.emptySortedMap())));

                  cs.add(
                      record
                          .apply(
                              new Mapper<String, SampleProvenance>(SampleProvenance.class) {
                                @Override
                                protected String apply(SampleProvenance lims) {
                                  return lims.getIusTag();
                                }
                              })
                          .orElse("NoIndex"));
                  cs.add(nullString); // IUS SWIDs
                  cs.add(nullString); // IUS attributes

                  cs.add(stringSanitizer.apply(record.workflow().getWorkflowName()));
                  cs.add(stringSanitizer.apply(record.workflow().getWorkflowVersion()));
                  cs.add(
                      stringSanitizer.apply(
                          record.workflow().getWorkflowName()
                              + "/"
                              + record.workflow().getWorkflowVersion()));
                  cs.add(nullString); // Workflow attributes

                  cs.add(stringSanitizer.apply(record.workflow().getWorkflowName()));
                  cs.add("COMPLETE");
                  cs.add(
                      stringSanitizer.apply(
                          record.workflow().getWorkflowName()
                              + "/"
                              + record.workflow().getWorkflowVersion()));
                  cs.add(
                      transformSimple(
                          ssForAttributes, ssForAttributes, record.workflow().getLabels()));

                  cs.add(
                      record.workflow().getInputFiles().stream()
                          .map(ssForAttributes)
                          .collect(Collectors.joining(",")));

                  cs.add(nullString); // Processing algorithm
                  cs.add(nullString); // Processing SWID
                  cs.add(nullString); // Processing attrbutes
                  cs.add(nullString); // Processing status

                  cs.add(stringSanitizer.apply(record.record().getMetatype()));
                  cs.add(
                      "vidarr:"
                          + record.workflow().getInstanceName()
                          + "/file/"
                          + record.workflow().getId());
                  cs.add(
                      transformSimple(
                          ssForAttributes, ssForAttributes, record.record().getLabels()));
                  cs.add(stringSanitizer.apply(record.record().getFilePath()));
                  cs.add(stringSanitizer.apply(record.record().getMd5sum()));
                  cs.add(Long.toString(record.record().getFileSize()));
                  cs.add(nullString); // File description

                  cs.add("false"); // Path skip
                  cs.add("false"); // Skip

                  cs.add(stale ? "STALE" : "OKAY");
                  cs.add(nullString); // Status reason

                  cs.add(nullString);
                  cs.add(record.provider());
                  cs.add(record.lims().getProvenanceId());
                  cs.add(record.lims().getVersion());
                  cs.add(record.lims().getLastModified().toString());
                  try {
                    output.printRecord(cs);
                  } catch (IOException e) {
                    throw new IOError(e);
                  }
                }
              }));
    }

    return 0;
  }
}
