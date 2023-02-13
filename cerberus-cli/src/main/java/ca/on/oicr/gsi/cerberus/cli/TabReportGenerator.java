package ca.on.oicr.gsi.cerberus.cli;

import ca.on.oicr.gsi.cerberus.fileprovenance.FileProvenanceConsumer;
import ca.on.oicr.gsi.cerberus.fileprovenance.ProvenanceRecord;
import ca.on.oicr.gsi.cerberus.fileprovenance.ProvenanceRecord.Mapper;
import ca.on.oicr.gsi.cerberus.pinery.LimsProvenanceInfo;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ProvenanceWorkflowRun;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * Consume file provenance records and write them to a GZIP-compressed TSV table compatible with the
 * format of the existing Niassa file provenance report
 */
public final class TabReportGenerator implements FileProvenanceConsumer, AutoCloseable {

  private static final CSVFormat CSV_FORMAT =
      CSVFormat.newFormat('\t')
          .withNullString("")
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
  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  public static final DateTimeFormatter LIMS_DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Function<String, String> SANITISE_ATTRIBUTE =
      new StringSanitizerBuilder()
          .add("\t", " ")
          .add(";", "\u2300")
          .add("=", "\u2300")
          .add("&", "\u2300")
          .build();
  private static final Function<String, String> SANITISE_FIELD =
      new StringSanitizerBuilder()
          .add("\t", "\u2300")
          .add(";", "\u2300")
          .add("=", "\u2300")
          .add("&", "\u2300")
          .add(" ", "_")
          .build();
  public static final ZoneId TORONTO_TZ = ZoneId.of("America/Toronto");

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

  private final CSVPrinter output;

  public TabReportGenerator(String outputFileName) throws IOException {
    File outputFile = new File(outputFileName);
    outputFile.deleteOnExit();
    output =
        new CSVPrinter(
            new PrintWriter(
                new GZIPOutputStream(
                    Files.newOutputStream(Path.of(outputFileName), StandardOpenOption.CREATE_NEW))),
            CSV_FORMAT);
  }

  @Override
  public void close() throws Exception {
    output.close();
  }

  @Override
  public void error(
      ProvenanceWorkflowRun<ExternalKey> workflow,
      Stream<LimsProvenanceInfo> availableLimsInformation) {
    // Do nothing with error records
    availableLimsInformation.close();
  }

  @Override
  public void file(boolean stale, boolean skip, ProvenanceRecord<LimsProvenance> record) {

    final var cs = new ArrayList<String>();
    final var sampleAttributes =
        transform(
            SANITISE_ATTRIBUTE,
            SANITISE_ATTRIBUTE,
            record
                .apply(
                    new Mapper<SortedMap<String, SortedSet<String>>, SampleProvenance>(
                        SampleProvenance.class) {
                      @Override
                      protected SortedMap<String, SortedSet<String>> apply(SampleProvenance lims) {
                        return lims.getSampleAttributes();
                      }
                    })
                .orElse(Collections.emptySortedMap()));

    cs.add(
        record
            .workflow()
            .getCompleted()
            .withZoneSameInstant(TORONTO_TZ)
            .format(DATE_TIME_FORMATTER));

    cs.add(
        record
            .apply(
                new Mapper<String, SampleProvenance>(SampleProvenance.class) {
                  @Override
                  protected String apply(SampleProvenance lims) {
                    return SANITISE_FIELD.apply(lims.getStudyTitle());
                  }
                })
            .orElse(""));
    cs.add(""); // Study swids not available
    cs.add(
        transform(
            SANITISE_ATTRIBUTE,
            SANITISE_ATTRIBUTE,
            record
                .apply(
                    new Mapper<SortedMap<String, SortedSet<String>>, SampleProvenance>(
                        SampleProvenance.class) {
                      @Override
                      protected SortedMap<String, SortedSet<String>> apply(SampleProvenance lims) {
                        return lims.getStudyAttributes();
                      }
                    })
                .orElse(Collections.emptySortedMap())));

    cs.add("");
    cs.add(""); // Experiment swids not available
    cs.add("");

    cs.add(
        record
            .apply(
                new Mapper<String, SampleProvenance>(SampleProvenance.class) {
                  @Override
                  protected String apply(SampleProvenance lims) {
                    return SANITISE_FIELD.apply(lims.getRootSampleName());
                  }
                })
            .orElse(""));
    cs.add(""); // Root sample swids not available

    cs.add(
        record
            .apply(
                new Mapper<String, SampleProvenance>(SampleProvenance.class) {
                  @Override
                  protected String apply(SampleProvenance lims) {
                    return SANITISE_FIELD.apply(lims.getParentSampleName());
                  }
                })
            .orElse(""));
    cs.add(""); // Parent sample swids not available
    cs.add(""); // Parent sample organism ID
    cs.add(sampleAttributes);

    cs.add(
        record
            .apply(
                new Mapper<String, SampleProvenance>(SampleProvenance.class) {
                  @Override
                  protected String apply(SampleProvenance lims) {
                    return SANITISE_FIELD.apply(lims.getSampleName());
                  }
                })
            .orElse(""));
    cs.add(""); // sample swids not available
    cs.add(""); // Organism IDs
    cs.add(null); // Organism codes

    cs.add(sampleAttributes);

    cs.add(
        SANITISE_FIELD.apply(
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
                .orElse("")));
    cs.add(""); // sequencer run swids not available
    cs.add(
        transform(
            SANITISE_ATTRIBUTE,
            SANITISE_ATTRIBUTE,
            record
                .<SortedMap<String, SortedSet<String>>>apply(
                    new Mapper<>(SampleProvenance.class) {
                      @Override
                      protected SortedMap<String, SortedSet<String>> apply(SampleProvenance lims) {
                        return lims.getSequencerRunAttributes();
                      }
                    },
                    new Mapper<>(LaneProvenance.class) {
                      @Override
                      protected SortedMap<String, SortedSet<String>> apply(LaneProvenance lims) {
                        return lims.getSequencerRunAttributes();
                      }
                    })
                .orElse(Collections.emptySortedMap())));
    cs.add(""); // Platform IDs
    cs.add(
        record
            .<String>apply(
                new Mapper<>(SampleProvenance.class) {
                  @Override
                  protected String apply(SampleProvenance lims) {
                    return SANITISE_FIELD.apply(lims.getSequencerRunPlatformModel());
                  }
                },
                new Mapper<>(LaneProvenance.class) {
                  @Override
                  protected String apply(LaneProvenance lims) {
                    return SANITISE_FIELD.apply(lims.getSequencerRunPlatformModel());
                  }
                })
            .orElse(""));

    cs.add(
        record
            .<String>apply(
                new Mapper<>(SampleProvenance.class) {
                  @Override
                  protected String apply(SampleProvenance lims) {
                    return SANITISE_FIELD.apply(lims.getSequencerRunName())
                        + "_lane_"
                        + lims.getLaneNumber();
                  }
                },
                new Mapper<>(LaneProvenance.class) {
                  @Override
                  protected String apply(LaneProvenance lims) {
                    return SANITISE_FIELD.apply(lims.getSequencerRunName())
                        + "_lane_"
                        + lims.getLaneNumber();
                  }
                })
            .orElse(""));
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
            .orElse(""));
    cs.add(""); // lane swids not available
    cs.add(
        transform(
            SANITISE_ATTRIBUTE,
            SANITISE_ATTRIBUTE,
            record
                .<SortedMap<String, SortedSet<String>>>apply(
                    new Mapper<>(SampleProvenance.class) {
                      @Override
                      protected SortedMap<String, SortedSet<String>> apply(SampleProvenance lims) {
                        return lims.getLaneAttributes();
                      }
                    },
                    new Mapper<>(LaneProvenance.class) {
                      @Override
                      protected SortedMap<String, SortedSet<String>> apply(LaneProvenance lims) {
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
    cs.add(""); // IUS SWIDs
    cs.add(
        record.workflow().getExternalKeys().stream()
            .filter(
                key ->
                    key.getId()
                        .equals(
                            record
                                .lims()
                                .getProvenanceId())) // Get external keys with same ID as LIMS
            // provenance ID, aka the run_lane_libraryID
            // string
            .map(key -> "shesmu-sha1=" + key.getVersions().get("shesmu-sha1"))
            .collect(Collectors.joining())); // IUS Attributes

    cs.add(SANITISE_FIELD.apply(record.workflow().getWorkflowName()));
    cs.add(SANITISE_FIELD.apply(record.workflow().getWorkflowVersion()));
    cs.add(
        SANITISE_FIELD.apply(
            record.workflow().getWorkflowName() + "/" + record.workflow().getWorkflowVersion()));
    cs.add(""); // Workflow attributes

    cs.add(SANITISE_FIELD.apply(record.workflow().getWorkflowName()));
    cs.add("COMPLETE");
    cs.add(SANITISE_FIELD.apply(record.workflow().getId()));
    cs.add(
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(record.workflow().getLabels().fields(), 0),
                false)
            .map(
                entry -> {
                  try {
                    return SANITISE_ATTRIBUTE.apply(entry.getKey())
                        + "="
                        + MAPPER.writeValueAsString(entry.getValue());
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.joining(";")));

    cs.add(
        record.workflow().getInputFiles().stream()
            .map(SANITISE_ATTRIBUTE)
            .map(i -> i.replace("_", record.workflow().getInstanceName()))
            .collect(Collectors.joining(",")));

    cs.add(""); // Processing algorithm
    cs.add(""); // Processing SWID
    cs.add(""); // Processing attributes
    cs.add(""); // Processing status

    cs.add(SANITISE_FIELD.apply(record.record().getMetatype()));
    cs.add("vidarr:" + record.workflow().getInstanceName() + "/file/" + record.record().getId());
    cs.add(transformSimple(SANITISE_ATTRIBUTE, SANITISE_ATTRIBUTE, record.record().getLabels()));
    cs.add(SANITISE_FIELD.apply(record.record().getPath()));
    cs.add(SANITISE_FIELD.apply(record.record().getMd5()));
    cs.add(Long.toString(record.record().getSize()));
    cs.add(""); // File description

    cs.add(Boolean.toString(skip)); // Path skip
    cs.add(Boolean.toString(skip)); // Skip

    cs.add(stale ? "STALE" : "OKAY");
    cs.add(""); // Status reason

    cs.add("");
    // here we mangle the "Provider" field to look more similar to the current FPR provider field.
    // TODO: Ideally a new field of "format revision" would be added in a future update of FPR.
    cs.add(
        record.currentExternalIdVersion().getProvider()
            + "-v"
            + record.currentExternalIdVersion().getFormatRevision());
    cs.add(record.currentExternalIdVersion().getId());
    cs.add(record.currentExternalIdVersion().getVersion());
    cs.add(record.lims().getLastModified().format(LIMS_DATE_TIME_FORMATTER));
    try {
      output.printRecord(cs);
    } catch (IOException e) {
      throw new IOError(e);
    }
  }
}
