package ca.on.oicr.gsi.cerberus.fileprovenance;

import ca.on.oicr.gsi.cerberus.JoinSink;
import ca.on.oicr.gsi.cerberus.JoinSinkCreator;
import ca.on.oicr.gsi.cerberus.pinery.LimsProvenanceInfo;
import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import ca.on.oicr.gsi.vidarr.api.ExternalId;
import ca.on.oicr.gsi.vidarr.api.ExternalKey;
import ca.on.oicr.gsi.vidarr.api.ProvenanceWorkflowRun;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public interface FileProvenanceConsumer {

  static JoinSinkCreator<ProvenanceWorkflowRun<ExternalKey>, LimsProvenanceInfo> of(
      FileProvenanceConsumer consumer) {
    return workflowRun ->
        new JoinSink<>() {
          private final Map<ExternalId, List<LimsProvenanceInfo>> limsKeys =
              new HashMap<>();

          @Override
          public void accept(LimsProvenanceInfo item) {
            limsKeys.computeIfAbsent(item.key(), k -> new ArrayList<>()).add(item);
          }

          @Override
          public void finished() {
            final var output = new ArrayList<ProvenanceRecord<LimsProvenance>>();
            var stale = false;
            final var limsInformation = new HashMap<ExternalId, LimsProvenanceInfo>();
            for (final var key : workflowRun.getExternalKeys()) {
              final var candidates = limsKeys.get(new ExternalId(key.getProvider(), key.getId()));
              if (candidates == null || candidates.isEmpty()) {
                consumer.error(workflowRun, limsKeys.values().stream().flatMap(List::stream));
                return;
              }
              final var match =
                  candidates.stream()
                      .filter(
                          lims ->
                              key.getVersions()
                                  .getOrDefault("pinery-hash-" + lims.formatRevision(), "")
                                  .equals(lims.lims().getVersion()))
                      .max(Comparator.comparing(LimsProvenanceInfo::formatRevision));
              if (match.isEmpty()) {
                stale = true;
                limsInformation.put(
                    new ExternalId(key.getProvider(), key.getId()),
                    candidates.stream()
                        .max(Comparator.comparing(LimsProvenanceInfo::formatRevision))
                        .orElseThrow());
              } else {
                limsInformation.put(new ExternalId(key.getProvider(), key.getId()), match.get());
              }
            }
            for (final var analysis : workflowRun.getAnalysis()) {
              for (final var key : analysis.getExternalKeys()) {
                final var lims = limsInformation.get(new ExternalId(key.getProvider(), key.getId()));
                output.add(
                    new ProvenanceRecord<>(
                        key.getProvider(),
                        lims.formatRevision(),
                        lims.lims(),
                        workflowRun,
                        analysis));
              }
            }
            final var s = stale;
            output.forEach(file -> consumer.file(s, file));
          }
        };
  }

  void error(
      ProvenanceWorkflowRun<ExternalKey> workflow,
      Stream<LimsProvenanceInfo> availableLimsInformation);

  void file(boolean stale, ProvenanceRecord<LimsProvenance> record);
}
