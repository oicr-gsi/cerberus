package ca.on.oicr.gsi.cerberus.pinery;

import ca.on.oicr.gsi.provenance.model.LimsProvenance;
import ca.on.oicr.gsi.vidarr.api.ExternalId;

/** Store Pinery data with Pinery server information */
public final class LimsProvenanceInfo implements Comparable<LimsProvenanceInfo> {
  private final int formatRevision;
  private final LimsProvenance lims;
  private final String provider;

  public LimsProvenanceInfo(String provider, int formatRevision, LimsProvenance lims) {
    this.provider = provider;
    this.formatRevision = formatRevision;
    this.lims = lims;
  }

  @Override
  public int compareTo(LimsProvenanceInfo other) {
    final var revisionComparison = Integer.compare(formatRevision, other.formatRevision);
    return revisionComparison == 0
        ? lims.getLastModified().compareTo(other.lims.getLastModified())
        : revisionComparison;
  }

  public int formatRevision() {
    return formatRevision;
  }

  public ExternalId key() {
    return new ExternalId(provider, lims.getProvenanceId());
  }

  public LimsProvenance lims() {
    return lims;
  }

  public String provider() {
    return provider;
  }
}
