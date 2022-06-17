package ca.on.oicr.gsi.cerberus.pinery;

public class ExternalIdVersion implements Comparable<ExternalIdVersion> {

  private final String provider;
  private final String id;
  private final int formatRevision;
  private final String version;

  public ExternalIdVersion(String provider, String id, int formatRevision, String version) {
    this.provider = provider;
    this.id = id;
    this.formatRevision = formatRevision;
    this.version = version;
  }

  public String getProvider() {
    return provider;
  }

  public String getId() {
    return id;
  }

  public int getFormatRevision() {
    return formatRevision;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public int compareTo(ExternalIdVersion other) {
    return Integer.compare(formatRevision, other.formatRevision);
  }
}
