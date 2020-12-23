package ca.on.oicr.gsi.cerberus.pinery;

import ca.on.oicr.pinery.client.HttpResponseException;
import ca.on.oicr.pinery.client.PineryClient;
import ca.on.oicr.ws.dto.LaneProvenanceDto;
import java.util.Set;
import java.util.stream.Stream;

public final class LaneProvenanceSource extends BaseProvenanceSource {

  public LaneProvenanceSource(String provider, String baseUrl, Set<Integer> versions) {
    super("lane", provider, baseUrl, versions);
  }

  @Override
  protected Stream<LaneProvenanceDto> fetch(PineryClient client, String version)
      throws HttpResponseException {
    return client.getLaneProvenance().version(version).stream();
  }
}
