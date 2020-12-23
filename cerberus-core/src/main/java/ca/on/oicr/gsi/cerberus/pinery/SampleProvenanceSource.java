package ca.on.oicr.gsi.cerberus.pinery;

import ca.on.oicr.pinery.client.HttpResponseException;
import ca.on.oicr.pinery.client.PineryClient;
import ca.on.oicr.ws.dto.SampleProvenanceDto;
import java.util.Set;
import java.util.stream.Stream;

public final class SampleProvenanceSource extends BaseProvenanceSource {

  public SampleProvenanceSource(String provider, String baseUrl, Set<Integer> versions) {
    super("sample", provider, baseUrl, versions);
  }

  @Override
  protected Stream<SampleProvenanceDto> fetch(PineryClient client, String version)
      throws HttpResponseException {
    return client.getSampleProvenance().version(version).stream();
  }
}
