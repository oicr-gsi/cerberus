package ca.on.oicr.gsi.cerberus.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Configuration {

  private Map<String, PineryConfiguration> pinery;
  private Map<String, String> vidarr;
  private List<String> ignore;

  public Map<String, PineryConfiguration> getPinery() {
    return pinery;
  }

  public Map<String, String> getVidarr() {
    return vidarr;
  }

  public List<String> getIgnore() {
    return ignore;
  }

  public void setPinery(Map<String, PineryConfiguration> pinery) {
    this.pinery = pinery;
  }

  public void setVidarr(Map<String, String> vidarr) {
    this.vidarr = vidarr;
  }

  public void setIgnore(ArrayList<String> ignore) {
    this.ignore = ignore;
  }
}
