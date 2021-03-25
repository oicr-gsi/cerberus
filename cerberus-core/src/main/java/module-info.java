module ca.on.oicr.gsi.cerberus {
  exports ca.on.oicr.gsi.cerberus;
  exports ca.on.oicr.gsi.cerberus.fileprovenance;
  exports ca.on.oicr.gsi.cerberus.pinery;
  exports ca.on.oicr.gsi.cerberus.vidarr;

  requires ca.on.oicr.gsi.pinery.api;
  requires ca.on.oicr.gsi.pinery.wsdto;
  requires ca.on.oicr.gsi.provenance.api;
  requires transitive ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.annotation;
  requires com.fasterxml.jackson.databind;
  requires com.fasterxml.jackson.datatype.jsr310;
  requires java.net.http;
  requires simpleclient;
}
