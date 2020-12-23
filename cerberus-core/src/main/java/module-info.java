module ca.on.oicr.gsi.cerberus {
  exports ca.on.oicr.gsi.cerberus;
  exports ca.on.oicr.gsi.cerberus.fileprovenance;
  exports ca.on.oicr.gsi.cerberus.pinery;
  exports ca.on.oicr.gsi.cerberus.vidarr;

  requires com.fasterxml.jackson.annotation;
  requires com.fasterxml.jackson.databind;
  requires java.net.http;
  requires simpleclient;
  requires transitive ca.on.oicr.gsi.vidarr.pluginapi;
  requires ca.oicr.gsi.pinery.client;
  requires ca.oicr.gsi.pinery.wsdto;
  requires ca.oicr.gsi.pinery.api;
  requires ca.oicr.gsi.provenance.api;
}
