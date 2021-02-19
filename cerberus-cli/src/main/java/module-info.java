module ca.on.oicr.gsi.cerberus.cli {
  exports ca.on.oicr.gsi.cerberus.cli;

  opens ca.on.oicr.gsi.cerberus.cli to
      info.picocli;

  requires ca.oicr.gsi.pinery.api;
  requires ca.oicr.gsi.pinery.wsdto;
  requires ca.oicr.gsi.provenance.api;
  requires ca.on.oicr.gsi.cerberus;
  requires ca.on.oicr.gsi.vidarr.pluginapi;
  requires com.fasterxml.jackson.annotation;
  requires com.fasterxml.jackson.databind;
  requires commons.csv;
  requires info.picocli;
  requires org.apache.commons.lang3;
}
