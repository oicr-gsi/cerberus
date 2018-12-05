package ca.on.oicr.gsi.cerberus.webservice;

import javax.servlet.annotation.WebServlet;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;

@WebServlet(name = "PrometheusServlet", urlPatterns = { "/metrics" })
public class PrometheusServlet extends MetricsServlet {

	private static final long serialVersionUID = 1L;

	public PrometheusServlet() {
		super();
		DefaultExports.initialize();
	}

}
