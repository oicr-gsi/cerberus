package ca.on.oicr.gsi.cerberus.webservice;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;

import ca.on.oicr.gsi.prometheus.LatencyHistogram;
import io.prometheus.client.Counter;

@WebFilter(urlPatterns = { "/*" }, filterName = "prometheus")
public class PrometheusFilter implements Filter {
	private static final LatencyHistogram REQUEST_TIME = new LatencyHistogram("cerberus_request_time",
			"Time to complete a Cerberus request in seconds.", "endpoint");
	private static final Counter THROW_COUNT = Counter
			.build("cerberus_request_throws", "The number of time an endpoint has thrown.").labelNames("endpoint")
			.register();

	@Override
	public void destroy() {
		// No clean up required.
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		String endpoint = "unknown";
		if (request instanceof HttpServletRequest) {
			endpoint = ((HttpServletRequest) request).getRequestURI();
		}
		try (AutoCloseable timer = REQUEST_TIME.start(endpoint)) {
			chain.doFilter(request, response);
		} catch (Exception e) {
			THROW_COUNT.labels(endpoint).inc();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void init(FilterConfig config) throws ServletException {
		// No initialisation required.
	}

}
