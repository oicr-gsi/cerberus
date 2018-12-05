package ca.on.oicr.gsi.cerberus.webservice;

import java.io.IOException;
import java.io.OutputStream;
import java.util.stream.Stream;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLStreamException;

import ca.on.oicr.gsi.status.ConfigurationSection;
import ca.on.oicr.gsi.status.Header;
import ca.on.oicr.gsi.status.NavigationMenu;
import ca.on.oicr.gsi.status.SectionRenderer;
import ca.on.oicr.gsi.status.ServerConfig;
import ca.on.oicr.gsi.status.StatusPage;

@WebServlet(name = "StatusServlet", urlPatterns = { "/" })
public class StatusServlet extends HttpServlet {

	public static ServerConfig SERVER_CONFIG = new ServerConfig() {

		@Override
		public Stream<NavigationMenu> navigation() {
			return Stream.of(NavigationMenu.item("query", "Query"));
		}

		@Override
		public String name() {
			return "Cerberus";
		}

		@Override
		public Stream<Header> headers() {
			return Stream.empty();
		}
	};
	private static final long serialVersionUID = -1L;

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		response.setContentType("text/html; charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		try (OutputStream stream = response.getOutputStream()) {
			new StatusPage(SERVER_CONFIG) {

				@Override
				public Stream<ConfigurationSection> sections() {
					return Stream.empty();
				}

				@Override
				protected void emitCore(SectionRenderer renderer) throws XMLStreamException {
					// No additional information.
				}
			}.renderPage(stream);
		}
	}

	@Override
	public String getServletInfo() {
		return "Show the main status page";
	}

}
