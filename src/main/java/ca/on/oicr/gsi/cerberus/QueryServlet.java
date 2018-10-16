package ca.on.oicr.gsi.cerberus;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import ca.on.oicr.gsi.status.BasePage;

@WebServlet(name = "QueryServlet", urlPatterns = { "/query" })
public class QueryServlet extends HttpServlet {
	private static final long serialVersionUID = -1L;

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		response.setContentType("text/html; charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		try (OutputStream stream = response.getOutputStream()) {
			new BasePage(StatusServlet.SERVER_CONFIG) {
				private void writeInput(XMLStreamWriter writer, String name, String type, String label, String value)
						throws XMLStreamException {
					writer.writeStartElement("tr");
					writer.writeStartElement("td");
					writer.writeCharacters(label);
					writer.writeEndElement();
					writer.writeStartElement("td");
					writer.writeStartElement("input");
					writer.writeAttribute("type", type);
					writer.writeAttribute("name", name);
					if (value != null)
						writer.writeAttribute("value", value);
					writer.writeEndElement();
					writer.writeEndElement();
					writer.writeEndElement();

				}

				@Override
				protected void renderContent(XMLStreamWriter writer) throws XMLStreamException {
					writer.writeStartElement("form");
					writer.writeAttribute("name", "Provenance Query");
					writer.writeAttribute("method", "post");
					writer.writeAttribute("action", "WebProvenance");
					writer.writeStartElement("p");
					writer.writeCharacters("Leave all fields blank for default");
					writer.writeEndElement();

					writer.writeStartElement("table");
					writer.writeAttribute("class", "even");

					writeInput(writer, "name", "text", "Filter name:", null);
					writeInput(writer, "value", "text", "Filter value:", null);
					writeInput(writer, "html_output", "checkbox", "HTML output:", "ON");
					writer.writeStartElement("tr");
					writer.writeStartElement("td");
					writer.writeAttribute("colspan", "2");
					writer.writeStartElement("input");
					writer.writeAttribute("type", "submit");
					writer.writeAttribute("value", "Get records");
					writer.writeEndElement();
					writer.writeEndElement();
					writer.writeEndElement();

					writer.writeEndElement();
					writer.writeEndElement();
				}
			}.renderPage(stream);
		}
	}

	@Override
	public String getServletInfo() {
		return "Allow the user to perform a query";
	}

}
