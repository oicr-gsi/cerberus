/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.webservice;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.fasterxml.jackson.databind.ObjectMapper;

import ca.on.oicr.gsi.status.BasePage;

/**
 *
 * @author ibancarz
 */
@WebServlet(name = "WebProvenanceServlet", urlPatterns = {"/WebProvenance"})
public class WebProvenanceServlet extends HttpServlet {

    /**
     * Processes requests for HTTP <code>POST</code> method.
     *
     * Placeholder class for a Cerberus web interface For now, just echoes back
     * the input filter parameters
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        String name = request.getParameter("name");
        String value = request.getParameter("value");
        Boolean htmlOutput = true;
        if (request.getParameter("html_output") == null) {
            htmlOutput = false;
        }

        Set<String> values = new HashSet();
        values.add(value);
        HashMap<String, Set<String>> filters = new HashMap();
        filters.put(name, values);

        ObjectMapper om = new ObjectMapper();
        String filterJson = om.writeValueAsString(filters);

        try (OutputStream out = response.getOutputStream()) {
            if (htmlOutput) {
                response.setContentType("text/html;charset=UTF-8");
                new BasePage(StatusServlet.SERVER_CONFIG) {
					
					@Override
					protected void renderContent(XMLStreamWriter writer) throws XMLStreamException {
						writer.writeStartElement("h1");
						writer.writeCharacters("Input filter parameters");
						writer.writeEndElement();
						writer.writeStartElement("pre");
						writer.writeCharacters(filterJson);
						writer.writeEndElement();
						
					}
				}.renderPage(out);
            } else {
                response.setContentType("application/json;charset=UTF-8");
                out.write(filterJson.getBytes(StandardCharsets.UTF_8));
            }
        }

    }

// <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs //
     */
//    @Override
//    protected void doGet(HttpServletRequest request, HttpServletResponse response)
//            throws ServletException, IOException {
//        processRequest(request, response);
//    }
    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}
