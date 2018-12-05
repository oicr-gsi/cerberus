/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.webservice;

import ca.on.oicr.gsi.cerberus.webservice.util.RequestParser;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.commons.lang.exception.ExceptionUtils.getStackTrace;


/**
 *
 * Web servlet to process HTTP request/response for file provenance
 *
 * @author ibancarz
 */
@WebServlet(name = "ProvenanceServlet", urlPatterns = { "/provenance" })
public class ProvenanceServlet extends HttpServlet {

    /**
     * Processes requests for the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        BufferedReader reqReader = request.getReader();
        String body = IOUtils.toString(reqReader);

        // read type, action, and filters from http request
        RequestParser rp = new RequestParser(body);
        String type = rp.getProvenanceType();
        String action = rp.getProvenanceAction();
        Map<FileProvenanceFilter, Set<String>> incFilters = rp.getIncFilters();
        Map<FileProvenanceFilter, Set<String>> excFilters = rp.getExcFilters();

        // read provider settings from server-side config file
        String providerSettings = null;
        try {
            String providerPath = this.getServletContext().getInitParameter("provenanceProviderSettings");
            providerSettings = readFileToString(new File(providerPath));
        } catch (IOError e) {
            String message = "<p>"+e.getMessage()+"</p>\n<p>"+getStackTrace(e)+"</p>\n";
            response.sendError(500, message);
        }

        response.setContentType("application/json;charset=UTF-8");
        BufferedWriter writer = new BufferedWriter(response.getWriter());
        try {
            ProvenanceHandler handler = new ProvenanceHandler(providerSettings, writer);
            handler.writeProvenanceJson(type, action, incFilters, excFilters);
        } catch (Exception e) {
            String message = "<p>"+e.getMessage()+"</p>\n<p>"+getStackTrace(e)+"</p>\n";
            response.sendError(500, message);
        }

    }

    //<editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
// Commented out for now as we are only using POST requests
// TODO: Support query with JSON in the body of a GET request?
//    
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
        return "Servlet to provide information on OICR file provenance";
    }// </editor-fold>

}
