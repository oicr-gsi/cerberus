/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;

/**
 *
 * Web servlet to process HTTP request/response for file provenance
 *
 * @author ibancarz
 */
@WebServlet(name = "ProvenanceServlet", urlPatterns = {"/provenance"})
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

        java.io.BufferedReader reqReader = request.getReader();
        String body = IOUtils.toString(reqReader);

        RequestParser rp = new RequestParser(body);
        String providerSettings = rp.getProviderSettings();
        Map<FileProvenanceFilter, Set<String>> filters = rp.getFilters();

        ProvenanceHandler handler = new ProvenanceHandler(providerSettings);
        String fpJson = handler.getFileProvenanceJson(filters);

        response.setContentType("application/json;charset=UTF-8");
        try (PrintWriter out = response.getWriter()) {
            out.println(fpJson);
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
