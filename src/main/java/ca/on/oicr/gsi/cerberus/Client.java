/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.xml.ws.http.HTTPException;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 *
 * Front-end for the Cerberus provenance web server:
 *
 * <ul>
 * <li>Composes appropriate HTTP requests, given filter criteria
 * <li>Transmits requests to the server
 * <li>Returns JSON response
 * </ul>
 *
 * Downstream applications can import this class and call methods to get file
 * provenance in JSON format
 * 
 * TODO enable an OPTIONS operation to get allowable filters
 *
 * @author ibancarz
 */
public class Client {

    private final CloseableHttpClient httpClient;
    private final HttpPost httpPost;
    private final Logger log = LogManager.getLogger(Client.class);

    /**
     * Constructor to allow testing with mock classes
     *
     * @param httpClient
     * @param httpPost
     */
    public Client(CloseableHttpClient httpClient, HttpPost httpPost) {

        this.httpClient = httpClient;
        this.httpPost = httpPost;

    }

    /**
     * Constructor from a single URI
     *
     * @param uri
     */
    public Client(URI uri) {

        CloseableHttpClient myHttpClient = HttpClients.createDefault();
        this.httpClient = myHttpClient;
        this.httpPost = new HttpPost(uri);

    }

    public void close() throws IOException {

        httpClient.close();
    }

    /**
     * Get file provenance with specified filter values
     * 
     * @param providerSettings, list of parameters for each Provider
     * @param filterSettings, map from FileProvenanceFilter function names to
     * filter values
     *
     *
     * @return fp, a JSON string containing file provenance data
     * @throws java.io.IOException
     * @throws javax.xml.ws.http.HTTPException
     */
    public String getFileProvenanceJson(ArrayList<Map> providerSettings,
            Map<String, Set<String>> filterSettings)
            throws IOException, HTTPException {

        // encode settings as json
        ObjectMapper om = new ObjectMapper();
        Map allSettings = new HashMap();
        allSettings.put("provider_settings", providerSettings);
        allSettings.put("filter_settings", filterSettings);
        String body = om.writeValueAsString(allSettings);
        log.info("HTTP message body: " + body);

        // populate HTTP request
        httpPost.reset();
        httpPost.removeHeaders("Accept");
        httpPost.removeHeaders("Content-type");
        httpPost.setHeader("Accept", "application/json");
        httpPost.setHeader("Content-type", "application/json");
        httpPost.setEntity(new StringEntity(body));

        // execute the POST operation and parse output
        HttpResponse response = httpClient.execute(httpPost);
        int status = response.getStatusLine().getStatusCode();

        if (status != 200) {
            log.error("HTTP error status: " + Integer.toString(status));
            throw new HTTPException(status);
        }
        log.debug("HTTP status is OK");

        // TODO return the InputStream instead?
        // More efficient/flexible if returning a large amount of data?
        HttpEntity entity = response.getEntity();
        InputStream stream1 = entity.getContent();
        String fp = IOUtils.toString(stream1);
        log.debug("Found content of HTTP response, length " + Integer.toString(fp.length()));
        return fp;
    }

    /**
     * Get file provenance with default filter values
     *
     * @param providerSettings
     * @return fp, a JSON string containing file provenance data
     * @throws IOException
     * @throws HTTPException
     */
    public String getFileProvenanceJson(ArrayList<Map> providerSettings)
            throws IOException, HTTPException {
        Map<String, Set<String>> filterSettings = getDefaultFilters();
        return getFileProvenanceJson(providerSettings, filterSettings);
    }

    private static Map<String, Set<String>> getDefaultFilters() {
        Map<String, Set<String>> filters = new HashMap<>();
        Set<String> sList = new HashSet(Arrays.asList("success"));
        filters.put("processing_status", sList);
        Set<String> cList = new HashSet(Arrays.asList("completed"));
        filters.put("workflow_run_status", cList);
        Set<String> fList = new HashSet(Arrays.asList("false"));
        filters.put("skip", fList);
        return filters;
    }

}
