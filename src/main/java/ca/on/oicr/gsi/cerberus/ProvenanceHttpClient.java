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
import java.util.HashMap;
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
 * <li>Returns the response as a JSON string
 * </ul>
 *
 *
 * TODO enable an OPTIONS operation to get allowable filters?
 *
 * @author ibancarz
 */
public class ProvenanceHttpClient extends Base {

    private final CloseableHttpClient httpClient;
    private final HttpPost httpPost;
    private final Logger log = LogManager.getLogger(ProvenanceHttpClient.class);
    private final ObjectMapper om;

    /**
     * Constructor to allow testing with mock classes
     *
     * @param httpClient
     * @param httpPost
     */
    public ProvenanceHttpClient(CloseableHttpClient httpClient, HttpPost httpPost) {
        this.httpClient = httpClient;
        this.httpPost = httpPost;
        om = new ObjectMapper();
    }

    /**
     * Constructor from a single URI
     *
     * @param uri
     */
    public ProvenanceHttpClient(URI uri) {
        CloseableHttpClient myHttpClient = HttpClients.createDefault();
        httpClient = myHttpClient;
        httpPost = new HttpPost(uri);
        om = new ObjectMapper();
    }

    /**
     * Close the HttpClient to free up system resources
     *
     * @throws IOException
     */
    public void close() throws IOException {
        httpClient.close();
    }

    /**
     * Get provenance for specified providers, filters, and type
     *
     * @param provenanceType, String: One of ANALYSIS, FILE, LANE, SAMPLE
     * @param providerSettings, list of parameters for each Provider
     * @param provenanceAction
     * @param incFilterSettings
     * @param excFilterSettings
     *
     * @return fp, a JSON string containing file provenance data
     * @throws java.io.IOException
     * @throws javax.xml.ws.http.HTTPException
     */
    public String getProvenanceJson(
            ArrayList<Map> providerSettings,
            String provenanceType,
            String provenanceAction,
            Map<String, Set<String>> incFilterSettings,
            Map<String, Set<String>> excFilterSettings)
            throws IOException, HTTPException {

        // encode settings as json
        Map allSettings = new HashMap();
        allSettings.put(PROVIDER_KEY, providerSettings);
        allSettings.put(PROVENANCE_ACTION_KEY, provenanceAction);
        allSettings.put(PROVENANCE_TYPE_KEY, provenanceType);
        allSettings.put(INC_FILTER_KEY, incFilterSettings);
        allSettings.put(EXC_FILTER_KEY, excFilterSettings);
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
        String pJson = IOUtils.toString(stream1);
        log.debug("Found content of HTTP response, length " + Integer.toString(pJson.length()));
        return pJson;
    }

    public String getProvenanceJson(
            ArrayList<Map> providerSettings,
            String provenanceType,
            String provenanceAction,
            Map<String, Set<String>> incFilterSettings)
            throws IOException, HTTPException {

        Map<String, Set<String>> excFilterSettings = new HashMap<>();
        return getProvenanceJson(providerSettings, provenanceType, provenanceAction, incFilterSettings, excFilterSettings);
    }

    public String getProvenanceJson(
            ArrayList<Map> providerSettings,
            String provenanceType)
            throws IOException, HTTPException {

        String provenanceAction = ProvenanceAction.NO_FILTER.name();
        Map<String, Set<String>> incFilterSettings = new HashMap<>();
        return getProvenanceJson(providerSettings, provenanceType, provenanceAction, incFilterSettings);
    }

}
