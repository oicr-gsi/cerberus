/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.client;

import ca.on.oicr.gsi.cerberus.util.PostField;
import ca.on.oicr.gsi.cerberus.util.ProvenanceAction;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
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
public class ProvenanceHttpClient implements AutoCloseable {

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
     * @param provenanceAction
     * @param incFilterSettings
     * @param excFilterSettings
     *
     * @return fp, InputStream containing file provenance data
     * @throws java.io.IOException
     * @throws javax.xml.ws.http.HTTPException
     */
    public InputStream getProvenanceJson(
            String provenanceType,
            String provenanceAction,
            Map<String, Set<String>> incFilterSettings,
            Map<String, Set<String>> excFilterSettings)
            throws IOException, HttpResponseException {

        // encode settings as json
        Map<PostField, Object> allSettings = new HashMap<>();
        allSettings.put(PostField.ACTION, provenanceAction);
        allSettings.put(PostField.TYPE, provenanceType);
        allSettings.put(PostField.INC_FILTER, incFilterSettings);
        allSettings.put(PostField.EXC_FILTER, excFilterSettings);
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
            log.error("HTTP response content follows:");
            String content = EntityUtils.toString(response.getEntity());
            log.error(content);
            throw new HttpResponseException(status, content);
        }
        log.debug("HTTP status is OK");

        HttpEntity entity = response.getEntity();
        return entity.getContent();
    }

    public InputStream getProvenanceJson(
            String provenanceType,
            String provenanceAction,
            Map<String, Set<String>> incFilterSettings)
            throws IOException, HttpResponseException {

        Map<String, Set<String>> excFilterSettings = new HashMap<>();
        return getProvenanceJson(provenanceType, provenanceAction, incFilterSettings, excFilterSettings);
    }

    public InputStream getProvenanceJson(
            String provenanceType)
            throws IOException, HttpResponseException {

        String provenanceAction = ProvenanceAction.NO_FILTER.name();
        Map<String, Set<String>> incFilterSettings = new HashMap<>();
        return getProvenanceJson(provenanceType, provenanceAction, incFilterSettings);
    }

}
