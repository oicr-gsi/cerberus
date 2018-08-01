/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * Tests for the provenance client
 *
 * @author ibancarz
 */

@RunWith(MockitoJUnitRunner.class)
public class ClientTest extends Base {

    // Create a Client with mock httpPost and httpRequest as arguments
    CloseableHttpClient myHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse myResponse = mock(CloseableHttpResponse.class);
    StatusLine myStatusLine = mock(StatusLine.class);
    HttpEntity myEntity = mock(HttpEntity.class);
    HttpPost myHttpPost = mock(HttpPost.class);
    Client myClient;
    String dummyOutput1;
    String dummyOutput2;

    @Before
    public void setUp() throws IOException {
        dummyOutput1 = "dummy_one";
        dummyOutput2 = "dummy_two";
        InputStream is1 = new ByteArrayInputStream(dummyOutput1.getBytes());
        InputStream is2 = new ByteArrayInputStream(dummyOutput2.getBytes());
        // return a new InputStream on first 2 invocations
        // allows us to get file provenance twice in the same test
        when(myEntity.getContent()).thenReturn(is1, is2);
        when(myResponse.getStatusLine()).thenReturn(myStatusLine);
        when(myStatusLine.getStatusCode()).thenReturn(200);
        when(myResponse.getEntity()).thenReturn(myEntity);
        when(myHttpClient.execute(any(HttpPost.class))).thenReturn(myResponse);
        myClient = new Client(myHttpClient, myHttpPost);
    }

    @Test
    public void testConstructor() {
        assertNotNull(myClient);
    }

    @Test
    public void testGetFileProvenance() throws IOException {

        Map<String, Set<String>> filterSettings = new HashMap<>();
        filterSettings.put("processing_status", new HashSet(Arrays.asList("success")));

        ArrayList<Map> providerSettings = getTestProviderSettings();
        String response1 = myClient.getFileProvenanceJson(providerSettings, filterSettings);
        assertTrue(response1.equals(dummyOutput1));
        String response2 = myClient.getFileProvenanceJson(providerSettings); // default filters
        assertTrue(response2.equals(dummyOutput2));
    }

}
