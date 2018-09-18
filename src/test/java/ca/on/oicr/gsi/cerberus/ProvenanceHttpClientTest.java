/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.cerberus.util.ProvenanceType;
import ca.on.oicr.gsi.cerberus.util.ProvenanceAction;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.IOUtils;
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
public class ProvenanceHttpClientTest extends Base {

    // Create a ProvenanceHttpClient with mock httpPost and httpRequest as arguments
    CloseableHttpClient myHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse myResponse = mock(CloseableHttpResponse.class);
    StatusLine myStatusLine = mock(StatusLine.class);
    HttpEntity myEntity = mock(HttpEntity.class);
    HttpPost myHttpPost = mock(HttpPost.class);
    String dummyOutput1;
    String dummyOutput2;
    String dummyOutput3;
    String dummyOutput4;
    ArrayList dummyOutputs;
    
    @Before
    public void setUp() throws IOException {

        dummyOutput1 = "dummy_one";
        dummyOutput2 = "dummy_two";
        dummyOutput3 = "dummy_three";
        dummyOutput4 = "dummy_four";
        dummyOutputs = new ArrayList<>();
        dummyOutputs.add(dummyOutput1);
        dummyOutputs.add(dummyOutput2);
        dummyOutputs.add(dummyOutput3);
        dummyOutputs.add(dummyOutput4);
        InputStream is1 = new ByteArrayInputStream(dummyOutput1.getBytes());
        InputStream is2 = new ByteArrayInputStream(dummyOutput2.getBytes());
        InputStream is3 = new ByteArrayInputStream(dummyOutput3.getBytes());
        InputStream is4 = new ByteArrayInputStream(dummyOutput4.getBytes());
        // specifying a single value in thenReturn() for myEntity doesn't work; each call after the first returns null
        when(myEntity.getContent()).thenReturn(is1, is2, is3, is4);
        when(myResponse.getStatusLine()).thenReturn(myStatusLine);
        when(myStatusLine.getStatusCode()).thenReturn(200);
        when(myResponse.getEntity()).thenReturn(myEntity);
        when(myHttpClient.execute(any(HttpPost.class))).thenReturn(myResponse);
    }

    @Test
    public void testConstructor() {
        ProvenanceHttpClient myClient = new ProvenanceHttpClient(myHttpClient, myHttpPost);
        assertNotNull(myClient);
    }

    @Test
    public void testAnalysisProvenance() throws IOException {
        ProvenanceHttpClient myClient = new ProvenanceHttpClient(myHttpClient, myHttpPost);
        String type = ProvenanceType.ANALYSIS.name();
        ArrayList<Map> providerSettings = getTestProviderSettings();
        Map<String, Set<String>> filters1 = new HashMap<>();
        filters1.put("processing_status", new HashSet(Arrays.asList("success")));
        String response1 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type)); // no filter
        assertTrue(response1.equals(dummyOutputs.get(0)));
        String response2 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type, ProvenanceAction.INC_FILTERS.name(), filters1)); 
        assertTrue(response2.equals(dummyOutputs.get(1)));
        String response3 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type, ProvenanceAction.BY_PROVIDER.name(), filters1));
        assertTrue(response3.equals(dummyOutputs.get(2)));  
    }
    
    @Test
    public void testFileProvenance() throws IOException {
        ProvenanceHttpClient myClient = new ProvenanceHttpClient(myHttpClient, myHttpPost);
        String type = ProvenanceType.FILE.name();
        ArrayList<Map> providerSettings = getTestProviderSettings();
        Map<String, Set<String>> filters1 = new HashMap<>();
        filters1.put("processing_status", new HashSet(Arrays.asList("success")));
        Map<String, Set<String>> filters2 = new HashMap<>();
        filters2.put("study", new HashSet(Arrays.asList("xenomorph")));
        String response1 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type)); // no filter
        assertTrue(response1.equals(dummyOutputs.get(0)));
        String response2 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type, ProvenanceAction.INC_FILTERS.name(), filters1));
        assertTrue(response2.equals(dummyOutputs.get(1)));
        String response3 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type, ProvenanceAction.INC_EXC_FILTERS.name(), filters1, filters2));
        assertTrue(response3.equals(dummyOutputs.get(2)));    
    }
    
    @Test
    public void testLaneProvenance() throws IOException {
        ProvenanceHttpClient myClient = new ProvenanceHttpClient(myHttpClient, myHttpPost);
        String type = ProvenanceType.LANE.name();
        ArrayList<Map> providerSettings = getTestProviderSettings();
        Map<String, Set<String>> filters1 = new HashMap<>();
        filters1.put("processing_status", new HashSet(Arrays.asList("success")));
        String response1 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type)); // no filter
        assertTrue(response1.equals(dummyOutputs.get(0)));
        String response2 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type, ProvenanceAction.INC_FILTERS.name(), filters1)); 
        assertTrue(response2.equals(dummyOutputs.get(1)));
        String response3 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type, ProvenanceAction.BY_PROVIDER.name(), filters1));
        assertTrue(response3.equals(dummyOutputs.get(2)));
        String response4 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type, ProvenanceAction.BY_PROVIDER_AND_ID.name(), filters1));
        assertTrue(response4.equals(dummyOutputs.get(3)));
    }
    
    @Test
    public void testSampleProvenance() throws IOException {
        ProvenanceHttpClient myClient = new ProvenanceHttpClient(myHttpClient, myHttpPost);
        String type = ProvenanceType.SAMPLE.name();
        ArrayList<Map> providerSettings = getTestProviderSettings();
        Map<String, Set<String>> filters1 = new HashMap<>();
        filters1.put("processing_status", new HashSet(Arrays.asList("success")));
        String response1 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type)); // no filter
        assertTrue(response1.equals(dummyOutputs.get(0)));
        String response2 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type, ProvenanceAction.INC_FILTERS.name(), filters1));
        assertTrue(response2.equals(dummyOutputs.get(1)));
        String response3 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type, ProvenanceAction.BY_PROVIDER.name(), filters1));
        assertTrue(response3.equals(dummyOutputs.get(2)));
        String response4 = IOUtils.toString(myClient.getProvenanceJson(providerSettings, type, ProvenanceAction.BY_PROVIDER_AND_ID.name(),  filters1));
        assertTrue(response4.equals(dummyOutputs.get(3)));
    }

}
