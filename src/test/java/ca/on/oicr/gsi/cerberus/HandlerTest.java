/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.provenance.DefaultProvenanceClient;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * Tests for the ProvenanceHandler class
 *
 * @author ibancarz
 */
@RunWith(MockitoJUnitRunner.class)
public class HandlerTest extends Base {

    private final ObjectMapper om;
    private String providerSettings;
    HashSet<String> dummySet;
    DefaultProvenanceClient dpc = mock(DefaultProvenanceClient.class);

    public HandlerTest() {
        om = new ObjectMapper();
        dummySet = new HashSet<>();
        dummySet.add("foo");
        dummySet.add("bar");
        when(dpc.getFileProvenance(any(Map.class))).thenReturn(dummySet);
    }

    @Before
    public void setUp() throws IOException {

        providerSettings = om.writeValueAsString(getTestProviderSettings());
    }

    @Test
    public void constructorTest() throws IOException {

        ProvenanceHandler ph1 = new ProvenanceHandler(providerSettings);
        assertNotNull(ph1);

        ProvenanceHandler ph2 = new ProvenanceHandler(providerSettings, dpc);
        assertNotNull(ph2);

    }

    @Test
    public void getFileProvenanceTest() throws IOException {

        // TODO make getDefaultFilters a method in base class?
        Map<FileProvenanceFilter, Set<String>> filters = new HashMap<>();

        FileProvenanceFilter filter1 = FileProvenanceFilter.valueOf("processing_status");
        Set<String> params1 = new HashSet(Arrays.asList("success"));
        filters.put(filter1, params1);
        FileProvenanceFilter filter2 = FileProvenanceFilter.valueOf("workflow_run_status");
        Set<String> params2 = new HashSet(Arrays.asList("completed"));
        filters.put(filter2, params2);
        FileProvenanceFilter filter3 = FileProvenanceFilter.valueOf("skip");
        Set<String> params3 = new HashSet(Arrays.asList("false"));
        filters.put(filter3, params3);
        FileProvenanceFilter filter4 = FileProvenanceFilter.valueOf("study");
        Set<String> params4 = new HashSet(Arrays.asList("wholeGenomeAmpKitEval"));
        filters.put(filter4, params4);

        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, dpc);
        String provenanceOutput = ph.getFileProvenanceJson(filters);
        assertNotNull(provenanceOutput);
        // verify method calls on mock object
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        // convert output back to HashSet and check equality
        // NB dpc would normally return a Collection of FileProvenance objects, not Strings
        TypeReference<HashSet<String>> typeRef = new TypeReference<HashSet<String>>() {
        };
        HashSet<String> decodedOutput = om.readValue(provenanceOutput, typeRef);
        assertTrue(dummySet.equals(decodedOutput));

    }

}
