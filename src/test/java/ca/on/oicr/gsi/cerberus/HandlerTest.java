/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.provenance.DefaultProvenanceClient;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyMap;
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

    private String providerSettings;
    private final ObjectMapper om = mock(ObjectMapper.class);
    private final HashSet<FileProvenance> dummyAnalysisProvenance;
    private final HashSet<FileProvenance> dummyFileProvenance;
    private final HashSet<LaneProvenance> dummyLaneProvenance;
    private final HashSet<SampleProvenance> dummySampleProvenance;
    private final Map<FileProvenanceFilter, Set<String>> filters;
    private final Map<FileProvenanceFilter, Set<String>> excFilters;
    private final DefaultProvenanceClient dpc = mock(DefaultProvenanceClient.class);
    private final FileProvenance ap = mock(FileProvenance.class);
    private final FileProvenance fp = mock(FileProvenance.class);
    private final LaneProvenance lp = mock(LaneProvenance.class);
    private final SampleProvenance sp = mock(SampleProvenance.class);
    private final String dummyJson;

    public HandlerTest() throws JsonProcessingException {
        dummyAnalysisProvenance = new HashSet<>();
        dummyAnalysisProvenance.add(ap);
        dummyFileProvenance = new HashSet<>();
        dummyFileProvenance.add(fp);
        dummyLaneProvenance = new HashSet<>();
        dummyLaneProvenance.add(lp);
        dummySampleProvenance = new HashSet<>();
        dummySampleProvenance.add(sp);
        filters = new HashMap<>();
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
        excFilters = new HashMap<>();
        FileProvenanceFilter filter5 = FileProvenanceFilter.valueOf("study");
        Set<String> params5 = new HashSet(Arrays.asList("xenomorph"));
        excFilters.put(filter5, params5);
        when(dpc.getFileProvenance(any(Map.class))).thenReturn(dummyAnalysisProvenance);
        when(dpc.getFileProvenance()).thenReturn(dummyAnalysisProvenance);
        when(dpc.getFileProvenance(any(Map.class))).thenReturn(dummyFileProvenance);
        when(dpc.getFileProvenance()).thenReturn(dummyFileProvenance);
        when(dpc.getLaneProvenance(any(Map.class))).thenReturn(dummyLaneProvenance);
        when(dpc.getLaneProvenance()).thenReturn(dummyLaneProvenance);
        when(dpc.getSampleProvenance(any(Map.class))).thenReturn(dummySampleProvenance);
        when(dpc.getSampleProvenance()).thenReturn(dummySampleProvenance);
        dummyJson = "[\"foo\",]";
        when(om.writeValueAsString(anyCollection())).thenReturn(dummyJson);
    }

    @Before
    public void setUp() throws IOException {
        // place in setup, not constructor, to avoid overridable method call warning
        providerSettings = new ObjectMapper().writeValueAsString(getTestProviderSettings());
    }

    @Test
    public void constructorTest() throws IOException {

        ProvenanceHandler ph1 = new ProvenanceHandler(providerSettings);
        assertNotNull(ph1);

        ProvenanceHandler ph2 = new ProvenanceHandler(providerSettings, dpc, om);
        assertNotNull(ph2);

    }

    @Test
    public void noFilterTest() throws IOException {
        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, dpc, om);
        String output;
        output = ph.getProvenanceJson(ProvenanceType.ANALYSIS.name());
        assertTrue(dummyJson.equals(output));
        output = ph.getProvenanceJson(ProvenanceType.FILE.name());
        assertTrue(dummyJson.equals(output));
        output = ph.getProvenanceJson(ProvenanceType.LANE.name());
        assertTrue(dummyJson.equals(output));
        output = ph.getProvenanceJson(ProvenanceType.SAMPLE.name());
        assertTrue(dummyJson.equals(output));
        // verify method calls on mock object
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getAnalysisProvenance();
        verify(dpc).getFileProvenance();
        verify(dpc).getLaneProvenance();
        verify(dpc).getSampleProvenance();
    }

    @Test
    public void incFilterTest() throws IOException {
        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, dpc, om);
        String output;
        output = ph.getProvenanceJson(ProvenanceType.ANALYSIS.name(), ProvenanceAction.INC_FILTERS.name(), filters);
        assertTrue(dummyJson.equals(output));
        output = ph.getProvenanceJson(ProvenanceType.FILE.name(), ProvenanceAction.INC_FILTERS.name(), filters);
        assertTrue(dummyJson.equals(output));
        output = ph.getProvenanceJson(ProvenanceType.LANE.name(), ProvenanceAction.INC_FILTERS.name(), filters);
        assertTrue(dummyJson.equals(output));
        output = ph.getProvenanceJson(ProvenanceType.SAMPLE.name(), ProvenanceAction.INC_FILTERS.name(), filters);
        assertTrue(dummyJson.equals(output));
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getAnalysisProvenance(anyMap());
        verify(dpc).getFileProvenance(anyMap());
        verify(dpc).getLaneProvenance(anyMap());
        verify(dpc).getSampleProvenance(anyMap());
    }

    @Test
    public void incExcFilterTest() throws IOException {
        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, dpc, om);
        String output;
        output = ph.getProvenanceJson(ProvenanceType.FILE.name(), ProvenanceAction.INC_EXC_FILTERS.name(), filters, excFilters);
        assertTrue(dummyJson.equals(output));
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getFileProvenance(anyMap(), anyMap());
    }

    @Test
    public void byProviderTest() throws IOException {
        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, dpc, om);
        String output;
        output = ph.getProvenanceJson(ProvenanceType.ANALYSIS.name(), ProvenanceAction.BY_PROVIDER.name(), filters);
        assertTrue(dummyJson.equals(output));
        output = ph.getProvenanceJson(ProvenanceType.LANE.name(), ProvenanceAction.BY_PROVIDER.name(), filters);
        assertTrue(dummyJson.equals(output));
        output = ph.getProvenanceJson(ProvenanceType.SAMPLE.name(), ProvenanceAction.BY_PROVIDER.name(), filters);
        assertTrue(dummyJson.equals(output));
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getAnalysisProvenanceByProvider(anyMap());
        verify(dpc).getLaneProvenanceByProvider(anyMap());
        verify(dpc).getSampleProvenanceByProvider(anyMap());
    }

    @Test
    public void byProviderAndIdTest() throws IOException {
        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, dpc, om);
        String output;
        output = ph.getProvenanceJson(ProvenanceType.LANE.name(), ProvenanceAction.BY_PROVIDER_AND_ID.name(), filters);
        assertTrue(dummyJson.equals(output));
        output = ph.getProvenanceJson(ProvenanceType.SAMPLE.name(), ProvenanceAction.BY_PROVIDER_AND_ID.name(), filters);
        assertTrue(dummyJson.equals(output));
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getLaneProvenanceByProviderAndId(anyMap());
        verify(dpc).getSampleProvenanceByProviderAndId(anyMap());
    }

    @Test
    public void analysisProvenanceTest() throws IOException {
        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, dpc, om);
        String output1 = ph.analysisNoFilter();
        assertNotNull(output1);
        String output2 = ph.analysisIncFilters(filters);
        assertNotNull(output2);
        String output3 = ph.analysisByProvider(filters);
        assertNotNull(output3);
        // verify method calls on mock object
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getAnalysisProvenance();
        verify(dpc).getAnalysisProvenance(anyMap());
        verify(dpc).getAnalysisProvenanceByProvider(anyMap());
        // check output is expected value from the mock ObjectMapper
        for (String output : new String[]{ output1, output2, output3 }) {
            assertTrue(dummyJson.equals(output));
        }
    }

    @Test
    public void fileProvenanceTest() throws IOException {
        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, dpc, om);
        String output1 = ph.fileNoFilter();
        assertNotNull(output1);
        String output2 = ph.fileIncFilters(filters);
        assertNotNull(output2);
        String output3 = ph.fileIncExcFilters(filters, excFilters);
        assertNotNull(output3);
        // verify method calls on mock object
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getFileProvenance(anyMap(), anyMap());
        verify(dpc).getFileProvenance(anyMap());
        verify(dpc).getFileProvenance();
        // check output is expected value from the mock ObjectMapper
        for (String output : new String[]{ output1, output2, output3 }) {
            assertTrue(dummyJson.equals(output));
        }
    }

    @Test
    public void getLaneProvenanceTest() throws IOException {
        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, dpc, om);
        String output1 = ph.laneNoFilter();
        assertNotNull(output1);
        String output2 = ph.laneIncFilters(filters);
        assertNotNull(output2);
        String output3 = ph.laneByProvider(filters);
        assertNotNull(output3);
        String provenanceOutput4 = ph.laneByProviderAndId(filters);
        assertNotNull(provenanceOutput4);
        // verify method calls on mock object
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getLaneProvenance();
        verify(dpc).getLaneProvenance(anyMap());
        verify(dpc).getLaneProvenanceByProvider(anyMap());
        verify(dpc).getLaneProvenanceByProviderAndId(anyMap());
        // check output is expected value from the mock ObjectMapper
        for (String output : new String[]{ output1, output2, output3 }) {
            assertTrue(dummyJson.equals(output));
        }
    }

    @Test
    public void getSampleProvenanceTest() throws IOException {
        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, dpc, om);
        String output1 = ph.sampleNoFilter();
        assertNotNull(output1);
        String output2 = ph.sampleIncFilters(filters);
        assertNotNull(output2);
        String output3 = ph.sampleByProvider(filters);
        assertNotNull(output3);
        String output4 = ph.sampleByProviderAndId(filters);
        assertNotNull(output4);
        // verify method calls on mock object
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getSampleProvenance(anyMap());
        verify(dpc).getSampleProvenance();
        // check output is expected value from the mock ObjectMapper
        for (String output : new String[]{ output1, output2, output3, output4 }) {
            assertTrue(dummyJson.equals(output));
        }
    }

}
