/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.cerberus.util.ProvenanceType;
import ca.on.oicr.gsi.cerberus.webservice.ProvenanceHandler;
import ca.on.oicr.gsi.cerberus.util.ProvenanceAction;
import ca.on.oicr.gsi.cerberus.model.AnalysisProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.FileProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.LaneProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.SampleProvenanceFromJSON;
import ca.on.oicr.gsi.provenance.DefaultProvenanceClient;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import static org.apache.commons.io.FileUtils.readFileToString;
import org.apache.commons.io.output.NullOutputStream;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
public class HandlerTest {

    private static ClassLoader classLoader;
    private final String providerSettings;
    private final HashSet<AnalysisProvenance> dummyAnalysisProvenance;
    private final HashSet<FileProvenance> dummyFileProvenance;
    private final HashSet<LaneProvenance> dummyLaneProvenance;
    private final HashSet<SampleProvenance> dummySampleProvenance;
    private final Map<FileProvenanceFilter, Set<String>> filters;
    private final Map<FileProvenanceFilter, Set<String>> excFilters;
    private final DefaultProvenanceClient dpc = mock(DefaultProvenanceClient.class);
    private final AnalysisProvenance ap;
    private final FileProvenance fp;
    private final LaneProvenance lp;
    private final SampleProvenance sp;
    private final ObjectMapper om;
    private final Map<String, Collection<AnalysisProvenance>> abp;
    private final Map<String, Collection<LaneProvenance>> lbp;
    private final Map<String, Collection<SampleProvenance>> sbp;
    private final Map<String, Map<String, LaneProvenance>> lbpid;
    private final Map<String, Map<String, SampleProvenance>> sbpid;

    private static final String DUMMY_PROVIDER = "dummy_provider";
    private static final String DUMMY_ID = "dummy_id";

    public HandlerTest() throws IOException {

        om = new ObjectMapper();
        classLoader = getClass().getClassLoader();
        File apJsonFile = new File(classLoader.getResource("dummyAnalysisProvenance.json").getFile());
        ap = new AnalysisProvenanceFromJSON(om.readTree(apJsonFile).get(0));
        File fpJsonFile = new File(classLoader.getResource("dummyFileProvenance.json").getFile());
        fp = new FileProvenanceFromJSON(om.readTree(fpJsonFile).get(0));
        File lpJsonFile = new File(classLoader.getResource("dummyLaneProvenance.json").getFile());
        lp = new LaneProvenanceFromJSON(om.readTree(lpJsonFile).get(0));
        File spJsonFile = new File(classLoader.getResource("dummySampleProvenance.json").getFile());
        sp = new SampleProvenanceFromJSON(om.readTree(spJsonFile).get(0));
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
        when(dpc.getAnalysisProvenance(any(Map.class))).thenReturn(dummyAnalysisProvenance);
        when(dpc.getAnalysisProvenance()).thenReturn(dummyAnalysisProvenance);

        when(dpc.getFileProvenance(any(Map.class), any(Map.class))).thenReturn(dummyFileProvenance);
        when(dpc.getFileProvenance(any(Map.class))).thenReturn(dummyFileProvenance);
        when(dpc.getFileProvenance()).thenReturn(dummyFileProvenance);
        when(dpc.getLaneProvenance(any(Map.class))).thenReturn(dummyLaneProvenance);
        when(dpc.getLaneProvenance()).thenReturn(dummyLaneProvenance);
        when(dpc.getSampleProvenance(any(Map.class))).thenReturn(dummySampleProvenance);
        when(dpc.getSampleProvenance()).thenReturn(dummySampleProvenance);

        abp = new HashMap<>();
        abp.put(DUMMY_PROVIDER, dummyAnalysisProvenance);
        when(dpc.getAnalysisProvenanceByProvider(any(Map.class))).thenReturn(abp);
        lbp = new HashMap<>();
        lbp.put(DUMMY_PROVIDER, dummyLaneProvenance);
        when(dpc.getLaneProvenanceByProvider(any(Map.class))).thenReturn(lbp);
        sbp = new HashMap<>();
        sbp.put(DUMMY_PROVIDER, dummySampleProvenance);
        when(dpc.getSampleProvenanceByProvider(any(Map.class))).thenReturn(sbp);

        lbpid = new HashMap<>();
        Map<String, LaneProvenance> laneTmp = new HashMap<>();
        laneTmp.put(DUMMY_ID, lp);
        lbpid.put(DUMMY_PROVIDER, laneTmp);
        when(dpc.getLaneProvenanceByProviderAndId(any(Map.class))).thenReturn(lbpid);
        sbpid = new HashMap<>();
        Map<String, SampleProvenance> sampleTmp = new HashMap<>();
        sampleTmp.put(DUMMY_ID, sp);
        sbpid.put(DUMMY_PROVIDER, sampleTmp);
        when(dpc.getSampleProvenanceByProviderAndId(any(Map.class))).thenReturn(sbpid);
        
        File providerFile = new File(classLoader.getResource("providerSettings.json").getFile());
        providerSettings = readFileToString(providerFile);
        
    }

    @Test
    public void constructorTest() throws IOException {

        OutputStreamWriter writer = new OutputStreamWriter(new NullOutputStream());

        ProvenanceHandler ph1 = new ProvenanceHandler(providerSettings, writer);
        assertNotNull(ph1);

        ProvenanceHandler ph2 = new ProvenanceHandler(providerSettings, writer, dpc);
        assertNotNull(ph2);

    }

    @Test
    public void noFilterTest() throws IOException {
        for (ProvenanceType type : ProvenanceType.values()) {
            StringWriter writer = new StringWriter();
            ProvenanceHandler ph = new ProvenanceHandler(providerSettings, writer, dpc);
            ph.writeProvenanceJson(type.name());
            JsonNode outNode = om.readTree(writer.toString()).get(0); // output is a 1-element list
            JsonNode inNode = getInputNode(type);
            assertTrue(inNode.equals(outNode));
        }
        // verify method calls on mock object
        verify(dpc, times(4)).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc, times(4)).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc, times(4)).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getAnalysisProvenance();
        verify(dpc).getFileProvenance();
        verify(dpc).getLaneProvenance();
        verify(dpc).getSampleProvenance();
    }

    @Test
    public void incFiltersTest() throws IOException {
        for (ProvenanceType type : ProvenanceType.values()) {
            StringWriter writer = new StringWriter();
            ProvenanceHandler ph = new ProvenanceHandler(providerSettings, writer, dpc);
            ph.writeProvenanceJson(type.name(), ProvenanceAction.INC_FILTERS.name(), filters);
            JsonNode outNode = om.readTree(writer.toString()).get(0); // output is a 1-element list
            JsonNode inNode = getInputNode(type);
            assertTrue(inNode.equals(outNode));
        }
        // verify method calls on mock object
        verify(dpc, times(4)).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc, times(4)).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc, times(4)).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getAnalysisProvenance(anyMap());
        verify(dpc).getFileProvenance(anyMap());
        verify(dpc).getLaneProvenance(anyMap());
        verify(dpc).getSampleProvenance(anyMap());
    }

    @Test
    public void incExcFiltersTest() throws IOException {
        ProvenanceType type = ProvenanceType.FILE;
        StringWriter writer = new StringWriter();
        ProvenanceHandler ph = new ProvenanceHandler(providerSettings, writer, dpc);
        ph.writeProvenanceJson(type.name(), ProvenanceAction.INC_EXC_FILTERS.name(), filters, excFilters);
        JsonNode outNode = om.readTree(writer.toString()).get(0); // output is a 1-element list
        JsonNode inNode = getInputNode(type);
        assertTrue(inNode.equals(outNode));

        // verify method calls on mock object
        verify(dpc).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getFileProvenance(anyMap(), anyMap());
    }

    @Test
    public void byProviderTest() throws IOException {
        ProvenanceType[] types = {ProvenanceType.ANALYSIS, ProvenanceType.LANE, ProvenanceType.SAMPLE};
        for (ProvenanceType type : types) {
            StringWriter writer = new StringWriter();
            ProvenanceHandler ph = new ProvenanceHandler(providerSettings, writer, dpc);
            ph.writeProvenanceJson(type.name(), ProvenanceAction.BY_PROVIDER.name(), filters);
            JsonNode outNode = om.readTree(writer.toString()).get(DUMMY_PROVIDER).get(0);
            JsonNode inNode = getInputNode(type);
            assertTrue(inNode.equals(outNode));
        }
        // verify method calls on mock object
        verify(dpc, times(3)).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc, times(3)).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc, times(3)).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getAnalysisProvenanceByProvider(anyMap());
        verify(dpc).getLaneProvenanceByProvider(anyMap());
        verify(dpc).getSampleProvenanceByProvider(anyMap());
    }

    @Test
    public void byProviderAndIdTest() throws IOException {

        ProvenanceType[] types = {ProvenanceType.LANE, ProvenanceType.SAMPLE};
        for (ProvenanceType type : types) {
            StringWriter writer = new StringWriter();
            ProvenanceHandler ph = new ProvenanceHandler(providerSettings, writer, dpc);
            ph.writeProvenanceJson(type.name(), ProvenanceAction.BY_PROVIDER_AND_ID.name(), filters);
            JsonNode outNode = om.readTree(writer.toString()).get(DUMMY_PROVIDER).get(DUMMY_ID);
            JsonNode inNode = getInputNode(type);
            assertTrue(inNode.equals(outNode));
        }
        // verify method calls on mock object
        verify(dpc, times(2)).registerAnalysisProvenanceProvider(anyString(), anyObject());
        verify(dpc, times(2)).registerLaneProvenanceProvider(anyString(), anyObject());
        verify(dpc, times(2)).registerSampleProvenanceProvider(anyString(), anyObject());
        verify(dpc).getLaneProvenanceByProviderAndId(anyMap());
        verify(dpc).getSampleProvenanceByProviderAndId(anyMap());

    }

    private JsonNode getInputNode(ProvenanceType type) {
        JsonNode inNode;
        switch (type) {
            case ANALYSIS:
                inNode = om.valueToTree(ap);
                break;
            case FILE:
                inNode = om.valueToTree(fp);
                break;
            case LANE:
                inNode = om.valueToTree(lp);
                break;
            case SAMPLE:
                inNode = om.valueToTree(sp);
                break;
            default:
                throw new RuntimeException();
        }
        return inNode;
    }
}