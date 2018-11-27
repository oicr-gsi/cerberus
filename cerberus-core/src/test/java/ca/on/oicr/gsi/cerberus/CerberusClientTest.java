/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.cerberus.util.ProvenanceType;
import ca.on.oicr.gsi.cerberus.util.ProvenanceAction;
import ca.on.oicr.gsi.cerberus.model.AnalysisProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.FileProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.LaneProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.SampleProvenanceFromJSON;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import static org.apache.commons.collections4.CollectionUtils.isEqualCollection;
import org.apache.commons.collections4.Equator;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author ibancarz
 */
@RunWith(MockitoJUnitRunner.class)
public class CerberusClientTest {

    ProvenanceHttpClient phc = mock(ProvenanceHttpClient.class);
    private static ObjectMapper om;
    private static ClassLoader classLoader;
    private static Map<FileProvenanceFilter, Set<String>> filters1;
    private static Map<FileProvenanceFilter, Set<String>> filters2;
    private static final String PROVIDER_KEY = "weyland-yutani";
    private static final String ID_KEY = "LV-426";
    private final CerberusClient client;

    public CerberusClientTest() {
        om = new ObjectMapper();
        om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        om.configure(SerializationFeature.INDENT_OUTPUT, true);
        classLoader = getClass().getClassLoader();
        filters1 = new HashMap<>();
        filters1.put(FileProvenanceFilter.valueOf("processing_status"), new HashSet(Arrays.asList("success")));
        filters2 = new HashMap<>();
        filters2.put(FileProvenanceFilter.valueOf("study"), new HashSet(Arrays.asList("xenomorph")));
        client = new CerberusClient(phc);
    }


    @Test
    public void getAnalysisProvenanceTest() throws IOException {
        Collection<AnalysisProvenance> apColl = new ArrayList<>();
        File apJsonFile = new File(classLoader.getResource("dummyAnalysisProvenance.json").getFile());
        AnalysisProvenance apExample = new AnalysisProvenanceFromJSON(om.readTree(apJsonFile).get(0));
        apColl.add(apExample);
        byte[] ap = om.writeValueAsBytes(apColl);
        when(phc.getProvenanceJson(eq(ProvenanceType.ANALYSIS.name()))).thenReturn(new ByteArrayInputStream(ap));
        when(phc.getProvenanceJson(eq(ProvenanceType.ANALYSIS.name()),
                eq(ProvenanceAction.INC_FILTERS.name()),
                any(Map.class)
        )).thenReturn(new ByteArrayInputStream(ap));
        Map<String, Collection<AnalysisProvenance>> apByProvider = new HashMap<>();
        apByProvider.put(PROVIDER_KEY, apColl);
        when(phc.getProvenanceJson(
                eq(ProvenanceType.ANALYSIS.name()),
                eq(ProvenanceAction.BY_PROVIDER.name()),
                any(Map.class)
        )).thenReturn(new ByteArrayInputStream(om.writeValueAsBytes(apByProvider)));
        
        AnalysisProvenanceEquator eq = new AnalysisProvenanceEquator();
        Collection<AnalysisProvenance> result;
        result = client.getAnalysisProvenance();
        assertTrue(isEqualCollection(result, apColl, eq));
        result = client.getAnalysisProvenance(filters1);
        assertTrue(isEqualCollection(result, apColl, eq));
        Map<String, Collection<AnalysisProvenance>> resultByProvider = client.getAnalysisProvenanceByProvider(filters1);
        assertTrue(isEqualCollection(resultByProvider.get(PROVIDER_KEY), apColl, eq));
    }

    @Test
    public void getFileProvenanceTest() throws IOException {
        Collection<FileProvenance> fpColl = new ArrayList<>();
        File fpJsonFile = new File(classLoader.getResource("dummyFileProvenance.json").getFile());
        fpColl.add(new FileProvenanceFromJSON(om.readTree(fpJsonFile).get(0)));
        byte[] fp = om.writeValueAsBytes(fpColl);
        when(phc.getProvenanceJson(eq(ProvenanceType.FILE.name()))).thenReturn(new ByteArrayInputStream(fp));
        when(phc.getProvenanceJson(
                eq(ProvenanceType.FILE.name()),
                eq(ProvenanceAction.INC_FILTERS.name()),
                any(Map.class)
        )).thenReturn(new ByteArrayInputStream(fp));
        when(phc.getProvenanceJson(
                eq(ProvenanceType.FILE.name()),
                eq(ProvenanceAction.INC_EXC_FILTERS.name()),
                any(Map.class),
                any(Map.class)
        )).thenReturn(new ByteArrayInputStream(fp));
        FileProvenanceEquator eq = new FileProvenanceEquator();
        Collection<FileProvenance> result;
        result = client.getFileProvenance();
        assertTrue(isEqualCollection(result, fpColl, eq));
        result = client.getFileProvenance(filters1);
        assertTrue(isEqualCollection(result, fpColl, eq));
        result = client.getFileProvenance(filters1, filters2);
        assertTrue(isEqualCollection(result, fpColl, eq));
    }

    @Test
    public void getLaneProvenanceTest() throws IOException {
        Collection<LaneProvenance> lpColl = new ArrayList<>();
        File lpJsonFile = new File(classLoader.getResource("dummyLaneProvenance.json").getFile());
        LaneProvenance lpExample = new LaneProvenanceFromJSON(om.readTree(lpJsonFile).get(0));
        lpColl.add(lpExample);
        byte[] lp = om.writeValueAsBytes(lpColl);
        when(phc.getProvenanceJson(eq(ProvenanceType.LANE.name()))).thenReturn(new ByteArrayInputStream(lp));
        when(phc.getProvenanceJson(
                eq(ProvenanceType.LANE.name()),
                eq(ProvenanceAction.INC_FILTERS.name()),
                any(Map.class)
        )).thenReturn(new ByteArrayInputStream(lp));
        Map<String, Collection<LaneProvenance>> lpByProvider = new HashMap<>();
        lpByProvider.put(PROVIDER_KEY, lpColl);
        when(phc.getProvenanceJson(
                eq(ProvenanceType.LANE.name()),
                eq(ProvenanceAction.BY_PROVIDER.name()),
                any(Map.class)
        )).thenReturn(new ByteArrayInputStream(om.writeValueAsBytes(lpByProvider)));
        Map<String, Map<String, LaneProvenance>> lpByProviderAndId = new HashMap<>();
        Map<String, LaneProvenance> lpById = new HashMap<>();
        lpById.put(ID_KEY, lpExample);
        lpByProviderAndId.put(PROVIDER_KEY, lpById);
        when(phc.getProvenanceJson(
                eq(ProvenanceType.LANE.name()),
                eq(ProvenanceAction.BY_PROVIDER_AND_ID.name()),
                any(Map.class)
        )).thenReturn(new ByteArrayInputStream(om.writeValueAsBytes(lpByProviderAndId)));
        LaneProvenanceEquator eq = new LaneProvenanceEquator();
        Collection<LaneProvenance> result;
        result = client.getLaneProvenance();
        assertTrue(isEqualCollection(result, lpColl, eq));
        result = client.getLaneProvenance(filters1);
        assertTrue(isEqualCollection(result, lpColl, eq));
        Map<String, Collection<LaneProvenance>> resultByProvider = client.getLaneProvenanceByProvider(filters1);
        assertTrue(isEqualCollection(resultByProvider.get(PROVIDER_KEY), lpColl, eq));
        Map<String, Map<String, LaneProvenance>> resultByProviderAndId = client.getLaneProvenanceByProviderAndId(filters1);
        assertTrue(eq.equate(resultByProviderAndId.get(PROVIDER_KEY).get(ID_KEY), lpExample));
    }

    @Test
    public void getSampleProvenanceTest() throws IOException {
        Collection<SampleProvenance> spColl = new ArrayList<>();
        File spJsonFile = new File(classLoader.getResource("dummySampleProvenance.json").getFile());
        SampleProvenance spExample = new SampleProvenanceFromJSON(om.readTree(spJsonFile).get(0));
        spColl.add(spExample);
        byte[] sp = om.writeValueAsBytes(spColl);
        when(phc.getProvenanceJson(eq(ProvenanceType.SAMPLE.name()))).thenReturn(new ByteArrayInputStream(sp));
        when(phc.getProvenanceJson(
                eq(ProvenanceType.SAMPLE.name()),
                eq(ProvenanceAction.INC_FILTERS.name()),
                any(Map.class)
        )).thenReturn(new ByteArrayInputStream(sp));
        Map<String, Collection<SampleProvenance>> spByProvider = new HashMap<>();
        spByProvider.put(PROVIDER_KEY, spColl);
        when(phc.getProvenanceJson(
                eq(ProvenanceType.SAMPLE.name()),
                eq(ProvenanceAction.BY_PROVIDER.name()),
                any(Map.class)
        )).thenReturn(new ByteArrayInputStream(om.writeValueAsBytes(spByProvider)));
        Map<String, Map<String, SampleProvenance>> spByProviderAndId = new HashMap<>();
        Map<String, SampleProvenance> spById = new HashMap<>();
        spById.put(ID_KEY, spExample);
        spByProviderAndId.put(PROVIDER_KEY, spById);
        when(phc.getProvenanceJson(
                eq(ProvenanceType.SAMPLE.name()),
                eq(ProvenanceAction.BY_PROVIDER_AND_ID.name()),
                any(Map.class)
        )).thenReturn(new ByteArrayInputStream(om.writeValueAsBytes(spByProviderAndId)));
        SampleProvenanceEquator eq = new SampleProvenanceEquator();
        Collection<SampleProvenance> result;
        result = client.getSampleProvenance();
        assertTrue(isEqualCollection(result, spColl, eq));
        result = client.getSampleProvenance(filters1);
        assertTrue(isEqualCollection(result, spColl, eq));
        Map<String, Collection<SampleProvenance>> resultByProvider = client.getSampleProvenanceByProvider(filters1);
        assertTrue(isEqualCollection(resultByProvider.get(PROVIDER_KEY), spColl, eq));
        Map<String, Map<String, SampleProvenance>> resultByProviderAndId = client.getSampleProvenanceByProviderAndId(filters1);
        assertTrue(eq.equate(resultByProviderAndId.get(PROVIDER_KEY).get(ID_KEY), spExample));
    }

    public static class AnalysisProvenanceEquator implements Equator<AnalysisProvenance> {

        @Override
        public boolean equate(AnalysisProvenance ap1, AnalysisProvenance ap2) {
            Integer id1 = ap1.getProcessingId();
            Integer id2 = ap2.getProcessingId();
            return id1.equals(id2);
        }

        @Override
        public int hash(AnalysisProvenance ap) {
            return ap.getProcessingId();
        }
    }

    public static class FileProvenanceEquator implements Equator<FileProvenance> {

        @Override
        public boolean equate(FileProvenance fp1, FileProvenance fp2) {
            // md5sum may be null, so use SWID instead
            Integer id1 = fp1.getFileSWID();
            Integer id2 = fp2.getFileSWID();
            return id1.equals(id2);
        }

        @Override
        public int hash(FileProvenance fp) {
            return fp.getFileSWID();
        }
    }

    public static class LaneProvenanceEquator implements Equator<LaneProvenance> {

        @Override
        public boolean equate(LaneProvenance lp1, LaneProvenance lp2) {
            String id1 = lp1.getLaneProvenanceId();
            String id2 = lp2.getLaneProvenanceId();
            return id1.equals(id2);
        }

        @Override
        public int hash(LaneProvenance lp) {
            return lp.getLaneProvenanceId().hashCode();
        }
    }

    public static class SampleProvenanceEquator implements Equator<SampleProvenance> {

        @Override
        public boolean equate(SampleProvenance sp1, SampleProvenance sp2) {
            String id1 = sp1.getSampleProvenanceId();
            String id2 = sp2.getSampleProvenanceId();
            return id1.equals(id2);
        }

        @Override
        public int hash(SampleProvenance sp) {
            return sp.getSampleProvenanceId().hashCode();
        }
    }
}
