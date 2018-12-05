/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.util;

import ca.on.oicr.gsi.cerberus.client.util.MapOfCollectionsReader;
import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author ibancarz
 */
public class MapOfCollectionsReaderTest {
    
    
    private static ClassLoader classLoader;

    public MapOfCollectionsReaderTest() {
        classLoader = getClass().getClassLoader();
    }

    @Test
    public void TestAnalysisByProvider() throws FileNotFoundException, IOException {
        File apJsonFile = new File(classLoader.getResource("dummyAnalysisProvenanceByProvider.json").getFile());
        FileInputStream input = new FileInputStream(apJsonFile);
        MapOfCollectionsReader reader = new MapOfCollectionsReader(input);
        Map<String, Collection<AnalysisProvenance>> map = reader.readAnalysis();
        assertEquals(map.get("Cambridge").size(), 2);
        assertEquals(map.get("Oxford").size(), 2);  
    }
    
    @Test
    public void TestLaneByProvider() throws FileNotFoundException, IOException {
        File lpJsonFile = new File(classLoader.getResource("dummyLaneProvenanceByProvider.json").getFile());
        FileInputStream input = new FileInputStream(lpJsonFile);
        MapOfCollectionsReader reader = new MapOfCollectionsReader(input);
        Map<String, Collection<LaneProvenance>> map = reader.readLane();
        assertEquals(map.get("Cambridge").size(), 2);
        assertEquals(map.get("Oxford").size(), 2);  
    }

    @Test
    public void TestSampleByProvider() throws FileNotFoundException, IOException {
        File spJsonFile = new File(classLoader.getResource("dummySampleProvenanceByProvider.json").getFile());
        FileInputStream input = new FileInputStream(spJsonFile);
        MapOfCollectionsReader reader = new MapOfCollectionsReader(input);
        Map<String, Collection<SampleProvenance>> map = reader.readSample();
        assertEquals(map.get("Cambridge").size(), 2);
        assertEquals(map.get("Oxford").size(), 2);  
    }
}
