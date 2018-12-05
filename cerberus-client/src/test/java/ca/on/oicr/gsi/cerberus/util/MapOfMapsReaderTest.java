/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.util;

import ca.on.oicr.gsi.cerberus.client.util.MapOfMapsReader;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ibancarz
 */
public class MapOfMapsReaderTest {

    private static ClassLoader classLoader;

    public MapOfMapsReaderTest() {
        classLoader = getClass().getClassLoader();
    }

    @Test
    public void TestLaneByProviderAndId() throws FileNotFoundException, IOException {
        File lpJsonFile = new File(classLoader.getResource("dummyLaneProvenanceByProviderAndId.json").getFile());
        FileInputStream input = new FileInputStream(lpJsonFile);
        MapOfMapsReader reader = new MapOfMapsReader(input);
        Map<String, Map<String, LaneProvenance>> map = reader.readLane();
        assertEquals(map.get("Cambridge").size(), 2);
        assertTrue(map.get("Cambridge").containsKey("Darwin"));
        assertTrue(map.get("Cambridge").containsKey("Trinity"));
        assertEquals(map.get("Oxford").size(), 2);
        assertTrue(map.get("Oxford").containsKey("Balliol"));
        assertTrue(map.get("Oxford").containsKey("Pembroke"));
    }

    
    @Test
    public void TestSampleByProviderAndId() throws FileNotFoundException, IOException {
        File spJsonFile = new File(classLoader.getResource("dummySampleProvenanceByProviderAndId.json").getFile());
        FileInputStream input = new FileInputStream(spJsonFile);
        MapOfMapsReader reader = new MapOfMapsReader(input);
        Map<String, Map<String, SampleProvenance>> map = reader.readSample();
        assertEquals(map.get("Cambridge").size(), 2);
        assertTrue(map.get("Cambridge").containsKey("Peterhouse"));
        assertTrue(map.get("Cambridge").containsKey("Wolfson"));
        assertEquals(map.get("Oxford").size(), 2);
        assertTrue(map.get("Oxford").containsKey("Queen's"));
        assertTrue(map.get("Oxford").containsKey("New"));
    }
    
}
