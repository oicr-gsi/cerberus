/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.File;
import java.io.IOException;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ibancarz
 */
public class TestLaneProvenanceFromJSON {
    
    private static ObjectMapper om;

    public TestLaneProvenanceFromJSON() {
        om = new ObjectMapper();
        om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        om.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    @Test
    public void testProvenance() throws IOException {

        ClassLoader classLoader = getClass().getClassLoader();
        File jsonFile = new File(classLoader.getResource("dummyLaneProvenance.json").getFile());
        // JsonNode input
        JsonNode inputNode = om.readTree(jsonFile).get(0);
        LaneProvenanceFromJSON lp1 = new LaneProvenanceFromJSON(inputNode);
        assertNotNull(lp1);
        JsonNode outputNode1 = om.readTree(om.writeValueAsString(lp1));
        assertTrue(inputNode.equals(outputNode1));
        // same again, with String input
        String inputString = inputNode.toString();
        LaneProvenanceFromJSON lp2 = new LaneProvenanceFromJSON(inputString);
        assertNotNull(lp2);
        JsonNode outputNode2 = om.readTree(om.writeValueAsString(lp2));
        assertTrue(inputNode.equals(outputNode2));
    }   
}
