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
public class TestSampleProvenanceFromJSON {

    private static ObjectMapper om;

    public TestSampleProvenanceFromJSON() {
        om = new ObjectMapper();
        om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        om.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    @Test
    public void testProvenance() throws IOException {

        ClassLoader classLoader = getClass().getClassLoader();
        File jsonFile = new File(classLoader.getResource("dummySampleProvenance.json").getFile());
        // JsonNode input
        JsonNode inputNode = om.readTree(jsonFile).get(0);
        SampleProvenanceFromJSON sp1 = new SampleProvenanceFromJSON(inputNode);
        assertNotNull(sp1);
        JsonNode outputNode1 = om.readTree(om.writeValueAsString(sp1));
        assertTrue(inputNode.equals(outputNode1));
        // same again, with String input
        String inputString = inputNode.toString();
        SampleProvenanceFromJSON sp2 = new SampleProvenanceFromJSON(inputString);
        assertNotNull(sp2);
        JsonNode outputNode2 = om.readTree(om.writeValueAsString(sp2));
        assertTrue(inputNode.equals(outputNode2));
    }

}
