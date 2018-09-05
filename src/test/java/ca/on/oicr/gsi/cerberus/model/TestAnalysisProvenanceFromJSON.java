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
public class TestAnalysisProvenanceFromJSON {

    private static ObjectMapper om;

    public TestAnalysisProvenanceFromJSON() {
        om = new ObjectMapper();
        om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        om.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    @Test
    public void testProvenance() throws IOException {

        ClassLoader classLoader = getClass().getClassLoader();
        File jsonFile = new File(classLoader.getResource("dummyAnalysisProvenance.json").getFile());
        // JsonNode input
        JsonNode inputNode = om.readTree(jsonFile).get(0);
        AnalysisProvenanceFromJSON ap1 = new AnalysisProvenanceFromJSON(inputNode);
        assertNotNull(ap1);
        JsonNode outputNode1 = om.readTree(om.writeValueAsString(ap1));
        assertTrue(inputNode.equals(outputNode1));
    }

}
