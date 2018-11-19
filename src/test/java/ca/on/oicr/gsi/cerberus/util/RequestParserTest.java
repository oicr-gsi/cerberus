/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.util;

import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ibancarz
 */
public class RequestParserTest {

    private static String input;
    private final Map<FileProvenanceFilter, Set<String>> filters1;
    private final Map<FileProvenanceFilter, Set<String>> filters2;
    private final ObjectMapper om;
    private static final String PROVENANCE_TYPE = "FILE";
    private static final String PROVENANCE_ACTION = "INC_EXC_FILTERS";

    public RequestParserTest() throws IOException {
        om = new ObjectMapper();
        // populate with a simple default filter
        String name1 = "processing_status";
        FileProvenanceFilter fpf1 = FileProvenanceFilter.valueOf(name1);
        filters1 = new HashMap<>();
        Set<String> values1 = new HashSet(Arrays.asList("success"));
        filters1.put(fpf1, values1);
        String name2 = "study";
        FileProvenanceFilter fpf2 = FileProvenanceFilter.valueOf(name2);
        filters2 = new HashMap<>();
        Set<String> values2 = new HashSet(Arrays.asList("xenomorph"));
        filters2.put(fpf2, values2);

    }

    @Before
    public void setUp() throws IOException {
        // create the JSON input string
        // construct a Map from scratch instead of converting getTestFilters() output to string
        // the latter will have "processing-status" instead of "processing_status"
        Map inputMap = new HashMap();
        Map<String, Set<String>> filterStrings1 = new HashMap<>();
        filterStrings1.put("processing_status", new HashSet(Arrays.asList("success")));
        Map<String, Set<String>> filterStrings2 = new HashMap<>();
        filterStrings2.put("study", new HashSet(Arrays.asList("xenomorph")));
        inputMap.put(PostField.INC_FILTER, filterStrings1);
        inputMap.put(PostField.EXC_FILTER, filterStrings2);
        inputMap.put(PostField.ACTION, PROVENANCE_ACTION);
        inputMap.put(PostField.TYPE, PROVENANCE_TYPE);
        input = om.writeValueAsString(inputMap);
    }

    @Test
    public void testConstructor() throws IOException {
        RequestParser rp = new RequestParser(input);
        assertNotNull(rp);
    }

    @Test
    public void testExcFilterSettings() throws IOException {
        RequestParser rp = new RequestParser(input);
        Map<FileProvenanceFilter, Set<String>> filtersOut = rp.getExcFilters();
        assertNotNull(filtersOut);
        assertTrue(filtersOut.equals(filters2));
    }
    
    @Test
    public void testIncFilterSettings() throws IOException {
        RequestParser rp = new RequestParser(input);
        Map<FileProvenanceFilter, Set<String>> filtersOut = rp.getIncFilters();
        assertNotNull(filtersOut);
        assertTrue(filtersOut.equals(filters1));
    }
    
    @Test
    public void testProvenanceAction() throws IOException {
        RequestParser rp = new RequestParser(input);
        String provenanceAction = rp.getProvenanceAction();
        assertTrue(provenanceAction.equals(PROVENANCE_ACTION));
    }
    
    @Test
    public void testProvenanceType() throws IOException {
        RequestParser rp = new RequestParser(input);
        String provenanceType = rp.getProvenanceType();
        assertTrue(provenanceType.equals(PROVENANCE_TYPE));
    }

}
