/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
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
public class RequestParserTest extends Base {

    private static String input;
    private final Map<FileProvenanceFilter, Set<String>> filters;
    private ArrayList<Map> providerSettings;
    private final ObjectMapper om;

    public RequestParserTest() throws IOException {
        om = new ObjectMapper();
        // populate with a simple default filter
        String name = "processing_status";
        FileProvenanceFilter fpf = FileProvenanceFilter.valueOf(name);
        filters = new HashMap();
        Set<String> values = new HashSet(Arrays.asList("success"));
        filters.put(fpf, values);

    }

    @Before
    public void setUp() throws IOException {
        // done here instead of in constructor to safely use overridable method
        providerSettings = getTestProviderSettings();
        // create the JSON input string
        // construct a Map from scratch instead of converting getTestFilters() output to string
        // the latter will have "processing-status" instead of "processing_status"
        Map inputMap = new HashMap();
        Map<String, Set<String>> filterStrings = new HashMap<>();
        filterStrings.put("processing_status", new HashSet(Arrays.asList("success")));
        inputMap.put("filter_settings", filterStrings);
        inputMap.put("provider_settings", providerSettings);
        input = om.writeValueAsString(inputMap);
    }

    @Test
    public void testConstructor() throws IOException {
        RequestParser rp = new RequestParser(input);
        assertNotNull(rp);
    }

    @Test
    public void testFilterSettings() throws IOException {
        RequestParser rp = new RequestParser(input);
        Map<FileProvenanceFilter, Set<String>> filtersOut = rp.getFilters();
        assertNotNull(filtersOut);
        assertTrue(filtersOut.equals(filters));
    }

    @Test
    public void testProviderSettings() throws IOException {

        RequestParser rp = new RequestParser(input);
        String providerSettingsOut = rp.getProviderSettings();
        assertNotNull(providerSettingsOut);

        // Compare by converting input HashMap and output String to JsonNodes
        String providerSettingsIn = om.writeValueAsString(providerSettings);
        JsonNode rootIn = om.readTree(providerSettingsIn);
        JsonNode rootOut = om.readTree(providerSettingsOut);
        assertTrue(rootIn.equals(rootOut));
    }

}
