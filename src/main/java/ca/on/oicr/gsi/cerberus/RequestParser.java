/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Class to parse Provenance Provider/Filter settings
 *
 * Usage: Call from servlet to parse the input JSON. Processing of Provenance
 * objects is done by ProvenanceHandler.
 *
 * Input: JSON string from the body of HTTP request
 *
 * @author ibancarz
 */
public class RequestParser {

    private final Logger log = LogManager.getLogger(RequestParser.class);

    private final ObjectMapper om;

    // TODO have a separate package for shared constants
    private static final String PROVIDER_KEY = "provider_settings";
    private static final String FILTER_KEY = "filter_settings";
    private final JsonNode providerNode;
    private final JsonNode filterNode;

    /**
     * Constructor with JSON input
     * 
     * @param body, String in JSON format with provider and filter settings
     * @throws IOException 
     */
    
    public RequestParser(String body) throws IOException {
        om = new ObjectMapper();
        JsonNode root = om.readTree(body);
        providerNode = root.get(PROVIDER_KEY);
        filterNode = root.get(FILTER_KEY);
    }

    /**
     * Get file provenance filters
     * 
     * A filter consists of:
     * <ul>
     * <li>Filter function: FileProvenanceFilter
     * <li>Parameters: Set of Strings
     * </ul>
     *
     * @return Map of filter settings
     */
    
    public Map<FileProvenanceFilter, Set<String>> getFilters() {

        HashMap<FileProvenanceFilter, Set<String>> filters = new HashMap<>();

        for (Iterator<String> i = filterNode.fieldNames(); i.hasNext();) {
            String filterName = i.next();
            JsonNode paramsNode = filterNode.get(filterName);

            HashSet<String> params = new HashSet<>();
            for (Iterator<JsonNode> j = paramsNode.elements(); j.hasNext();) {
                params.add(j.next().textValue());
            }

            try {
                FileProvenanceFilter fpf = FileProvenanceFilter.valueOf(filterName);
                filters.put(fpf, params);
            } catch (IllegalArgumentException e) {
                log.fatal("Invalid file provenance filter name '" + filterName + "': " + e.getMessage());
                throw e;
            }
        }

        return filters;
    }

    /**
     *
     * Get input parameters for a ProviderLoader See
     * pipedev/pipedev-provenance-impl
     *
     * ProviderLoader expects a JSON string with appropriate data structure
     * Extract the string from relevant JSON node and return
     *
     * @throws JsonProcessingException
     * @return String provider text
     */
    public String getProviderSettings() throws JsonProcessingException {

        return om.writeValueAsString(providerNode);
    }
}
