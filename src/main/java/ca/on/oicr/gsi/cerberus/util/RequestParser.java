/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.util;

import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
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

    private final JsonNode actionNode;
    private final JsonNode typeNode;
    private final JsonNode incFilterNode;
    private final JsonNode excFilterNode;

    /**
     * Constructor with JSON input
     *
     * Required input nodes:
     * <ul> Providers: Array. Parameters for ProvenanceProvider objects.
     * <li> Filters: Map. Required, but may be empty.
     * <li> Provenance type: String. Must be one of a list of allowed values.
     * </ul>
     *
     *
     * @param body, String in JSON format with provider and filter settings
     * @throws IOException
     */
    public RequestParser(String body) throws IOException {
        om = new ObjectMapper();
        JsonNode root = om.readTree(body);
        ArrayList<String> missingKeys = new ArrayList<>();
        for (PostField key : PostField.values()) {
            if (!root.has(key.name())) {
                missingKeys.add(key.name());
            }
        }
        if (!missingKeys.isEmpty()) {
            String msg = "Invalid JSON input; missing required keys: " + String.join(", ", missingKeys);
            log.fatal(msg);
            throw new IllegalArgumentException(msg);
        }
        incFilterNode = root.get(PostField.INC_FILTER.name());
        excFilterNode = root.get(PostField.EXC_FILTER.name());
        typeNode = root.get(PostField.TYPE.name());
        actionNode = root.get(PostField.ACTION.name());
    }

    /**
     * Get file provenance filters for inclusion
     *
     * A filter consists of:
     * <ul>
     * <li>Filter function: FileProvenanceFilter
     * <li>Parameters: Set of Strings
     * </ul>
     *
     * @return Map of filter settings
     */
    public Map<FileProvenanceFilter, Set<String>> getIncFilters() {
        return parseFilters(incFilterNode);
    }
    
    
    /**
     * Get file provenance filters for exclusion
     * 
     * @return 
     */
    
    public Map<FileProvenanceFilter, Set<String>> getExcFilters() {
        return parseFilters(excFilterNode);
    }

    
    /**
     * Get the provenance type: ANALYSIS, FILE, LANE, SAMPLE Check input against
     * enumeration of allowable types
     *
     * @return String, representation of the type
     * @throws JsonProcessingException
     */
    public String getProvenanceAction() throws JsonProcessingException {

        String actionInput = om.writeValueAsString(actionNode);
        actionInput = actionInput.replace("\"", ""); // remove quotes from deserialized JSON
        ProvenanceAction action;
        try {
            action = ProvenanceAction.valueOf(actionInput);
        } catch (IllegalArgumentException e) {
            log.fatal("Invalid provenance action '" + actionInput + "': " + e.getMessage());
            throw e;
        }
        return action.name();
    }

        /**
     * Get the provenance type: ANALYSIS, FILE, LANE, SAMPLE Check input against
     * enumeration of allowable types
     *
     * @return String, representation of the type
     * @throws JsonProcessingException
     */
    public String getProvenanceType() throws JsonProcessingException {

        String typeInput = om.writeValueAsString(typeNode);
        typeInput = typeInput.replace("\"", ""); // remove quotes from deserialized JSON
        ProvenanceType type;
        try {
            type = ProvenanceType.valueOf(typeInput);
        } catch (IllegalArgumentException e) {
            log.fatal("Invalid provenance type '" + typeInput + "': " + e.getMessage());
            throw e;
        }
        return type.name();
    }
    
    
      private Map<FileProvenanceFilter, Set<String>> parseFilters(JsonNode node) {

        HashMap<FileProvenanceFilter, Set<String>> filters = new HashMap<>();

        for (Iterator<String> i = node.fieldNames(); i.hasNext();) {
            String filterName = i.next();
            JsonNode paramsNode = node.get(filterName);

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
  
    
}
