/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.client.util;

import ca.on.oicr.gsi.cerberus.model.LaneProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.SampleProvenanceFromJSON;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author ibancarz
 */
public class MapOfMapsReader {

    private final JsonParser parser;

    public MapOfMapsReader(InputStream input) throws IOException {
        parser = new MappingJsonFactory().createParser(input);
    }

    public Map<String, Map<String, LaneProvenance>> readLane() throws IOException {
        Map<String, Map<String, LaneProvenance>> map = new HashMap<>();
        Map<String, LaneProvenance> subMap = new HashMap<>();
        int depth = 0;
        String provider = null;
        String id = null;
        do {
            JsonToken token = parser.nextToken();
            if (token == null) {
                throw new RuntimeException("Null token; unexpected end of JSON stream");
            }
            switch (token) {
                case START_OBJECT:
                    if (depth == 2) { // start of Provenance object
                        JsonNode node = parser.readValueAsTree();
                        subMap.put(id, new LaneProvenanceFromJSON(node));
                    } else {
                        depth++;
                    }
                    break;
                case FIELD_NAME:
                    if (depth == 1) {
                        provider = parser.getCurrentName();
                    } else if (depth == 2) {
                        id = parser.getCurrentName();
                    }
                    break;
                case END_OBJECT:
                    if (depth == 2) {
                        map.put(provider, subMap);
                        subMap = new HashMap<>();
                    }
                    depth--;
                    break;
                default:
                    throw new RuntimeException("Unexpected JSON token: "+token.name());
            }
        } while (depth != 0);
        return map;
    }

    public Map<String, Map<String, SampleProvenance>> readSample() throws IOException {
        Map<String, Map<String, SampleProvenance>> map = new HashMap<>();
        Map<String, SampleProvenance> subMap = new HashMap<>();
        int depth = 0;
        String provider = null;
        String id = null;
        do {
            JsonToken token = parser.nextToken();
            if (token == null) {
                throw new RuntimeException("Null token; unexpected end of JSON stream");
            }
            switch (token) {
                case START_OBJECT:
                    if (depth == 2) { // start of Provenance object
                        JsonNode node = parser.readValueAsTree();
                        subMap.put(id, new SampleProvenanceFromJSON(node));
                    } else {
                        depth++;
                    }
                    break;
                case FIELD_NAME:
                    if (depth == 1) {
                        provider = parser.getCurrentName();
                    } else if (depth == 2) {
                        id = parser.getCurrentName();
                    }
                    break;
                case END_OBJECT:
                    if (depth == 2) {
                        map.put(provider, subMap);
                        subMap = new HashMap<>();
                    }
                    depth--;
                    break;
                default:
                    throw new RuntimeException("Unexpected JSON token: "+token.name());
            }
        } while (depth != 0);
        return map;
    }

}
