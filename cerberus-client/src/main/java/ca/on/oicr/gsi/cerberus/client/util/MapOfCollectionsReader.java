/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.client.util;

import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 *
 * @author ibancarz
 */
public class MapOfCollectionsReader {

    private final JsonParser parser;

    public MapOfCollectionsReader(InputStream input) throws IOException {
        parser = new MappingJsonFactory().createParser(input);
    }

    public Map<String, Collection<AnalysisProvenance>> readAnalysis() throws IOException   {       
        Map<String, Collection<AnalysisProvenance>> map = new HashMap<>();
        JsonToken firstToken = parser.nextToken();
        if (firstToken != JsonToken.START_OBJECT) {
            throw new RuntimeException("Expected start of a JSON object, got " + firstToken.name());
        }
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String provider = parser.getCurrentName();
            CollectionReader reader = new CollectionReader(parser);
            Collection<AnalysisProvenance> coll = new HashSet<>();
            AnalysisProvenance ap = reader.nextAnalysisProvenance();
            while (ap != null) {
                coll.add(ap);
                ap = reader.nextAnalysisProvenance();
            }
            map.put(provider, coll);
        }
        return map;
    }
    
    public Map<String, Collection<LaneProvenance>> readLane() throws IOException   {       
        Map<String, Collection<LaneProvenance>> map = new HashMap<>();
        JsonToken firstToken = parser.nextToken();
        if (firstToken != JsonToken.START_OBJECT) {
            throw new RuntimeException("Expected start of a JSON object, got " + firstToken.name());
        }
        JsonToken token = parser.nextToken();
        while (token != JsonToken.END_OBJECT) {
            if (token == null) {
                throw new RuntimeException("Null token; unexpected end of JSON stream");
            }
            String provider = parser.getCurrentName();
            CollectionReader reader = new CollectionReader(parser);
            Collection<LaneProvenance> coll = new HashSet<>();
            LaneProvenance lp = reader.nextLaneProvenance();
            while (lp != null) {
                coll.add(lp);
                lp = reader.nextLaneProvenance();
            }
            map.put(provider, coll);
            token = parser.nextToken();
        }
        return map;
    }
    
    public Map<String, Collection<SampleProvenance>> readSample() throws IOException   {       
        Map<String, Collection<SampleProvenance>> map = new HashMap<>();
        JsonToken firstToken = parser.nextToken();
        if (firstToken != JsonToken.START_OBJECT) {
            throw new RuntimeException("Expected start of a JSON object, got " + firstToken.name());
        }
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String provider = parser.getCurrentName();
            CollectionReader reader = new CollectionReader(parser);
            Collection<SampleProvenance> coll = new HashSet<>();
            SampleProvenance sp = reader.nextSampleProvenance();
            while (sp != null) {
                coll.add(sp);
                sp = reader.nextSampleProvenance();
            }
            map.put(provider, coll);
        }
        return map;
    }
    
    

}
