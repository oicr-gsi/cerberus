/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.client.util;

import ca.on.oicr.gsi.cerberus.model.AnalysisProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.FileProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.LaneProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.SampleProvenanceFromJSON;
import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import java.io.IOException;
import java.io.InputStream;

/**
 * <ul>
 * <li>Class to parse Provenance objects from an InputStream containing JSON
 * <li>Avoid reading the entire InputStream into a single JsonNode, to avoid
 * memory issues
 * <li>Instead, use the low-level JsonParser to create Provenance objects from
 * strings
 * </ul>
 *
 *
 * @author ibancarz
 */
public class CollectionReader {

    private final JsonParser parser;

    public CollectionReader(InputStream input) throws IOException {
        this(new MappingJsonFactory().createParser(input));
    }

    public CollectionReader(JsonParser parser) throws IOException {
        this.parser = parser;
        JsonToken firstToken = this.parser.nextToken();
        if (firstToken != JsonToken.START_ARRAY) {
            throw new RuntimeException("Expected start of a JSON array, got " + firstToken.name());
        }
    }

    public AnalysisProvenance nextAnalysisProvenance() throws IOException {
        JsonNode node = nextProvenanceNode();
        if (node == null) {
            return null;
        } else {
            return new AnalysisProvenanceFromJSON(node);
        }
    }

    public FileProvenance nextFileProvenance() throws IOException {
        JsonNode node = nextProvenanceNode();
        if (node == null) {
            return null;
        } else {
            return new FileProvenanceFromJSON(node);
        }
    }

    public LaneProvenance nextLaneProvenance() throws IOException {
        JsonNode node = nextProvenanceNode();
        if (node == null) {
            return null;
        } else {
            return new LaneProvenanceFromJSON(node);
        }
    }

    public SampleProvenance nextSampleProvenance() throws IOException {
        JsonNode node = nextProvenanceNode();
        if (node == null) {
            return null;
        } else {
            return new SampleProvenanceFromJSON(node);
        }
    }

    private JsonNode nextProvenanceNode() throws IOException {
        JsonToken token = parser.nextToken();
        if (token == null || token == JsonToken.END_ARRAY) {
            return null;
        } else if (token != JsonToken.START_OBJECT) {
            throw new RuntimeException("Expected start of a JSON object, got " + token.name());
        } else {
            JsonNode provenanceNode = parser.readValueAsTree();
            return provenanceNode;
        }
    }
}
