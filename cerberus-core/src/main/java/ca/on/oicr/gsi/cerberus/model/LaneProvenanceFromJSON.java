/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.model;

import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author ibancarz
 */
public class LaneProvenanceFromJSON extends ProvenanceFromJSON implements LaneProvenance {

    private final Logger log = LogManager.getLogger(FileProvenanceFromJSON.class);

    public ZonedDateTime createdDate;
    private final SortedMap<String, SortedSet<String>> laneAttributes;
    private final String laneNumber;
    private final String laneProvenanceId;
    private final ZonedDateTime lastModified;
    private final String provenanceId;
    private final SortedMap<String, SortedSet<String>> sequencerRunAttributes;
    private final String sequencerRunName;
    private final String sequencerRunPlatformModel;
    private final Boolean skip;
    private final String version;

    private final String CREATEDDATE_KEY = "createdDate";
    private final String LANEATTRIBUTES_KEY = "laneAttributes";
    private final String LANENUMBER_KEY = "laneNumber";
    private final String LANEPROVENANCEID_KEY = "laneProvenanceId";
    private final String LASTMODIFIED_KEY = "lastModified";
    private final String PROVENANCEID_KEY = "provenanceId";
    private final String SEQUENCERRUNATTRIBUTES_KEY = "sequencerRunAttributes";
    private final String SEQUENCERRUNNAME_KEY = "sequencerRunName";
    private final String SEQUENCERRUNPLATFORMMODEL_KEY = "sequencerRunPlatformModel";
    private final String SKIP_KEY = "skip";
    private final String VERSION_KEY = "version";

    private final List<String> REQUIRED_KEYS = Arrays.asList(
            CREATEDDATE_KEY,
            LANEATTRIBUTES_KEY,
            LANENUMBER_KEY,
            LANEPROVENANCEID_KEY,
            LASTMODIFIED_KEY,
            PROVENANCEID_KEY,
            SEQUENCERRUNATTRIBUTES_KEY,
            SEQUENCERRUNNAME_KEY,
            SEQUENCERRUNPLATFORMMODEL_KEY,
            SKIP_KEY,
            VERSION_KEY
    );

    
    public LaneProvenanceFromJSON(String jsonString) throws IOException {
        this(new ObjectMapper().readTree(jsonString));
    }
    
    public LaneProvenanceFromJSON(JsonNode root) throws IOException  {

         ArrayList<String> missingList = new ArrayList<>();
        for (String key : REQUIRED_KEYS) {
            if (!root.has(key)) {
                missingList.add(key);
            }
        }
        if (!missingList.isEmpty()) {
            String missing = String.join(", ", missingList);
            String msg = "Missing required fields in Provenance JSON input: " + missing;
            log.fatal(msg);
            throw new IllegalArgumentException(msg);
        }
        for (Iterator<String> i = root.fieldNames(); i.hasNext();) {
            String name = i.next();
            if (!REQUIRED_KEYS.contains(name)) {
                log.warn("Found unknown field name in Provenance JSON input, skipping: " + name);

            }
        }

        // string/integer/boolean attributes
        laneNumber = root.get(LANENUMBER_KEY).textValue();
        laneProvenanceId = root.get(LANEPROVENANCEID_KEY).textValue();
        provenanceId = root.get(PROVENANCEID_KEY).textValue();
        sequencerRunName = root.get(SEQUENCERRUNNAME_KEY).textValue();
        sequencerRunPlatformModel = root.get(SEQUENCERRUNPLATFORMMODEL_KEY).textValue();
        skip = root.get(SKIP_KEY).asBoolean();
        version = root.get(VERSION_KEY).textValue();

        // SortedMap<String, SortedSet<String>> attributes
        laneAttributes = nodeToSortedMap(root.get(LANEATTRIBUTES_KEY));
        sequencerRunAttributes = nodeToSortedMap(root.get(SEQUENCERRUNATTRIBUTES_KEY));

        // misc attributes
        createdDate = nodeToZonedDateTime(root.get(CREATEDDATE_KEY));
        lastModified = nodeToZonedDateTime(root.get(LASTMODIFIED_KEY));
        
        
    }

    @Override
    public String getSequencerRunName() {
        return sequencerRunName;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getSequencerRunAttributes() {
       return sequencerRunAttributes;
    }

    @Override
    public String getSequencerRunPlatformModel() {
        return sequencerRunPlatformModel;
    }

    @Override
    public String getLaneNumber() {
        return laneNumber;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getLaneAttributes() {
        return laneAttributes;
    }

    @Override
    public Boolean getSkip() {
        return skip;
    }

    @Override
    public String getLaneProvenanceId() {
        return laneProvenanceId;
    }

    @Override
    public ZonedDateTime getCreatedDate() {
        return createdDate;
    }

    @Override
    public String getProvenanceId() {
        return provenanceId;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public ZonedDateTime getLastModified() {
        return lastModified;
    }

}
