/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.model;

import ca.on.oicr.gsi.provenance.model.SampleProvenance;
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
public class SampleProvenanceFromJSON extends ProvenanceFromJSON implements SampleProvenance {

    private final Logger log = LogManager.getLogger(FileProvenanceFromJSON.class);

    private final ZonedDateTime createdDate;
    private final String iusTag;
    private final SortedMap<String, SortedSet<String>> laneAttributes;
    private final String laneNumber;
    private final ZonedDateTime lastModified;
    private final String parentSampleName;
    private final String provenanceId;
    private final String rootSampleName;
    private final SortedMap<String, SortedSet<String>> sampleAttributes;
    private final String sampleName;
    private final String sampleProvenanceId;
    private final SortedMap<String, SortedSet<String>> sequencerRunAttributes;
    private final String sequencerRunName;
    private final String sequencerRunPlatformModel;
    private final Boolean skip;
    private final SortedMap<String, SortedSet<String>> studyAttributes;
    private final String studyTitle;
    private final String version;

    private final String CREATEDDATE_KEY = "createdDate";
    private final String IUSTAG_KEY = "iusTag";
    private final String LANEATTRIBUTES_KEY = "laneAttributes";
    private final String LANENUMBER_KEY = "laneNumber";
    private final String LASTMODIFIED_KEY = "lastModified";
    private final String PARENTSAMPLENAME_KEY = "parentSampleName";
    private final String PROVENANCEID_KEY = "provenanceId";
    private final String ROOTSAMPLENAME_KEY = "rootSampleName";
    private final String SAMPLEATTRIBUTES_KEY = "sampleAttributes";
    private final String SAMPLENAME_KEY = "sampleName";
    private final String SAMPLEPROVENANCEID_KEY = "sampleProvenanceId";
    private final String SEQUENCERRUNATTRIBUTES_KEY = "sequencerRunAttributes";
    private final String SEQUENCERRUNNAME_KEY = "sequencerRunName";
    private final String SEQUENCERRUNPLATFORMMODEL_KEY = "sequencerRunPlatformModel";
    private final String SKIP_KEY = "skip";
    private final String STUDYATTRIBUTES_KEY = "studyAttributes";
    private final String STUDYTITLE_KEY = "studyTitle";
    private final String VERSION_KEY = "version";

    private final List<String> REQUIRED_KEYS = Arrays.asList(
            CREATEDDATE_KEY,
            IUSTAG_KEY,
            LANEATTRIBUTES_KEY,
            LANENUMBER_KEY,
            LASTMODIFIED_KEY,
            PARENTSAMPLENAME_KEY,
            PROVENANCEID_KEY,
            ROOTSAMPLENAME_KEY,
            SAMPLEATTRIBUTES_KEY,
            SAMPLENAME_KEY,
            SAMPLEPROVENANCEID_KEY,
            SEQUENCERRUNATTRIBUTES_KEY,
            SEQUENCERRUNNAME_KEY,
            SEQUENCERRUNPLATFORMMODEL_KEY,
            SKIP_KEY,
            STUDYATTRIBUTES_KEY,
            STUDYTITLE_KEY,
            VERSION_KEY
    );

    public SampleProvenanceFromJSON(String jsonString) throws IOException {
        this(new ObjectMapper().readTree(jsonString));
    }

    public SampleProvenanceFromJSON(JsonNode root) throws IOException {

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
        iusTag = root.get(IUSTAG_KEY).textValue();
        laneNumber = root.get(LANENUMBER_KEY).textValue();
        parentSampleName = root.get(PARENTSAMPLENAME_KEY).textValue();
        provenanceId = root.get(PROVENANCEID_KEY).textValue();
        rootSampleName = root.get(ROOTSAMPLENAME_KEY).textValue();
        sampleName = root.get(SAMPLENAME_KEY).textValue();
        sampleProvenanceId = root.get(SAMPLEPROVENANCEID_KEY).textValue();
        sequencerRunName = root.get(SEQUENCERRUNNAME_KEY).textValue();
        sequencerRunPlatformModel = root.get(SEQUENCERRUNPLATFORMMODEL_KEY).textValue();
        skip = root.get(SKIP_KEY).asBoolean();
        studyTitle = root.get(STUDYTITLE_KEY).textValue();
        version = root.get(VERSION_KEY).textValue();

        // SortedMap<String, SortedSet<String>> attributes
        laneAttributes = nodeToSortedMap(root.get(LANEATTRIBUTES_KEY));
        sampleAttributes = nodeToSortedMap(root.get(SAMPLEATTRIBUTES_KEY));
        sequencerRunAttributes = nodeToSortedMap(root.get(SEQUENCERRUNATTRIBUTES_KEY));
        studyAttributes = nodeToSortedMap(root.get(STUDYATTRIBUTES_KEY));

        // misc attributes
        createdDate = nodeToZonedDateTime(root.get(CREATEDDATE_KEY));
        lastModified = nodeToZonedDateTime(root.get(LASTMODIFIED_KEY));
    }

    @Override
    public String getStudyTitle() {
        return studyTitle;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getStudyAttributes() {
        return studyAttributes;
    }

    @Override
    public String getRootSampleName() {
        return rootSampleName;
    }

    @Override
    public String getParentSampleName() {
        return parentSampleName;
    }

    @Override
    public String getSampleName() {
        return sampleName;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getSampleAttributes() {
        return sampleAttributes;
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
    public String getIusTag() {
        return iusTag;
    }

    @Override
    public Boolean getSkip() {
        return skip;
    }

    @Override
    public String getSampleProvenanceId() {
        return sampleProvenanceId;
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
