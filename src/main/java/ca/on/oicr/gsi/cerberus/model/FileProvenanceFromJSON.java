/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.model;

import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.IusLimsKey;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 *
 * @author ibancarz
 */
public class FileProvenanceFromJSON extends ProvenanceFromJSON implements FileProvenance {

    private final Logger log = LogManager.getLogger(FileProvenanceFromJSON.class);

    private final SortedMap<String, SortedSet<String>> fileAttributes;
    private final String fileDescription;
    private final String fileMd5sum;
    private final String fileMetaType;
    private final String filePath;
    private final Integer fileSWID;
    private final String fileSize;
    private final SortedMap<String, SortedSet<String>> iusAttributes;
    private final Collection<IusLimsKey> iusLimsKeys;
    private final Collection<String> iusSWIDs;
    private final Collection<String> iusTags;
    private final SortedMap<String, SortedSet<String>> laneAttributes;
    private final Collection<String> laneNames;
    private final Collection<String> laneNumbers;
    private final ZonedDateTime lastModified;
    private final SortedMap<String, SortedSet<String>> parentSampleAttributes;
    private final Collection<String> parentSampleNames;
    private final Collection<String> parentSampleOrganismIDs;
    private final String processingAlgorithm;
    private final SortedMap<String, SortedSet<String>> processingAttributes;
    private final Integer processingSWID;
    private final String processingStatus;
    private final Collection<IusLimsKey> relatedIusLimsKeys; // optional; not specified by FileProvenance interface
    private final Collection<String> releatedIusSWIDs; // optional; not specified by FileProvenance interface. Replicates a typo, to be fixed in future Provenance release
    private final Collection<String> rootSampleNames;
    private final SortedMap<String, SortedSet<String>> sampleAttributes;
    private final Collection<String> sampleNames;
    private final Collection<String> sampleOrganismCodes;
    private final Collection<String> sampleOrganismIDs;
    private final SortedMap<String, SortedSet<String>> sequencerRunAttributes;
    private final Collection<String> sequencerRunNames;
    private final Collection<String> sequencerRunPlatformIDs;
    private final Collection<String> sequencerRunPlatformNames;
    private final String skip;
    private final Status status;
    private final String statusReason;
    private final SortedMap<String, SortedSet<String>> studyAttributes;
    private final Collection<String> studyTitles;
    private final SortedMap<String, SortedSet<String>> workflowAttributes;
    private final String workflowName;
    private final SortedMap<String, SortedSet<String>> workflowRunAttributes;
    private final SortedSet<Integer> workflowRunInputFileSWIDs;
    private final String workflowRunName;
    private final Integer workflowRunSWID;
    private final String workflowRunStatus;
    private final Integer workflowSWID;
    private final String workflowVersion;

    private final String FILEATTRIBUTES_KEY = "fileAttributes";
    private final String FILEDESCRIPTION_KEY = "fileDescription";
    private final String FILEMD5SUM_KEY = "fileMd5sum";
    private final String FILEMETATYPE_KEY = "fileMetaType";
    private final String FILEPATH_KEY = "filePath";
    private final String FILESWID_KEY = "fileSWID";
    private final String FILESIZE_KEY = "fileSize";
    private final String IUSATTRIBUTES_KEY = "iusAttributes";
    private final String IUSLIMSKEYS_KEY = "iusLimsKeys";
    private final String IUSSWIDS_KEY = "iusSWIDs";
    private final String IUSTAGS_KEY = "iusTags";
    private final String LANEATTRIBUTES_KEY = "laneAttributes";
    private final String LANENAMES_KEY = "laneNames";
    private final String LANENUMBERS_KEY = "laneNumbers";
    private final String LASTMODIFIED_KEY = "lastModified";
    private final String PARENTSAMPLEATTRIBUTES_KEY = "parentSampleAttributes";
    private final String PARENTSAMPLENAMES_KEY = "parentSampleNames";
    private final String PARENTSAMPLEORGANISMIDS_KEY = "parentSampleOrganismIDs";
    private final String PROCESSINGALGORITHM_KEY = "processingAlgorithm";
    private final String PROCESSINGATTRIBUTES_KEY = "processingAttributes";
    private final String PROCESSINGSWID_KEY = "processingSWID";
    private final String PROCESSINGSTATUS_KEY = "processingStatus";
    private final String RELATEDIUSLIMSKEYS_KEY = "relatedIusLimsKeys"; // optional; not specified by FileProvenance interface
    private final String RELATEDIUSSWIDS_KEY = "releatedIusSWIDs"; // replicates a typo, to be fixed in future Provenance release
    private final String ROOTSAMPLENAMES_KEY = "rootSampleNames";
    private final String SAMPLEATTRIBUTES_KEY = "sampleAttributes";
    private final String SAMPLENAMES_KEY = "sampleNames";
    private final String SAMPLEORGANISMCODES_KEY = "sampleOrganismCodes";
    private final String SAMPLEORGANISMIDS_KEY = "sampleOrganismIDs";
    private final String SEQUENCERRUNATTRIBUTES_KEY = "sequencerRunAttributes";
    private final String SEQUENCERRUNNAMES_KEY = "sequencerRunNames";
    private final String SEQUENCERRUNPLATFORMIDS_KEY = "sequencerRunPlatformIDs";
    private final String SEQUENCERRUNPLATFORMNAMES_KEY = "sequencerRunPlatformNames";
    private final String SKIP_KEY = "skip";
    private final String STATUS_KEY = "status";
    private final String STATUSREASON_KEY = "statusReason";
    private final String STUDYATTRIBUTES_KEY = "studyAttributes";
    private final String STUDYTITLES_KEY = "studyTitles";
    private final String WORKFLOWATTRIBUTES_KEY = "workflowAttributes";
    private final String WORKFLOWNAME_KEY = "workflowName";
    private final String WORKFLOWRUNATTRIBUTES_KEY = "workflowRunAttributes";
    private final String WORKFLOWRUNINPUTFILESWIDS_KEY = "workflowRunInputFileSWIDs";
    private final String WORKFLOWRUNNAME_KEY = "workflowRunName";
    private final String WORKFLOWRUNSWID_KEY = "workflowRunSWID";
    private final String WORKFLOWRUNSTATUS_KEY = "workflowRunStatus";
    private final String WORKFLOWSWID_KEY = "workflowSWID";
    private final String WORKFLOWVERSION_KEY = "workflowVersion";

    private final List<String> REQUIRED_KEYS = Arrays.asList(
            FILEATTRIBUTES_KEY,
            FILEDESCRIPTION_KEY,
            FILEMD5SUM_KEY,
            FILEMETATYPE_KEY,
            FILEPATH_KEY,
            FILESWID_KEY,
            FILESIZE_KEY,
            IUSATTRIBUTES_KEY,
            IUSLIMSKEYS_KEY,
            IUSSWIDS_KEY,
            IUSTAGS_KEY,
            LANEATTRIBUTES_KEY,
            LANENAMES_KEY,
            LANENUMBERS_KEY,
            LASTMODIFIED_KEY,
            PARENTSAMPLEATTRIBUTES_KEY,
            PARENTSAMPLENAMES_KEY,
            PARENTSAMPLEORGANISMIDS_KEY,
            PROCESSINGALGORITHM_KEY,
            PROCESSINGATTRIBUTES_KEY,
            PROCESSINGSWID_KEY,
            PROCESSINGSTATUS_KEY,
            ROOTSAMPLENAMES_KEY,
            SAMPLEATTRIBUTES_KEY,
            SAMPLENAMES_KEY,
            SAMPLEORGANISMCODES_KEY,
            SAMPLEORGANISMIDS_KEY,
            SEQUENCERRUNATTRIBUTES_KEY,
            SEQUENCERRUNNAMES_KEY,
            SEQUENCERRUNPLATFORMIDS_KEY,
            SEQUENCERRUNPLATFORMNAMES_KEY,
            SKIP_KEY,
            STATUS_KEY,
            STATUSREASON_KEY,
            STUDYATTRIBUTES_KEY,
            STUDYTITLES_KEY,
            WORKFLOWATTRIBUTES_KEY,
            WORKFLOWNAME_KEY,
            WORKFLOWRUNATTRIBUTES_KEY,
            WORKFLOWRUNINPUTFILESWIDS_KEY,
            WORKFLOWRUNNAME_KEY,
            WORKFLOWRUNSWID_KEY,
            WORKFLOWRUNSTATUS_KEY,
            WORKFLOWSWID_KEY,
            WORKFLOWVERSION_KEY
    );

    // keys not specified by FileProvanance interface, but by implementations
    private final List<String> SUPPLEMENTARY_KEYS = Arrays.asList(
            RELATEDIUSLIMSKEYS_KEY,
            RELATEDIUSSWIDS_KEY
    );

    public FileProvenanceFromJSON(String jsonString) throws IOException {
        this(new ObjectMapper().readTree(jsonString));
    }

    public FileProvenanceFromJSON(JsonNode root) throws IOException {

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
                if (SUPPLEMENTARY_KEYS.contains(name)) {
                    log.debug("Found supplementary field name in Provenance JSON input: " + name);
                } else {
                    log.warn("Found unknown field name in Provenance JSON input, skipping: " + name);
                }
            }
        }

        // string/integer attributes
        fileDescription = root.get(FILEDESCRIPTION_KEY).textValue();
        fileMd5sum = root.get(FILEMD5SUM_KEY).textValue();
        fileMetaType = root.get(FILEMETATYPE_KEY).textValue();
        filePath = root.get(FILEPATH_KEY).textValue();
        fileSWID = root.get(FILESWID_KEY).asInt();
        fileSize = root.get(FILESIZE_KEY).textValue();
        processingAlgorithm = root.get(PROCESSINGALGORITHM_KEY).textValue();
        processingSWID = root.get(PROCESSINGSWID_KEY).asInt();
        processingStatus = root.get(PROCESSINGSTATUS_KEY).textValue();
        skip = root.get(SKIP_KEY).textValue();
        statusReason = root.get(STATUSREASON_KEY).textValue();
        workflowName = root.get(WORKFLOWNAME_KEY).textValue();
        workflowRunName = root.get(WORKFLOWRUNNAME_KEY).textValue();
        workflowRunSWID = root.get(WORKFLOWRUNSWID_KEY).asInt();
        workflowRunStatus = root.get(WORKFLOWRUNSTATUS_KEY).textValue();
        workflowSWID = root.get(WORKFLOWSWID_KEY).asInt();
        workflowVersion = root.get(WORKFLOWVERSION_KEY).textValue();

        // Collection<String> attributes
        iusSWIDs = nodeToStringCollection(root.get(IUSSWIDS_KEY));
        iusTags = nodeToStringCollection(root.get(IUSTAGS_KEY));
        laneNames = nodeToStringCollection(root.get(LANENAMES_KEY));
        laneNumbers = nodeToStringCollection(root.get(LANENUMBERS_KEY));
        parentSampleNames = nodeToStringCollection(root.get(PARENTSAMPLENAMES_KEY));
        releatedIusSWIDs = nodeToStringCollection(root.get(RELATEDIUSSWIDS_KEY)); // reproduces a typo
        parentSampleOrganismIDs = nodeToStringCollection(root.get(PARENTSAMPLEORGANISMIDS_KEY));
        rootSampleNames = nodeToStringCollection(root.get(ROOTSAMPLENAMES_KEY));
        sampleNames = nodeToStringCollection(root.get(SAMPLENAMES_KEY));
        sampleOrganismCodes = nodeToStringCollection(root.get(SAMPLEORGANISMCODES_KEY));
        sampleOrganismIDs = nodeToStringCollection(root.get(SAMPLEORGANISMIDS_KEY));
        sequencerRunNames = nodeToStringCollection(root.get(SEQUENCERRUNNAMES_KEY));
        sequencerRunPlatformIDs = nodeToStringCollection(root.get(SEQUENCERRUNPLATFORMIDS_KEY));
        sequencerRunPlatformNames = nodeToStringCollection(root.get(SEQUENCERRUNPLATFORMNAMES_KEY));
        studyTitles = nodeToStringCollection(root.get(STUDYTITLES_KEY));

        // SortedMap<String, SortedSet<String>> attributes
        fileAttributes = nodeToSortedMap(root.get(FILEATTRIBUTES_KEY));
        iusAttributes = nodeToSortedMap(root.get(IUSATTRIBUTES_KEY));
        laneAttributes = nodeToSortedMap(root.get(LANEATTRIBUTES_KEY));
        parentSampleAttributes = nodeToSortedMap(root.get(PARENTSAMPLEATTRIBUTES_KEY));
        processingAttributes = nodeToSortedMap(root.get(PROCESSINGATTRIBUTES_KEY));
        sampleAttributes = nodeToSortedMap(root.get(SAMPLEATTRIBUTES_KEY));
        sequencerRunAttributes = nodeToSortedMap(root.get(SEQUENCERRUNATTRIBUTES_KEY));
        studyAttributes = nodeToSortedMap(root.get(STUDYATTRIBUTES_KEY));
        workflowAttributes = nodeToSortedMap(root.get(WORKFLOWATTRIBUTES_KEY));
        workflowRunAttributes = nodeToSortedMap(root.get(WORKFLOWRUNATTRIBUTES_KEY));

        // misc attributes
        iusLimsKeys = nodeToIusLimsKeys(root.get(IUSLIMSKEYS_KEY));
        relatedIusLimsKeys = nodeToIusLimsKeys(root.get(RELATEDIUSLIMSKEYS_KEY));
        lastModified = nodeToZonedDateTime(root.get(LASTMODIFIED_KEY));
        status = Status.valueOf(root.get(STATUS_KEY).textValue());
        workflowRunInputFileSWIDs = nodeToSortedSetOfIntegers(root.get(WORKFLOWRUNINPUTFILESWIDS_KEY));
    }

    @Override
    public Collection<String> getStudyTitles() {
        return studyTitles;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getStudyAttributes() {
        return studyAttributes;
    }

    @Override
    public Collection<String> getRootSampleNames() {
        return rootSampleNames;
    }

    @Override
    public Collection<String> getParentSampleNames() {
        return parentSampleNames;
    }

    @Override
    public Collection<String> getParentSampleOrganismIDs() {
        return parentSampleOrganismIDs;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getParentSampleAttributes() {
        return parentSampleAttributes;
    }

    @Override
    public Collection<String> getSampleNames() {
        return sampleNames;
    }

    @Override
    public Collection<String> getSampleOrganismIDs() {
        return sampleOrganismIDs;
    }

    @Override
    public Collection<String> getSampleOrganismCodes() {
        return sampleOrganismCodes;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getSampleAttributes() {
        return sampleAttributes;
    }

    @Override
    public Collection<String> getSequencerRunNames() {
        return sequencerRunNames;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getSequencerRunAttributes() {
        return sequencerRunAttributes;
    }

    @Override
    public Collection<String> getSequencerRunPlatformIDs() {
        return sequencerRunPlatformIDs;
    }

    @Override
    public Collection<String> getSequencerRunPlatformNames() {
        return sequencerRunPlatformNames;
    }

    @Override
    public Collection<String> getLaneNames() {
        return laneNames;
    }

    @Override
    public Collection<String> getLaneNumbers() {
        return laneNumbers;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getLaneAttributes() {
        return laneAttributes;
    }

    @Override
    public String getWorkflowName() {
        return workflowName;
    }

    @Override
    public String getWorkflowVersion() {
        return workflowVersion;
    }

    @Override
    public Integer getWorkflowSWID() {
        return workflowSWID;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getWorkflowAttributes() {
        return workflowAttributes;
    }

    @Override
    public String getWorkflowRunName() {
        return workflowRunName;
    }

    @Override
    public String getWorkflowRunStatus() {
        return workflowRunStatus;
    }

    @Override
    public Integer getWorkflowRunSWID() {
        return workflowRunSWID;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getWorkflowRunAttributes() {
        return workflowRunAttributes;
    }

    @Override
    public SortedSet<Integer> getWorkflowRunInputFileSWIDs() {
        return workflowRunInputFileSWIDs;
    }

    @Override
    public String getProcessingAlgorithm() {
        return processingAlgorithm;
    }

    @Override
    public Integer getProcessingSWID() {
        return processingSWID;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getProcessingAttributes() {
        return processingAttributes;
    }

    @Override
    public String getProcessingStatus() {
        return processingStatus;
    }

    @Override
    public String getFileMetaType() {
        return fileMetaType;
    }

    @Override
    public Integer getFileSWID() {
        return fileSWID;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getFileAttributes() {
        return fileAttributes;
    }

    @Override
    public String getFilePath() {
        return filePath;
    }

    @Override
    public String getFileMd5sum() {
        return fileMd5sum;
    }

    @Override
    public String getFileSize() {
        return fileSize;
    }

    @Override
    public String getFileDescription() {
        return fileDescription;
    }

    @Override
    public String getSkip() {
        return skip;
    }

    @Override
    public ZonedDateTime getLastModified() {
        return lastModified;
    }

    @Override
    public Collection<IusLimsKey> getIusLimsKeys() {
        return iusLimsKeys;
    }

    @Override
    public Collection<String> getIusSWIDs() {
        return iusSWIDs;
    }

    public Collection<String> getReleatedIusSWIDs() { // reproduces typo
        return releatedIusSWIDs;
    }

    @Override
    public Collection<String> getIusTags() {
        return iusTags;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getIusAttributes() {
        return iusAttributes;
    }

    public Collection<IusLimsKey> getRelatedIusLimsKeys() {
        return relatedIusLimsKeys;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public String getStatusReason() {
        return statusReason;
    }

}
