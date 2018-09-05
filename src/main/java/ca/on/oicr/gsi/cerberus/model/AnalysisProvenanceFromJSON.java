/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.model;

import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.IusLimsKey;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class to deserialize JSON into an AnalysisProvenance object
 * 
 * <ul>
 * <li>Some overlap in attributes and methods with FileProvenanceFromJSON; could consolidate into the ProvenanceFromJSON class
 <li>Some differences in data type, eg. skip attribute is Boolean instead of String
 * <li>Also differences in nomenclature, eg. "Id" in attribute names instead of "SWID"
 * <li>TODO modify the respective Provenance interfaces for consistent data types
 * </ul>
 * 
 * @author ibancarz
 */
public class AnalysisProvenanceFromJSON extends ProvenanceFromJSON implements AnalysisProvenance {

    private final Logger log = LogManager.getLogger(FileProvenanceFromJSON.class);

    // similar to FileProvenance, but 'Id' replaces 'SWID'
    private final SortedMap<String, SortedSet<String>> fileAttributes;
    private final String fileDescription;
    private final String fileMd5sum;
    private final String fileMetaType;
    private final String filePath;
    private final Integer fileId;
    private final String fileSize;
    private final SortedMap<String, SortedSet<String>> iusAttributes;
    private final Set<IusLimsKey> iusLimsKeys; // different data type to FileProvenance
    private final ZonedDateTime lastModified;
    private final String processingAlgorithm;
    private final SortedMap<String, SortedSet<String>> processingAttributes;
    private final Integer processingId;
    private final String processingStatus;
    private final Boolean skip; // different data type to FileProvenance
    private final SortedMap<String, SortedSet<String>> workflowAttributes;
    private final Integer workflowId;
    private final String workflowName;
    private final SortedMap<String, SortedSet<String>> workflowRunAttributes;
    private final Integer workflowRunId;
    private final SortedSet<Integer> workflowRunInputFileIds;
    private final String workflowRunName;
    private final String workflowRunStatus;
    private final String workflowVersion;

    private final String FILEATTRIBUTES_KEY = "fileAttributes";
    private final String FILEDESCRIPTION_KEY = "fileDescription";
    private final String FILEMD5SUM_KEY = "fileMd5sum";
    private final String FILEMETATYPE_KEY = "fileMetaType";
    private final String FILEPATH_KEY = "filePath";
    private final String FILEID_KEY = "fileId";
    private final String FILESIZE_KEY = "fileSize";
    private final String IUSATTRIBUTES_KEY = "iusAttributes";
    private final String IUSLIMSKEYS_KEY = "iusLimsKeys";
    private final String LASTMODIFIED_KEY = "lastModified";
    private final String PROCESSINGALGORITHM_KEY = "processingAlgorithm";
    private final String PROCESSINGATTRIBUTES_KEY = "processingAttributes";
    private final String PROCESSINGID_KEY = "processingId";
    private final String PROCESSINGSTATUS_KEY = "processingStatus";
    private final String SKIP_KEY = "skip";
    private final String WORKFLOWATTRIBUTES_KEY = "workflowAttributes";
    private final String WORKFLOWNAME_KEY = "workflowName";
    private final String WORKFLOWRUNATTRIBUTES_KEY = "workflowRunAttributes";
    private final String WORKFLOWRUNINPUTFILEIDS_KEY = "workflowRunInputFileIds";
    private final String WORKFLOWRUNNAME_KEY = "workflowRunName";
    private final String WORKFLOWRUNID_KEY = "workflowRunId";
    private final String WORKFLOWRUNSTATUS_KEY = "workflowRunStatus";
    private final String WORKFLOWID_KEY = "workflowId";
    private final String WORKFLOWVERSION_KEY = "workflowVersion";

    private final List<String> REQUIRED_KEYS = Arrays.asList(
            FILEATTRIBUTES_KEY,
            FILEDESCRIPTION_KEY,
            FILEMD5SUM_KEY,
            FILEMETATYPE_KEY,
            FILEPATH_KEY,
            FILEID_KEY,
            FILESIZE_KEY,
            IUSATTRIBUTES_KEY,
            IUSLIMSKEYS_KEY,
            LASTMODIFIED_KEY,
            PROCESSINGALGORITHM_KEY,
            PROCESSINGATTRIBUTES_KEY,
            PROCESSINGID_KEY,
            PROCESSINGSTATUS_KEY,
            SKIP_KEY,
            WORKFLOWATTRIBUTES_KEY,
            WORKFLOWNAME_KEY,
            WORKFLOWRUNATTRIBUTES_KEY,
            WORKFLOWRUNINPUTFILEIDS_KEY,
            WORKFLOWRUNNAME_KEY,
            WORKFLOWRUNID_KEY,
            WORKFLOWRUNSTATUS_KEY,
            WORKFLOWID_KEY,
            WORKFLOWVERSION_KEY
    );

    public AnalysisProvenanceFromJSON(String jsonString) throws IOException {
        this(new ObjectMapper().readTree(jsonString));
    }

    public AnalysisProvenanceFromJSON(JsonNode root) throws IOException {

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
        fileDescription = root.get(FILEDESCRIPTION_KEY).textValue();
        fileMd5sum = root.get(FILEMD5SUM_KEY).textValue();
        fileMetaType = root.get(FILEMETATYPE_KEY).textValue();
        filePath = root.get(FILEPATH_KEY).textValue();
        fileId = root.get(FILEID_KEY).asInt();
        fileSize = root.get(FILESIZE_KEY).textValue();
        processingAlgorithm = root.get(PROCESSINGALGORITHM_KEY).textValue();
        processingId = root.get(PROCESSINGID_KEY).asInt();
        processingStatus = root.get(PROCESSINGSTATUS_KEY).textValue();
        skip = root.get(SKIP_KEY).asBoolean();
        workflowName = root.get(WORKFLOWNAME_KEY).textValue();
        workflowRunName = root.get(WORKFLOWRUNNAME_KEY).textValue();
        workflowRunId = root.get(WORKFLOWRUNID_KEY).asInt();
        workflowRunStatus = root.get(WORKFLOWRUNSTATUS_KEY).textValue();
        workflowId = root.get(WORKFLOWID_KEY).asInt();
        workflowVersion = root.get(WORKFLOWVERSION_KEY).textValue();
        
        // SortedMap<String, SortedSet<String>> attributes
        fileAttributes = nodeToSortedMap(root.get(FILEATTRIBUTES_KEY));
        iusAttributes = nodeToSortedMap(root.get(IUSATTRIBUTES_KEY));
        processingAttributes = nodeToSortedMap(root.get(PROCESSINGATTRIBUTES_KEY));
        workflowAttributes = nodeToSortedMap(root.get(WORKFLOWATTRIBUTES_KEY));
        workflowRunAttributes = nodeToSortedMap(root.get(WORKFLOWRUNATTRIBUTES_KEY)); 
        
        
        // misc attributes
        iusLimsKeys = nodeToIusLimsKeySet(root.get(IUSLIMSKEYS_KEY));
        lastModified = nodeToZonedDateTime(root.get(LASTMODIFIED_KEY));
        workflowRunInputFileIds = nodeToSortedSetOfIntegers(root.get(WORKFLOWRUNINPUTFILEIDS_KEY));
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
    public Integer getWorkflowId() {
        return workflowId;
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
    public Integer getWorkflowRunId() {
        return workflowRunId;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getWorkflowRunAttributes() {
        return workflowRunAttributes;
    }

    @Override
    public SortedSet<Integer> getWorkflowRunInputFileIds() {
        return workflowRunInputFileIds;
    }

    @Override
    public String getProcessingAlgorithm() {
        return processingAlgorithm;
    }

    @Override
    public Integer getProcessingId() {
        return processingId;
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
    public Integer getFileId() {
        return fileId;
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
    public SortedMap<String, SortedSet<String>> getFileAttributes() {
        return fileAttributes;
    }

    @Override
    public Boolean getSkip() {
        return skip;
    }

    @Override
    public ZonedDateTime getLastModified() {
        return lastModified;
    }

    @Override
    public Set<IusLimsKey> getIusLimsKeys() {
        return iusLimsKeys;
    }

    @Override
    public SortedMap<String, SortedSet<String>> getIusAttributes() {
        return iusAttributes;
    }

}
