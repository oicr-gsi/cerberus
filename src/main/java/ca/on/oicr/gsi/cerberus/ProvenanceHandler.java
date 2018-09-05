/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.provenance.AnalysisProvenanceProvider;
import ca.on.oicr.gsi.provenance.DefaultProvenanceClient;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.LaneProvenanceProvider;
import ca.on.oicr.gsi.provenance.MultiThreadedDefaultProvenanceClient;
import ca.on.oicr.gsi.provenance.ProviderLoader;
import ca.on.oicr.gsi.provenance.SampleProvenanceProvider;
import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 *
 * Class with methods to handle provenance queries
 *
 * <ul>
 * <li> Set up provenance Providers at object creation
 * <li> Provider types: Analysis, Lane, Sample
 * <li> Return appropriate file provenance JSON for given filters
 * </ul>
 *
 * <p>
 * Has (optional) truncation of result sets above a given size before converting
 * to JSON. TODO use this as the basis for pagination of results.</p>
 *
 * <p>
 * TODO implement cacheing for provenance results</p>
 *
 * @author ibancarz
 */
public class ProvenanceHandler {

    private final DefaultProvenanceClient dpc;
    private final Logger log = LogManager.getLogger(ProvenanceHandler.class);
    private final ObjectMapper om;
    private boolean truncateResults = true;
    private Integer maxResults = 10;

    /**
     * Constructor with given ProviderSettings, DefaultProvenanceClient and
     * ObjectMapper
     *
     * Allows injection of mock classes for tests
     *
     * @param providerSettings
     * @param dpc, DefaultProvenanceClient
     * @param om, ObjectMapper
     */
    public ProvenanceHandler(String providerSettings, DefaultProvenanceClient dpc, ObjectMapper om) {
        this.dpc = dpc;
        this.om = om;
        this.om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.om.configure(SerializationFeature.INDENT_OUTPUT, true);
        registerProviders(providerSettings);
    }

    /**
     * Constructor with ProviderSettings only
     *
     * @param providerSettings
     */
    public ProvenanceHandler(String providerSettings) {
        dpc = new MultiThreadedDefaultProvenanceClient();
        om = new ObjectMapper();
        om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        om.configure(SerializationFeature.INDENT_OUTPUT, true);
        registerProviders(providerSettings);
    }

    public String getProvenanceJson(
            String provenanceTypeString,
            String provenanceActionString,
            Map<FileProvenanceFilter, Set<String>> incFilters,
            Map<FileProvenanceFilter, Set<String>> excFilters
    ) throws IOException {
        ProvenanceType type = ProvenanceType.valueOf(provenanceTypeString);
        ProvenanceAction action = ProvenanceAction.valueOf(provenanceActionString);
        String json = "";

        // TODO need to paginate (or truncate) large sets of results before serializing to JSON
        switch (type) {
            case ANALYSIS:
                switch (action) {
                    case NO_FILTER:
                        json = analysisNoFilter();
                        break;
                    case INC_FILTERS:
                        json = analysisIncFilters(incFilters);
                        break;
                    case BY_PROVIDER:
                        json = analysisByProvider(incFilters);
                        break;
                    default:
                        throw new IllegalArgumentException("Action '" + action.name() + "' not supported for provenance type '" + type.name() + "'");
                }
                break;
            case FILE:
                switch (action) {
                    case NO_FILTER:
                        json = fileNoFilter();
                        break;
                    case INC_FILTERS:
                        json = fileIncFilters(incFilters);
                        break;
                    case INC_EXC_FILTERS:
                        json = fileIncExcFilters(incFilters, excFilters);
                        break;
                    default:
                        throw new IllegalArgumentException("Action '" + action.name() + "' not supported for provenance type '" + type.name() + "'");
                }
                break;
            case LANE:
                switch (action) {
                    case NO_FILTER:
                        json = laneNoFilter();
                        break;
                    case INC_FILTERS:
                        json = laneIncFilters(incFilters);
                        break;
                    case BY_PROVIDER:
                        json = laneByProvider(incFilters);
                        break;
                    case BY_PROVIDER_AND_ID:
                        json = laneByProviderAndId(incFilters);
                        break;
                    default:
                        throw new IllegalArgumentException("Action '" + action.name() + "' not supported for provenance type '" + type.name() + "'");
                }
                break;
            case SAMPLE:
                switch (action) {
                    case NO_FILTER:
                        json = sampleNoFilter();
                        break;
                    case INC_FILTERS:
                        json = sampleIncFilters(incFilters);
                        break;
                    case BY_PROVIDER:
                        json = sampleByProvider(incFilters);
                        break;
                    case BY_PROVIDER_AND_ID:
                        json = sampleByProviderAndId(incFilters);
                        break;
                    default:
                        throw new IllegalArgumentException("Action '" + action.name() + "' not supported for provenance type '" + type.name() + "'");
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown provenance type '" + type + "'");
        }
        return json;
    }

    /**
     * If only the type is given, return provenance with no filters
     *
     * @param provenanceTypeString
     * @return
     * @throws IOException
     */
    public String getProvenanceJson(String provenanceTypeString) throws IOException {
        Map<FileProvenanceFilter, Set<String>> incFilters = new HashMap<>();
        return getProvenanceJson(provenanceTypeString, ProvenanceAction.NO_FILTER.name(), incFilters);
    }

    /**
     * Exclusion filters may be omitted; they are only relevant to the
     * INC_EXC_FILTERS action
     *
     * @param provenanceTypeString
     * @param provenanceActionString
     * @param incFilters
     * @return
     * @throws IOException
     */
    public String getProvenanceJson(
            String provenanceTypeString,
            String provenanceActionString,
            Map<FileProvenanceFilter, Set<String>> incFilters
    ) throws IOException {
        if (provenanceActionString.equals(ProvenanceAction.INC_EXC_FILTERS.name())) {
            log.warn("No exclusion filter argument for action " + ProvenanceAction.INC_EXC_FILTERS.name() + "; defaulting to empty exclusion filter.");
        }
        Map<FileProvenanceFilter, Set<String>> excFilters = new HashMap<>();
        return getProvenanceJson(provenanceTypeString, provenanceActionString, incFilters, excFilters);
    }

    /**
     * Find analysis provenance with given filters
     *
     * @param filters
     * @return String; analysis provenance in JSON format
     * @throws IOException
     */
    public String analysisIncFilters(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of analysis provenance");
        Collection<AnalysisProvenance> fps = dpc.getAnalysisProvenance(filters);
        log.info("Completed download of " + fps.size() + " analysis provenance records in " + sw.stop());
        return collToJson(fps);
    }

    /**
     * Find analysis provenance with default filters
     *
     * @return
     * @throws IOException
     */
    public String analysisNoFilter()
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of file provenance");
        Collection<AnalysisProvenance> fps = dpc.getAnalysisProvenance();
        log.info("Completed download of " + fps.size() + " analysis provenance records in " + sw.stop());
        return collToJson(fps);
    }

    /**
     * Find analysis provenance by provider
     *
     * @param filters
     * @return
     * @throws IOException
     */
    public String analysisByProvider(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of analysis provenance");
        Map<String, Collection<AnalysisProvenance>> provenance = dpc.getAnalysisProvenanceByProvider(filters);
        log.info("Completed download of records for " + provenance.size() + " providers in " + sw.stop());
        return mapOfCollsToJson(new HashMap<>(provenance)); // 'new' HashMap to meet generic type requirement
    }

    /**
     * Find file provenance with given filters
     *
     * @param filters
     * @return String; file provenance in JSON format
     * @throws IOException
     */
    public String fileIncFilters(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of analysis provenance");
        Collection<FileProvenance> fps = dpc.getFileProvenance(filters);
        log.info("Completed download of " + fps.size() + " file provenance records in " + sw.stop());
        return collToJson(fps);
    }

    /**
     * Find file provenance with default filters
     *
     * @return
     * @throws IOException
     */
    public String fileNoFilter()
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of file provenance");
        Collection<FileProvenance> fps = dpc.getFileProvenance();
        log.info("Completed download of " + fps.size() + " file provenance records in " + sw.stop());
        return collToJson(fps);
    }

    /**
     * Find file provenance with inclusion & exclusion filters
     *
     * @param incFilters
     * @param excFilters
     * @return
     * @throws IOException
     */
    public String fileIncExcFilters(Map<FileProvenanceFilter, Set<String>> incFilters, Map<FileProvenanceFilter, Set<String>> excFilters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of file provenance");
        Collection<FileProvenance> fps = dpc.getFileProvenance(incFilters, excFilters);
        log.info("Completed download of " + fps.size() + " file provenance records in " + sw.stop());
        return collToJson(fps);
    }

    /**
     * Find lane provenance with given filters
     *
     * @param filters
     * @return String; lane provenance in JSON format
     * @throws IOException
     */
    public String laneIncFilters(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of lane provenance");
        Collection<LaneProvenance> lps = dpc.getLaneProvenance(filters);
        log.info("Completed download of " + lps.size() + " lane provenance records in " + sw.stop());
        return collToJson(lps);
    }

    /**
     * Find lane provenance with default filters
     *
     * @return
     * @throws IOException
     */
    public String laneNoFilter()
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of lane provenance");
        Collection<LaneProvenance> lps = dpc.getLaneProvenance();
        log.info("Completed download of " + lps.size() + " lane provenance records in " + sw.stop());
        return collToJson(lps);
    }

    /**
     *
     * @param filters
     * @return
     * @throws IOException
     */
    public String laneByProvider(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of lane provenance");
        Map<String, Collection<LaneProvenance>> provenance = dpc.getLaneProvenanceByProvider(filters);
        log.info("Completed download of provenance records for " + provenance.size() + " providers in " + sw.stop());
        return mapOfCollsToJson(new HashMap<>(provenance)); // 'new' HashMap to meet generic type requirement
    }

    /**
     *
     * @param filters
     * @return
     * @throws IOException
     */
    public String laneByProviderAndId(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of lane provenance");
        Map<String, Map<String, LaneProvenance>> provenance = dpc.getLaneProvenanceByProviderAndId(filters);
        log.info("Completed download of provenance records for " + provenance.size() + " providers in " + sw.stop());
        return mapOfMapsToJson(new HashMap<>(provenance)); // 'new' HashMap to meet generic type requirement
    }

    /**
     * Find sample provenance with given filters
     *
     * @param filters
     * @return String; sample provenance in JSON format
     * @throws IOException
     */
    public String sampleIncFilters(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of sample provenance");
        Collection<SampleProvenance> sps = dpc.getSampleProvenance(filters);
        log.info("Completed download of " + sps.size() + " sample provenance records in " + sw.stop());
        return collToJson(sps);
    }

    /**
     * Find sample provenance with default filters
     *
     * @return
     * @throws IOException
     */
    public String sampleNoFilter()
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of sample provenance");
        Collection<SampleProvenance> sps = dpc.getSampleProvenance();
        log.info("Completed download of " + sps.size() + " sample provenance records in " + sw.stop());
        return collToJson(sps);
    }

    public String sampleByProvider(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of sample provenance");
        Map<String, Collection<SampleProvenance>> spbp = dpc.getSampleProvenanceByProvider(filters);
        log.info("Completed download of records for " + spbp.size() + " providers in " + sw.stop());
        return mapOfCollsToJson(new HashMap<>(spbp)); // 'new' HashMap to meet generic type requirement
    }

    public String sampleByProviderAndId(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of sample provenance");
        Map<String, Map<String, SampleProvenance>> provenance = dpc.getSampleProvenanceByProviderAndId(filters);
        log.info("Completed download of provenance records for " + provenance.size() + " providers in " + sw.stop());
        return mapOfMapsToJson(new HashMap<>(provenance)); // 'new' HashMap to meet generic type requirement
    }

    public Integer getMaxResults() {
        return maxResults;
    }

    public void setMaxResults(Integer max) {
        if (max <= 0) {
            throw new IllegalArgumentException("Max results must be > 0");
        }
        maxResults = max;
    }

    public boolean getTruncateResults() {
        return truncateResults;
    }

    public void setTruncateResults(boolean trunc) {
        truncateResults = trunc;
    }

    /**
     * Register provenance providers; see pipedev Client class
     *
     * @param providerSettings
     */
    private void registerProviders(String providerSettings) {
        ProviderLoader providerLoader;
        try {
            providerLoader = new ProviderLoader(providerSettings);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        Set<Entry<String, AnalysisProvenanceProvider>> appSet = providerLoader.getAnalysisProvenanceProviders().entrySet();
        Set<Entry<String, LaneProvenanceProvider>> lppSet = providerLoader.getLaneProvenanceProviders().entrySet();
        Set<Entry<String, SampleProvenanceProvider>> sppSet = providerLoader.getSampleProvenanceProviders().entrySet();

        for (Entry<String, AnalysisProvenanceProvider> e : appSet) {
            dpc.registerAnalysisProvenanceProvider(e.getKey(), e.getValue());
        }
        for (Entry<String, LaneProvenanceProvider> e : lppSet) {
            dpc.registerLaneProvenanceProvider(e.getKey(), e.getValue());
        }
        for (Entry<String, SampleProvenanceProvider> e : sppSet) {
            dpc.registerSampleProvenanceProvider(e.getKey(), e.getValue());
        }
        log.info("Registered " + Integer.toString(appSet.size()) + " analysis provenance providers");
        log.info("Registered " + Integer.toString(lppSet.size()) + " lane provenance providers");
        log.info("Registered " + Integer.toString(sppSet.size()) + " sample provenance providers");
    }

    /**
     * Convert a collection of provenance objects to a JSON string Truncate
     * output if necessary
     *
     * @param pColl
     * @return
     * @throws JsonProcessingException
     */
    private String collToJson(Collection pColl) throws JsonProcessingException {
        Integer length = pColl.size();
        log.debug("Length of collection to convert to JSON: " + Integer.toString(length));
        if (truncateResults && length > maxResults) {
            // truncate output to first max elements
            log.warn("Found " + Integer.toString(length) + " provenance results, truncating to " + Integer.toString(maxResults));
            pColl = shrinkCollection(pColl, maxResults);
        }
        return om.writeValueAsString(pColl);
    }

    /**
     *
     * Convert a Map of Collections (eg. of Provenance objects) to a JSON string
     * Truncate output if necessary
     *
     * @param pColl
     * @return
     * @throws JsonProcessingException
     */
    private String mapOfCollsToJson(Map<String, Collection> pMap) throws JsonProcessingException {
        Integer total = 0;
        for (String key : pMap.keySet()) {
            total += pMap.get(key).size();
        }
        log.debug("Total provenance results: " + Integer.toString(total));
        if (truncateResults && total > maxResults) {
            // truncate output to (at most) maxResults elements (arbitrarily selected)
            log.warn("Found " + Integer.toString(total) + " provenance results, truncating to " + Integer.toString(maxResults));
            Map<String, Collection> newMap = new HashMap<>();
            Integer available = maxResults; // number of spaces available for provenance results
            for (String key : pMap.keySet()) {
                Collection pColl = pMap.get(key);
                if (available.equals(0)) {
                    break;
                } else if (pColl.size() <= available) {
                    newMap.put(key, pColl);
                    available -= pColl.size();
                } else {
                    pColl = shrinkCollection(pColl, available);
                    newMap.put(key, pColl);
                    break;
                }
            }
            pMap = newMap;
        }
        return om.writeValueAsString(pMap);
    }

    /**
     *
     * Convert a Map of Maps (eg. of Provenance objects) to a JSON string
     * Truncate output if necessary
     *
     * @param pMap
     * @return
     * @throws JsonProcessingException
     */
    private String mapOfMapsToJson(Map<String, Map> pMap) throws JsonProcessingException {

        Integer total = 0;
        for (String key : pMap.keySet()) {
            total += pMap.get(key).size();
        }
        log.debug("Total provenance results: " + Integer.toString(total));
        if (truncateResults && total > maxResults) {
            // truncate output to (at most) maxResults elements (arbitrarily selected)
            log.warn("Found " + Integer.toString(total) + " provenance results, truncating to " + Integer.toString(maxResults));
            Map<String, Map> newMap = new HashMap<>();
            Integer available = maxResults; // number of spaces available for provenance results
            for (String a : pMap.keySet()) {
                Map<String, Object> subMap = pMap.get(a);
                if (available.equals(0)) {
                    break;
                } else if (subMap.size() <= available) {
                    newMap.put(a, subMap);
                    available -= subMap.size();
                } else {
                    Collection<String> subKeys = subMap.keySet();
                    subKeys = shrinkCollection(subKeys, available);
                    Map<String, Object> newSubMap = new HashMap<>();
                    for (String b : subKeys) {
                        newSubMap.put(b, subMap.get(b));
                    }
                    newMap.put(a, newSubMap);
                    break;
                }
            }
            pMap = newMap;
        }
        return om.writeValueAsString(pMap);
    }

    /**
     * Shrink a collection to (at most) the given maximum size Choice of
     * elements to return is arbitrary
     *
     * @param coll
     * @param max
     * @return
     */
    private Collection shrinkCollection(Collection coll, int max) {
        if (coll.size() <= max) {
            return coll;
        } else {
            Object[] pArray = coll.toArray();
            Collection newColl = new ArrayList();
            for (int i = 0; i < max; i++) {
                newColl.add(pArray[i]);
            }
            return newColl;
        }
    }

}
