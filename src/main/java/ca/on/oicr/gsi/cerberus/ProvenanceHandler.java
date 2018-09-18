/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.cerberus.util.ProvenanceType;
import ca.on.oicr.gsi.cerberus.util.ProvenanceAction;
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
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.io.Writer;
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
 * TODO implement cacheing for provenance results</p>
 *
 * @author ibancarz
 */
public class ProvenanceHandler {

    private final DefaultProvenanceClient dpc;
    private final Logger log = LogManager.getLogger(ProvenanceHandler.class);
    private final JsonFactory factory;
    private final ObjectMapper om;
    private Writer writer;

    /**
     * Constructor with given ProviderSettings, DefaultProvenanceClient and
     * ObjectMapper
     *
     * Allows injection of mock classes for tests
     *
     * @param providerSettings
     * @param writer
     * @param dpc, DefaultProvenanceClient
     * @throws java.io.IOException
     */
    public ProvenanceHandler(String providerSettings, Writer writer, DefaultProvenanceClient dpc) throws IOException {
        this.factory = new JsonFactory();
        this.writer = writer;
        this.dpc = dpc;
        om = new ObjectMapper();
        registerProviders(providerSettings);
    }

    /**
     * Constructor with ProviderSettings only
     *
     * @param providerSettings
     * @param writer
     * @throws java.io.IOException
     */
    public ProvenanceHandler(String providerSettings, Writer writer) throws IOException {
        this.factory = new JsonFactory();
        this.writer = writer;
        om = new ObjectMapper();
        dpc = new MultiThreadedDefaultProvenanceClient();
        registerProviders(providerSettings);
    }

    public Writer getWriter() {
        return writer;
    }

    public void setWriter(Writer writer) {
        this.writer = writer;
    }

    public void writeProvenanceJson(
            String provenanceTypeString,
            String provenanceActionString,
            Map<FileProvenanceFilter, Set<String>> incFilters,
            Map<FileProvenanceFilter, Set<String>> excFilters
    ) throws IOException {
        ProvenanceType type = ProvenanceType.valueOf(provenanceTypeString);
        ProvenanceAction action = ProvenanceAction.valueOf(provenanceActionString);
        switch (type) {
            case ANALYSIS:
                switch (action) {
                    case NO_FILTER:
                        analysisNoFilter();
                        break;
                    case INC_FILTERS:
                        analysisIncFilters(incFilters);
                        break;
                    case BY_PROVIDER:
                        analysisByProvider(incFilters);
                        break;
                    default:
                        throw new IllegalArgumentException("Action '" + action.name() + "' not supported for provenance type '" + type.name() + "'");
                }
                break;
            case FILE:
                switch (action) {
                    case NO_FILTER:
                        fileNoFilter();
                        break;
                    case INC_FILTERS:
                        fileIncFilters(incFilters);
                        break;
                    case INC_EXC_FILTERS:
                        fileIncExcFilters(incFilters, excFilters);
                        break;
                    default:
                        throw new IllegalArgumentException("Action '" + action.name() + "' not supported for provenance type '" + type.name() + "'");
                }
                break;
            case LANE:
                switch (action) {
                    case NO_FILTER:
                        laneNoFilter();
                        break;
                    case INC_FILTERS:
                        laneIncFilters(incFilters);
                        break;
                    case BY_PROVIDER:
                        laneByProvider(incFilters);
                        break;
                    case BY_PROVIDER_AND_ID:
                        laneByProviderAndId(incFilters);
                        break;
                    default:
                        throw new IllegalArgumentException("Action '" + action.name() + "' not supported for provenance type '" + type.name() + "'");
                }
                break;
            case SAMPLE:
                switch (action) {
                    case NO_FILTER:
                        sampleNoFilter();
                        break;
                    case INC_FILTERS:
                        sampleIncFilters(incFilters);
                        break;
                    case BY_PROVIDER:
                        sampleByProvider(incFilters);
                        break;
                    case BY_PROVIDER_AND_ID:
                        sampleByProviderAndId(incFilters);
                        break;
                    default:
                        throw new IllegalArgumentException("Action '" + action.name() + "' not supported for provenance type '" + type.name() + "'");
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown provenance type '" + type + "'");
        }
    }

    /**
     * If only the type is given, return provenance with no filters
     *
     * @param provenanceTypeString
     * @throws IOException
     */
    public void writeProvenanceJson(String provenanceTypeString) throws IOException {
        Map<FileProvenanceFilter, Set<String>> incFilters = new HashMap<>();
        writeProvenanceJson(provenanceTypeString, ProvenanceAction.NO_FILTER.name(), incFilters);
    }

    /**
     * Exclusion filters may be omitted; they are only relevant to the
     * INC_EXC_FILTERS action
     *
     * @param provenanceTypeString
     * @param provenanceActionString
     * @param incFilters
     * @throws IOException
     */
    public void writeProvenanceJson(
            String provenanceTypeString,
            String provenanceActionString,
            Map<FileProvenanceFilter, Set<String>> incFilters
    ) throws IOException {
        if (provenanceActionString.equals(ProvenanceAction.INC_EXC_FILTERS.name())) {
            log.warn("No exclusion filter argument for action " + ProvenanceAction.INC_EXC_FILTERS.name() + "; defaulting to empty exclusion filter.");
        }
        Map<FileProvenanceFilter, Set<String>> excFilters = new HashMap<>();
        writeProvenanceJson(provenanceTypeString, provenanceActionString, incFilters, excFilters);
    }

    /**
     * Find analysis provenance with given filters
     *
     * @param filters
     * @throws IOException
     */
    private void analysisIncFilters(Map<FileProvenanceFilter, Set<String>> filters) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of analysis provenance");
        Collection<AnalysisProvenance> fps = dpc.getAnalysisProvenance(filters);
        log.info("Completed download of " + fps.size() + " analysis provenance records in " + sw.stop());
        writeCollToJson(fps);
    }

    /**
     * Find analysis provenance with default filters
     *
     * @throws IOException
     */
    private void analysisNoFilter() throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of file provenance");
        Collection<AnalysisProvenance> fps = dpc.getAnalysisProvenance();
        log.info("Completed download of " + fps.size() + " analysis provenance records in " + sw.stop());
        writeCollToJson(fps);
    }

    /**
     * Find analysis provenance by provider
     *
     * @param filters
     * @throws IOException
     */
    private void analysisByProvider(Map<FileProvenanceFilter, Set<String>> filters) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of analysis provenance");
        Map<String, Collection<AnalysisProvenance>> provenance = dpc.getAnalysisProvenanceByProvider(filters);
        log.info("Completed download of records for " + provenance.size() + " providers in " + sw.stop());
        writeMapOfCollsToJson(new HashMap<>(provenance)); // 'new' HashMap to meet generic type requirement
    }

    /**
     * Find file provenance with given filters
     *
     * @param filters
     * @throws IOException
     */
    private void fileIncFilters(Map<FileProvenanceFilter, Set<String>> filters) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of analysis provenance");
        Collection<FileProvenance> fps = dpc.getFileProvenance(filters);
        log.info("Completed download of " + fps.size() + " file provenance records in " + sw.stop());
        writeCollToJson(fps);
    }

    /**
     * Find file provenance with default filters
     *
     * @throws IOException
     */
    private void fileNoFilter() throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of file provenance");
        Collection<FileProvenance> fps = dpc.getFileProvenance();
        log.info("Completed download of " + fps.size() + " file provenance records in " + sw.stop());
        writeCollToJson(fps);
    }

    /**
     * Find file provenance with inclusion & exclusion filters
     *
     * @param incFilters
     * @param excFilters
     * @throws IOException
     */
    private void fileIncExcFilters(Map<FileProvenanceFilter, Set<String>> incFilters, Map<FileProvenanceFilter, Set<String>> excFilters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of file provenance");
        Collection<FileProvenance> fps = dpc.getFileProvenance(incFilters, excFilters);
        log.info("Completed download of " + fps.size() + " file provenance records in " + sw.stop());
        writeCollToJson(fps);
    }

    /**
     * Find lane provenance with given filters
     *
     * @param filters
     * @throws IOException
     */
    private void laneIncFilters(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of lane provenance");
        Collection<LaneProvenance> lps = dpc.getLaneProvenance(filters);
        log.info("Completed download of " + lps.size() + " lane provenance records in " + sw.stop());
        writeCollToJson(lps);
    }

    /**
     * Find lane provenance with default filters
     *
     * @throws IOException
     */
    private void laneNoFilter() throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of lane provenance");
        Collection<LaneProvenance> lps = dpc.getLaneProvenance();
        log.info("Completed download of " + lps.size() + " lane provenance records in " + sw.stop());
        writeCollToJson(lps);
    }

    /**
     *
     * @param filters
     * @throws IOException
     */
    private void laneByProvider(Map<FileProvenanceFilter, Set<String>> filters) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of lane provenance");
        Map<String, Collection<LaneProvenance>> provenance = dpc.getLaneProvenanceByProvider(filters);
        log.info("Completed download of provenance records for " + provenance.size() + " providers in " + sw.stop());
        writeMapOfCollsToJson(new HashMap<>(provenance)); // 'new' HashMap to meet generic type requirement
    }

    /**
     *
     * @param filters
     * @throws IOException
     */
    private void laneByProviderAndId(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of lane provenance");
        Map<String, Map<String, LaneProvenance>> provenance = dpc.getLaneProvenanceByProviderAndId(filters);
        log.info("Completed download of provenance records for " + provenance.size() + " providers in " + sw.stop());
        Map<String, Map<String, Object>> newMap = new HashMap<>(); // 'new' HashMap to meet generic type requirement
        for (String key : provenance.keySet()) {
            newMap.put(key, new HashMap<>(provenance.get(key)));
        }
        writeMapOfMapsToJson(newMap);
    }

    /**
     * Find sample provenance with given filters
     *
     * @param filters
     * @throws IOException
     */
    private void sampleIncFilters(Map<FileProvenanceFilter, Set<String>> filters) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of sample provenance");
        Collection<SampleProvenance> sps = dpc.getSampleProvenance(filters);
        log.info("Completed download of " + sps.size() + " sample provenance records in " + sw.stop());
        writeCollToJson(sps);
    }

    /**
     * Find sample provenance with default filters
     *
     * @throws IOException
     */
    private void sampleNoFilter() throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of sample provenance");
        Collection<SampleProvenance> sps = dpc.getSampleProvenance();
        log.info("Completed download of " + sps.size() + " sample provenance records in " + sw.stop());
        writeCollToJson(sps);
    }

    private void sampleByProvider(Map<FileProvenanceFilter, Set<String>> filters) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of sample provenance");
        Map<String, Collection<SampleProvenance>> spbp = dpc.getSampleProvenanceByProvider(filters);
        log.info("Completed download of records for " + spbp.size() + " providers in " + sw.stop());
        writeMapOfCollsToJson(new HashMap<>(spbp)); // 'new' HashMap to meet generic type requirement
    }

    private void sampleByProviderAndId(Map<FileProvenanceFilter, Set<String>> filters) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of sample provenance");
        Map<String, Map<String, SampleProvenance>> provenance = dpc.getSampleProvenanceByProviderAndId(filters);
        log.info("Completed download of provenance records for " + provenance.size() + " providers in " + sw.stop());
        Map<String, Map<String, Object>> newMap = new HashMap<>(); // 'new' HashMap to meet generic type requirement
        for (String key : provenance.keySet()) {
            newMap.put(key, new HashMap<>(provenance.get(key)));
        }
        writeMapOfMapsToJson(newMap);
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
     * @param coll
     * @return
     * @throws IOException
     */
    private void writeCollToJson(Collection coll) throws IOException {
        log.debug("Length of collection to write as JSON: " + Integer.toString(coll.size()));
        try (JsonGenerator generator = factory.createGenerator(writer)) {
            writeCollToJson(coll, generator);
        }
        log.debug("Finished writing collection as JSON");
    }

    private void writeCollToJson(Collection coll, JsonGenerator generator) throws IOException {
        generator.writeStartArray(coll.size());
        for (Object obj : coll) {
            // JsonGenerator can't parse complex objects, but ObjectMapper can
            String objString = om.writeValueAsString(obj);
            generator.writeRawValue(objString);
        }
        generator.writeEndArray();
    }

    /**
     *
     * Convert a Map of Collections (eg. of Provenance objects) to a JSON string
     *
     * @param pColl
     * @return
     * @throws IOException
     */
    private void writeMapOfCollsToJson(Map<String, Collection> map) throws IOException {
        log.debug("Ready to write map of collections as JSON.");
        Integer total = 0;
        try (JsonGenerator generator = factory.createGenerator(writer)) {
            generator.writeStartObject();
            for (String key : map.keySet()) {
                Collection coll = map.get(key);
                total += coll.size();
                generator.writeFieldName(key);
                writeCollToJson(coll, generator);
            }
            generator.writeEndObject();
        }
        log.debug("Wrote a total of " + total.toString() + " objects as JSON.");
    }

    /**
     *
     * Convert a Map of Maps (eg. of Provenance objects) to a JSON string
     *
     * @param map
     * @return
     * @throws IOException
     */
    private void writeMapOfMapsToJson(Map<String, Map<String, Object>> map) throws IOException {
        log.debug("Ready to write map of maps as JSON.");
        Integer total = 0;
        try (JsonGenerator generator = factory.createGenerator(writer)) {
            generator.writeStartObject();
            for (String key1 : map.keySet()) {
                Map<String, Object> subMap = map.get(key1);
                total += subMap.size();
                generator.writeFieldName(key1);
                generator.writeStartObject();
                for (String key2 : subMap.keySet()) {
                    generator.writeFieldName(key2);
                    // JsonGenerator can't parse complex objects, but ObjectMapper can
                    String objString = om.writeValueAsString(subMap.get(key2));
                    generator.writeRawValue(objString);
                }
                generator.writeEndObject();
            }
            generator.writeEndObject();
        }
        log.debug("Wrote a total of " + total.toString() + " objects as JSON.");
    }

}
