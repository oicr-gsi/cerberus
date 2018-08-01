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
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Stopwatch;
import java.io.IOException;
import java.util.Collection;
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
 * <li> Return appropriate file provenance for given filters
 * </ul>
 *
 * TODO implement cacheing for provenance results
 *
 * @author ibancarz
 */
public class ProvenanceHandler {

    private final DefaultProvenanceClient dpc;
    private final Logger log = LogManager.getLogger(ProvenanceHandler.class);

    /**
     * Constructor with given ProviderSettings and DefaultProvenanceClient
     * 
     * @param providerSettings
     * @param dpc 
     */
    
    public ProvenanceHandler(String providerSettings, DefaultProvenanceClient dpc) {
        this.dpc = dpc;
        registerProviders(providerSettings);
    }
    
    /**
     * Constructor with ProviderSettings only
     * 
     * @param providerSettings 
     */

    public ProvenanceHandler(String providerSettings) {
        dpc = new MultiThreadedDefaultProvenanceClient();
        registerProviders(providerSettings);
    }

    /**
     * Find file provenance with given filters
     * 
     * @param filters
     * @return String; file provenance in JSON format
     * @throws IOException 
     */
    
    public String getFileProvenanceJson(Map<FileProvenanceFilter, Set<String>> filters)
            throws IOException {

        // Convert Strings to FileProvenanceFilters for getFileProvenance input
        Collection<FileProvenance> fpCollection = getFileProvenance(filters);
        ObjectMapper om = new ObjectMapper();

        // convert FileProvenance to JSON
        om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        om.configure(SerializationFeature.INDENT_OUTPUT, true);
        String provenanceJson = om.writeValueAsString(fpCollection);
        return provenanceJson;
    }

    private Collection<FileProvenance> getFileProvenance(Map<FileProvenanceFilter, Set<String>> filters) {
        Stopwatch sw = Stopwatch.createStarted();
        log.info("Starting download of file provenance");
        Collection<FileProvenance> fps = dpc.getFileProvenance(filters);
        log.info("Completed download of " + fps.size() + " file provenance records in " + sw.stop());
        return fps;
    }

    private void registerProviders(String providerSettings) {
        // register providers -- see pipedev Client class
        ProviderLoader providerLoader;
        try {
            providerLoader = new ProviderLoader(providerSettings);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        for (Entry<String, AnalysisProvenanceProvider> e : providerLoader.getAnalysisProvenanceProviders().entrySet()) {
            dpc.registerAnalysisProvenanceProvider(e.getKey(), e.getValue());
        }
        for (Entry<String, LaneProvenanceProvider> e : providerLoader.getLaneProvenanceProviders().entrySet()) {
            dpc.registerLaneProvenanceProvider(e.getKey(), e.getValue());
        }
        for (Entry<String, SampleProvenanceProvider> e : providerLoader.getSampleProvenanceProviders().entrySet()) {
            dpc.registerSampleProvenanceProvider(e.getKey(), e.getValue());
        }
    }

}
