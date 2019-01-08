/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.client;

import ca.on.oicr.gsi.cerberus.util.ProvenanceType;
import ca.on.oicr.gsi.cerberus.client.util.CollectionReader;
import ca.on.oicr.gsi.cerberus.client.util.MapOfCollectionsReader;
import ca.on.oicr.gsi.cerberus.client.util.MapOfMapsReader;
import ca.on.oicr.gsi.cerberus.util.ProvenanceAction;
import ca.on.oicr.gsi.provenance.ExtendedProvenanceClient;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Class to implement the ExtendedProvenanceClient interface
 *
 * <ul>
 * <li> Makes provenance requests using ProvenanceHttpClient, gets JSON response
 * <li> Deserializes the JSON into Provenance objects
 * <li> Can be used to replace older implementations of
 * ExtendedProvenanceClient, eg. in pipedev
 * </ul>
 *
 * @author ibancarz
 */
public class CerberusClient implements ExtendedProvenanceClient, AutoCloseable {

    private final ProvenanceHttpClient phc;
    private final Logger log = LogManager.getLogger(CerberusClient.class);

    public CerberusClient(URI uri) {
        phc = new ProvenanceHttpClient(uri);
    }

    public CerberusClient(CloseableHttpClient httpClient, HttpPost httpPost) {
        phc = new ProvenanceHttpClient(httpClient, httpPost);
    }

    public CerberusClient(ProvenanceHttpClient phc) {
        this.phc = phc;
    }

    public Collection<AnalysisProvenance> getAnalysisProvenance() {
        String type = ProvenanceType.ANALYSIS.name();
        Collection<AnalysisProvenance> coll = new ArrayList<>();
        try {
            log.debug("Getting provenance type " + type + ", no filters");
            InputStream jsonStream = phc.getProvenanceJson(type);
            CollectionReader reader = new CollectionReader(jsonStream);
            AnalysisProvenance ap = reader.nextAnalysisProvenance();
            while (ap != null) {
                coll.add(ap);
                ap = reader.nextAnalysisProvenance();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return coll;
    }

    public Collection<AnalysisProvenance> getAnalysisProvenance(Map<FileProvenanceFilter, Set<String>> filters) {
        String type = ProvenanceType.ANALYSIS.name();
        String action = ProvenanceAction.INC_FILTERS.name();
        Collection<AnalysisProvenance> coll = new ArrayList<>();
        try {
            log.debug("Getting provenance type " + type + ", action " + action);
            InputStream jsonStream = phc.getProvenanceJson(type, action, filtersToStrings(filters));
            CollectionReader reader = new CollectionReader(jsonStream);
            AnalysisProvenance ap = reader.nextAnalysisProvenance();
            while (ap != null) {
                coll.add(ap);
                ap = reader.nextAnalysisProvenance();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return coll;
    }

    @Override
    public Map<String, Collection<AnalysisProvenance>> getAnalysisProvenanceByProvider(Map<FileProvenanceFilter, Set<String>> filters) {
        String type = ProvenanceType.ANALYSIS.name();
        String action = ProvenanceAction.BY_PROVIDER.name();
        Map<String, Collection<AnalysisProvenance>> map = new HashMap<>();
        try {
            log.debug("Getting provenance type " + type + ", action " + action);
            InputStream jsonStream = phc.getProvenanceJson(type, action, filtersToStrings(filters));
            map = new MapOfCollectionsReader(jsonStream).readAnalysis();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return map;
    }

    @Override
    public Collection<FileProvenance> getFileProvenance() {
        String type = ProvenanceType.FILE.name();
        Collection<FileProvenance> coll = new ArrayList<>();
        try {
            log.debug("Getting provenance type " + type + ", no filters");
            InputStream jsonStream = phc.getProvenanceJson(type);
            CollectionReader reader = new CollectionReader(jsonStream);
            FileProvenance fp = reader.nextFileProvenance();
            while (fp != null) {
                coll.add(fp);
                fp = reader.nextFileProvenance();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return coll;
    }

    @Override
    public Collection<FileProvenance> getFileProvenance(Map<FileProvenanceFilter, Set<String>> filters) {
        String type = ProvenanceType.FILE.name();
        String action = ProvenanceAction.INC_FILTERS.name();
        Collection<FileProvenance> coll = new ArrayList<>();
        try {
            log.debug("Getting provenance type " + type + ", action " + action);
            InputStream jsonStream = phc.getProvenanceJson(type, action, filtersToStrings(filters));
            CollectionReader reader = new CollectionReader(jsonStream);
            FileProvenance fp = reader.nextFileProvenance();
            while (fp != null) {
                coll.add(fp);
                fp = reader.nextFileProvenance();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return coll;
    }

    @Override
    public Collection<FileProvenance> getFileProvenance(Map<FileProvenanceFilter, Set<String>> incFilters, Map<FileProvenanceFilter, Set<String>> excFilters) {
        String type = ProvenanceType.FILE.name();
        String action = ProvenanceAction.INC_EXC_FILTERS.name();
        Collection<FileProvenance> coll = new ArrayList<>();
        try {
            log.debug("Getting provenance type " + type + ", action " + action);
            InputStream jsonStream = phc.getProvenanceJson(type, action, filtersToStrings(incFilters), filtersToStrings(excFilters));
            CollectionReader reader = new CollectionReader(jsonStream);
            FileProvenance fp = reader.nextFileProvenance();
            while (fp != null) {
                coll.add(fp);
                fp = reader.nextFileProvenance();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return coll;
    }

    @Override
    public Collection<LaneProvenance> getLaneProvenance() {
        String type = ProvenanceType.LANE.name();
        Collection<LaneProvenance> coll = new ArrayList<>();
        try {
            log.debug("Getting provenance type " + type + ", no filters");
            InputStream jsonStream = phc.getProvenanceJson(type);
            CollectionReader reader = new CollectionReader(jsonStream);
            LaneProvenance lp = reader.nextLaneProvenance();
            while (lp != null) {
                coll.add(lp);
                lp = reader.nextLaneProvenance();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return coll;
    }

    @Override
    public Collection<LaneProvenance> getLaneProvenance(Map<FileProvenanceFilter, Set<String>> filters) {
        String type = ProvenanceType.LANE.name();
        String action = ProvenanceAction.INC_FILTERS.name();
        Collection<LaneProvenance> coll = new ArrayList<>();
        try {
            log.debug("Getting provenance type " + type + ", action " + action);
            InputStream jsonStream = phc.getProvenanceJson(type, action, filtersToStrings(filters));
            CollectionReader reader = new CollectionReader(jsonStream);
            LaneProvenance lp = reader.nextLaneProvenance();
            while (lp != null) {
                coll.add(lp);
                lp = reader.nextLaneProvenance();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return coll;
    }

    @Override
    public Map<String, Collection<LaneProvenance>> getLaneProvenanceByProvider(Map<FileProvenanceFilter, Set<String>> filters) {
        String type = ProvenanceType.LANE.name();
        String action = ProvenanceAction.BY_PROVIDER.name();
        Map<String, Collection<LaneProvenance>> map = new HashMap<>();
        try {
            log.debug("Getting provenance type " + type + ", action " + action);
            InputStream jsonStream = phc.getProvenanceJson(type, action, filtersToStrings(filters));
            map = new MapOfCollectionsReader(jsonStream).readLane();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return map;
    }

    @Override
    public Map<String, Map<String, LaneProvenance>> getLaneProvenanceByProviderAndId(Map<FileProvenanceFilter, Set<String>> filters) {
        String type = ProvenanceType.LANE.name();
        String action = ProvenanceAction.BY_PROVIDER_AND_ID.name();
        Map<String, Map<String, LaneProvenance>> map = new HashMap<>();
        try {
            log.debug("Getting provenance type " + type + ", action " + action);
            InputStream jsonStream = phc.getProvenanceJson(type, action, filtersToStrings(filters));
            map = new MapOfMapsReader(jsonStream).readLane();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return map;
    }

    @Override
    public Collection<SampleProvenance> getSampleProvenance() {
        String type = ProvenanceType.SAMPLE.name();
        Collection<SampleProvenance> coll = new ArrayList<>();
        try {
            log.debug("Getting provenance type " + type + ", no filters");
            InputStream jsonStream = phc.getProvenanceJson(type);
            CollectionReader reader = new CollectionReader(jsonStream);
            SampleProvenance sp = reader.nextSampleProvenance();
            while (sp != null) {
                coll.add(sp);
                sp = reader.nextSampleProvenance();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return coll;
    }

    @Override
    public Collection<SampleProvenance> getSampleProvenance(Map<FileProvenanceFilter, Set<String>> filters) {
        String type = ProvenanceType.SAMPLE.name();
        String action = ProvenanceAction.INC_FILTERS.name();
        Collection<SampleProvenance> coll = new ArrayList<>();
        try {
            log.debug("Getting provenance type " + type + ", action " + action);
            InputStream jsonStream = phc.getProvenanceJson(type, action, filtersToStrings(filters));
            CollectionReader reader = new CollectionReader(jsonStream);
            SampleProvenance sp = reader.nextSampleProvenance();
            while (sp != null) {
                coll.add(sp);
                sp = reader.nextSampleProvenance();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return coll;
    }

    @Override
    public Map<String, Collection<SampleProvenance>> getSampleProvenanceByProvider(Map<FileProvenanceFilter, Set<String>> filters) {
        String type = ProvenanceType.SAMPLE.name();
        String action = ProvenanceAction.BY_PROVIDER.name();
        Map<String, Collection<SampleProvenance>> map = new HashMap<>();
        try {
            log.debug("Getting provenance type " + type + ", action " + action);
            InputStream jsonStream = phc.getProvenanceJson(type, action, filtersToStrings(filters));
            map = new MapOfCollectionsReader(jsonStream).readSample();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return map;
    }

    @Override
    public Map<String, Map<String, SampleProvenance>> getSampleProvenanceByProviderAndId(Map<FileProvenanceFilter, Set<String>> filters) {
        String type = ProvenanceType.SAMPLE.name();
        String action = ProvenanceAction.BY_PROVIDER_AND_ID.name();
        Map<String, Map<String, SampleProvenance>> map = new HashMap<>();
        try {
            log.debug("Getting provenance type " + type + ", action " + action);
            InputStream jsonStream = phc.getProvenanceJson(type, action, filtersToStrings(filters));
            map = new MapOfMapsReader(jsonStream).readSample();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return map;
    }

    private Map<String, Set<String>> filtersToStrings(Map<FileProvenanceFilter, Set<String>> filters) {
        Map<String, Set<String>> filterStrings = new HashMap<>();
        for (FileProvenanceFilter filter : filters.keySet()) {
            filterStrings.put(filter.name(), filters.get(filter));
        }
        return filterStrings;
    }

    @Override
    public void close() throws Exception {
        phc.close();
    }
}
