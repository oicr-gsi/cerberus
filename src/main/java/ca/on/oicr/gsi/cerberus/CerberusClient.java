/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.cerberus.model.AnalysisProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.FileProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.LaneProvenanceFromJSON;
import ca.on.oicr.gsi.cerberus.model.SampleProvenanceFromJSON;
import ca.on.oicr.gsi.provenance.ExtendedProvenanceClient;
import ca.on.oicr.gsi.provenance.FileProvenanceFilter;
import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
public class CerberusClient implements ExtendedProvenanceClient {

    private final ProvenanceHttpClient phc;
    private final ArrayList<Map> providerSettings;
    private final Logger log = LogManager.getLogger(CerberusClient.class);
    private final ObjectMapper om;

    public CerberusClient(ArrayList<Map> providerSettings, URI uri) {
        phc = new ProvenanceHttpClient(uri);
        this.providerSettings = providerSettings;
        om = new ObjectMapper();
    }

    public CerberusClient(ArrayList<Map> providerSettings, CloseableHttpClient httpClient, HttpPost httpPost) {
        phc = new ProvenanceHttpClient(httpClient, httpPost);
        this.providerSettings = providerSettings;
        om = new ObjectMapper();
    }

    public CerberusClient(ArrayList<Map> providerSettings, ProvenanceHttpClient phc) {
        this.phc = phc;
        this.providerSettings = providerSettings;
        om = new ObjectMapper();
    }

    public Collection<AnalysisProvenance> getAnalysisProvenance() {
        String type = ProvenanceType.ANALYSIS.name();
        Collection<AnalysisProvenance> coll = new ArrayList<>();
        try {
            log.debug("Getting provenance type " + type + ", no filters");
            String jsonString = phc.getProvenanceJson(providerSettings, type);
            JsonNode root = om.readTree(jsonString);
            for (Iterator<JsonNode> j = root.elements(); j.hasNext();) {
                coll.add(new AnalysisProvenanceFromJSON(j.next()));
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
            String jsonString = phc.getProvenanceJson(providerSettings, type, action, filtersToStrings(filters));
            JsonNode root = om.readTree(jsonString);
            for (Iterator<JsonNode> j = root.elements(); j.hasNext();) {
                coll.add(new AnalysisProvenanceFromJSON(j.next()));
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
            String jsonString = phc.getProvenanceJson(providerSettings, type, action, filtersToStrings(filters));
            JsonNode root = om.readTree(jsonString);
            for (Iterator<String> i = root.fieldNames(); i.hasNext();) {
                String providerName = i.next();
                JsonNode providerNode = root.get(providerName);
                Collection<AnalysisProvenance> coll = new ArrayList<>();
                for (Iterator<JsonNode> j = providerNode.elements(); j.hasNext();) {
                    coll.add(new AnalysisProvenanceFromJSON(j.next()));
                }
                map.put(providerName, coll);
            }
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
            String jsonString = phc.getProvenanceJson(providerSettings, type);
            JsonNode root = om.readTree(jsonString);
            for (Iterator<JsonNode> j = root.elements(); j.hasNext();) {
                coll.add(new FileProvenanceFromJSON(j.next()));
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
            String jsonString = phc.getProvenanceJson(providerSettings, type, action, filtersToStrings(filters));
            JsonNode root = om.readTree(jsonString);
            for (Iterator<JsonNode> j = root.elements(); j.hasNext();) {
                coll.add(new FileProvenanceFromJSON(j.next()));
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
            String jsonString = phc.getProvenanceJson(providerSettings, type, action, filtersToStrings(incFilters), filtersToStrings(excFilters));
            JsonNode root = om.readTree(jsonString);
            for (Iterator<JsonNode> j = root.elements(); j.hasNext();) {
                coll.add(new FileProvenanceFromJSON(j.next()));
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
            String jsonString = phc.getProvenanceJson(providerSettings, type);
            JsonNode root = om.readTree(jsonString);
            for (Iterator<JsonNode> j = root.elements(); j.hasNext();) {
                coll.add(new LaneProvenanceFromJSON(j.next()));
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
            String jsonString = phc.getProvenanceJson(providerSettings, type, action, filtersToStrings(filters));
            JsonNode root = om.readTree(jsonString);
            for (Iterator<JsonNode> j = root.elements(); j.hasNext();) {
                coll.add(new LaneProvenanceFromJSON(j.next()));
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
            String jsonString = phc.getProvenanceJson(providerSettings, type, action, filtersToStrings(filters));
            JsonNode root = om.readTree(jsonString);
            for (Iterator<String> i = root.fieldNames(); i.hasNext();) {
                String providerName = i.next();
                JsonNode providerNode = root.get(providerName);
                Collection<LaneProvenance> coll = new ArrayList<>();
                for (Iterator<JsonNode> j = providerNode.elements(); j.hasNext();) {
                    coll.add(new LaneProvenanceFromJSON(j.next()));
                }
                map.put(providerName, coll);
            }
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
            String jsonString = phc.getProvenanceJson(providerSettings, type, action, filtersToStrings(filters));
            JsonNode root = om.readTree(jsonString);
            for (Iterator<String> i = root.fieldNames(); i.hasNext();) {
                String providerName = i.next();
                JsonNode providerNode = root.get(providerName);
                Map<String, LaneProvenance> subMap = new HashMap<>();
                for (Iterator<String> j = providerNode.fieldNames(); j.hasNext();) {
                    String id = j.next();
                    JsonNode idNode = providerNode.get(id);
                    subMap.put(id, new LaneProvenanceFromJSON(idNode));
                }
                map.put(providerName, subMap);
            }
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
            String jsonString = phc.getProvenanceJson(providerSettings, type);
            JsonNode root = om.readTree(jsonString);
            for (Iterator<JsonNode> j = root.elements(); j.hasNext();) {
                coll.add(new SampleProvenanceFromJSON(j.next()));
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
            String jsonString = phc.getProvenanceJson(providerSettings, type, action, filtersToStrings(filters));
            JsonNode root = om.readTree(jsonString);
            for (Iterator<JsonNode> j = root.elements(); j.hasNext();) {
                coll.add(new SampleProvenanceFromJSON(j.next()));
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
            String jsonString = phc.getProvenanceJson(providerSettings, type, action, filtersToStrings(filters));
            JsonNode root = om.readTree(jsonString);
            for (Iterator<String> i = root.fieldNames(); i.hasNext();) {
                String providerName = i.next();
                JsonNode providerNode = root.get(providerName);
                Collection<SampleProvenance> coll = new ArrayList<>();
                for (Iterator<JsonNode> j = providerNode.elements(); j.hasNext();) {
                    coll.add(new SampleProvenanceFromJSON(j.next()));
                }
                map.put(providerName, coll);
            }
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
            String jsonString = phc.getProvenanceJson(providerSettings, type, action, filtersToStrings(filters));
            JsonNode root = om.readTree(jsonString);
            for (Iterator<String> i = root.fieldNames(); i.hasNext();) {
                String providerName = i.next();
                JsonNode providerNode = root.get(providerName);
                Map<String, SampleProvenance> subMap = new HashMap<>();
                for (Iterator<String> j = providerNode.fieldNames(); j.hasNext();) {
                    String id = j.next();
                    JsonNode idNode = providerNode.get(id);
                    subMap.put(id, new SampleProvenanceFromJSON(idNode));
                }
                map.put(providerName, subMap);
            }
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
}
