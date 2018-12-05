/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.model;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.JsonNode;

import ca.on.oicr.gsi.provenance.model.IusLimsKey;

/**
 *
 * @author ibancarz
 */
public abstract class ProvenanceFromJSON {
    
     protected final Collection<IusLimsKey> nodeToIusLimsKeys(JsonNode node) throws IOException {
        Collection<IusLimsKey> coll = new ArrayList<>();
        for (Iterator<JsonNode> i = node.elements(); i.hasNext();) {
            JsonNode child = i.next();
            Integer iusSWID = child.get("iusSWID").asInt();
            // read the limsKey data
            JsonNode limsKeyNode = child.get("limsKey");
            LimsKeyDto key = new LimsKeyDto();
            key.setId(limsKeyNode.get("id").textValue());
            key.setProvider(limsKeyNode.get("provider").textValue());
            key.setVersion(limsKeyNode.get("version").textValue());
            ZonedDateTime lastMod = nodeToZonedDateTime(limsKeyNode.get("lastModified"));
            key.setLastModified(lastMod);
            // construct an IusLimsKey and add to Collection
            IusLimsKeyDto iusLimsKey = new IusLimsKeyDto();
            iusLimsKey.setIusSWID(iusSWID);
            iusLimsKey.setLimsKey(key);
            coll.add(iusLimsKey);
        }
        return coll;
    }
     
    protected final Set<IusLimsKey> nodeToIusLimsKeySet(JsonNode node) throws IOException {
        // can't use TreeSet, as IusLimsKeys are not Comparable
        return new HashSet<>(nodeToIusLimsKeys(node));
    }

    protected final SortedMap<String, SortedSet<String>> nodeToSortedMap(JsonNode node) {
        SortedMap<String, SortedSet<String>> map = new TreeMap<>();
        for (Iterator<String> i = node.fieldNames(); i.hasNext();) {
            String key = i.next();
            JsonNode child = node.get(key);
            SortedSet<String> set = new TreeSet<>();
            for (Iterator<JsonNode> j = child.elements(); j.hasNext();) {
                String value = j.next().textValue();
                set.add(value);
            }
            map.put(key, set);
        }
        return map;
    }

    protected final SortedSet<Integer> nodeToSortedSetOfIntegers(JsonNode node) {
        SortedSet<Integer> set = new TreeSet<>();
        for (Iterator<JsonNode> i = node.elements(); i.hasNext();) {
            set.add(i.next().asInt());
        }
        return set;
    }

    protected final Collection<String> nodeToStringCollection(JsonNode node) {
        Collection<String> coll = new ArrayList<>();
        for (Iterator<JsonNode> i = node.elements(); i.hasNext();) {
            coll.add(i.next().textValue());
        }
        return coll;
    }

    protected final ZonedDateTime nodeToZonedDateTime(JsonNode node) throws UnsupportedOperationException {
        // standard Jackson deserializer expects a timestamp string, not a data structure
        // see https://www.baeldung.com/jackson-serialize-dates
        // instead unpack the data structure and create a ZonedDateTime "by hand"
        JsonNode zoneNode = node.get("zone");
        if (zoneNode.get("totalSeconds").asInt() != 0) {
            throw new UnsupportedOperationException("Non-zero offset in time zone identifier is not supported");
        }
        ZoneId zoneId = ZoneId.of(zoneNode.get("id").textValue());
        int year = node.get("year").asInt();
        int month = node.get("monthValue").asInt();
        int dayOfMonth = node.get("dayOfMonth").asInt();
        int hour = node.get("hour").asInt();
        int minute = node.get("minute").asInt();
        int second = node.get("second").asInt();
        int nano = node.get("nano").asInt();
        return ZonedDateTime.of(year, month, dayOfMonth, hour, minute, second, nano, zoneId);
    }

    
    
}
