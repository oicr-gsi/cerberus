/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import ca.on.oicr.gsi.cerberus.util.ProvenanceAction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import static org.apache.commons.io.FileUtils.readFileToString;

/**
 *
 * Simple command-line interface for the Cerberus web service
 *
 * @author ibancarz
 */
public class Cerberus {

    public static void main(String[] args) {

        Options options = new Options();

        options.addOption("a", "action", true, "Provenance action: One of NO_FILTER, INC_FILTERS, INC_EXC_FILTERS, BY_PROVIDER, BY_PROVIDER_AND_ID");
        options.addOption("h", "help", false, "Print help message");
        options.addOption("i", "include", true, "Path to a JSON file; include results matching filter parameters in file");
        options.addOption("n", "no-gzip", false, "Omit gzip compression of output (not recommended for large datasets)");
        options.addOption("o", "out", true, "Path to a file for JSON output");
        options.addOption("t", "type", true, "Provenance type: One of ANALYSIS, LANE, FILE, SAMPLE");
        options.addOption("u", "uri", true, "URI of the Cerberus web service");
        options.addOption("x", "exclude", true, "Path to a JSON file; exclude results matching filter parameters in file");

        CommandLineParser parser = new BasicParser();
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            System.exit(1);
        }

        if (line.hasOption("help")) {
            new HelpFormatter().printHelp("java Cerberus", options);
            System.exit(0);
        }

        URI uri = null;
        try {
            uri = new URI(line.getOptionValue("uri"));
        } catch (URISyntaxException exp) {
            System.err.println("Incorrect URI format: " + exp.getMessage());
            System.exit(1);
        }

        ProvenanceHttpClient client = new ProvenanceHttpClient(uri);

        // TODO validate inputs, eg. type and action are in respective enums
        String type = line.getOptionValue("type");
        String action = line.getOptionValue("action");

        try {
            Map<String, Set<String>> incFilters = null;

            // obtain the InputStream
            if (line.hasOption("include")) {
                incFilters = readFilterSettings(line.getOptionValue("include"));
            }
            InputStream provenanceInput = null;
            if (action.equals(ProvenanceAction.INC_EXC_FILTERS.name())) {
                Map<String, Set<String>> excFilters = readFilterSettings(line.getOptionValue("exclude"));
                provenanceInput = client.getProvenanceJson(type, action, incFilters, excFilters);
            } else if (action.equals(ProvenanceAction.NO_FILTER.name())) {
                provenanceInput = client.getProvenanceJson(type);
            } else {
                provenanceInput = client.getProvenanceJson(type, action, incFilters);
            }

            // configure the OutputStream
            OutputStream outStream = null;
            String outputPath = line.getOptionValue("out");
            if (outputPath.equals("-")) {
                outStream = System.out;
            } else {
                outStream = new FileOutputStream(new File(outputPath));
            }
            if (line.hasOption("no-gzip")) {
                outStream = new BufferedOutputStream(outStream, 64 * 1024);
            } else {
                outStream = new GZIPOutputStream(outStream, 64 * 1024);
            }

            // read from InputStream to buffer; write from buffer to OutputStream
            byte[] buffer = new byte[64 * 1024];
            int bytesRead;
            while ((bytesRead = provenanceInput.read(buffer)) != -1) {
                outStream.write(buffer, 0, bytesRead);
            }
            outStream.close();

        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
            System.exit(1);
        }

    }

    private static Map<String, Set<String>> readFilterSettings(String inputPath) throws IOException {
        Map<String, Set<String>> filters = new HashMap<>();
        ObjectMapper om = new ObjectMapper();
        String contents = readFileToString(new File(inputPath));
        JsonNode root = om.readTree(contents);
        for (Iterator<String> i = root.fieldNames(); i.hasNext();) {
            String filterName = i.next();
            JsonNode node = root.get(filterName);
            HashSet<String> set = new HashSet<>();
            for (Iterator<JsonNode> j = node.elements(); j.hasNext();) {
                JsonNode paramNode = j.next();
                set.add(paramNode.textValue());
            }
            filters.put(filterName, set);
        }
        return filters;
    }

}
