/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

/**
 *
 * DRAFT
 *
 * Class to handle queries for file provenance Initially, will read and query a
 * mock data structure
 *
 * Returns ProvenanceResult objects
 *
 * @author ibancarz
 */
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.google.gson.Gson;

public class QueryHandler {

    private final HashMap<String, String> mockData;

    private final ArrayList<ProvenanceResult> allResults;

    public QueryHandler() throws IOException {

        URL url = getClass().getResource("/data/mock.json");
        List<String> lines = Files.readAllLines(Paths.get(url.getPath()));
        String inputString = String.join("\n", lines);

        Gson gson = new Gson();
        this.mockData = gson.fromJson(inputString, HashMap.class);

        this.allResults = new ArrayList();
        for (HashMap.Entry<String, String> entry : mockData.entrySet()) {
            ProvenanceResult result = new ProvenanceResult(entry.getKey(), entry.getValue());
            this.allResults.add(result);
        }

    }

    public ArrayList<ProvenanceResult> search(String name, String owner) {

        if (name == null) {
            name = "";
        }
        if (owner == null) {
            owner = "";
        }

        ArrayList<ProvenanceResult> searchResults = new ArrayList();

        //System.out.println("Running search");
        if (name.isEmpty() && owner.isEmpty()) {
            // null query; return the entire dataset
            System.out.println("Null input; returning entire dataset");
            searchResults = this.allResults;
        } else {
            // check for matching name/owner
            for (ProvenanceResult result : this.allResults) {

                if (name.isEmpty() && result.getOwner().equals(owner)) {
                    System.out.println("Name empty; owner " + owner);
                    searchResults.add(result);
                } else if (owner.isEmpty() && result.getName().equals(name)) {
                    System.out.println("Name " + name + "; owner empty");
                    searchResults.add(result);
                } else if (result.getName().equals(name) && result.getOwner().equals(owner)) {
                    System.out.println("Name " + name + "; owner " + owner);
                    searchResults.add(result);
                }

            }
        }

        return searchResults;
    }

}
