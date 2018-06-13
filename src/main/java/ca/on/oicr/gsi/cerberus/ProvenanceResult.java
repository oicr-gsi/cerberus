/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

/**
 *
 * VERY DRAFT
 *
 * Placeholder class to represent a file provenance result Will be replaced by
 * output/results from the provenance package
 *
 * Constructor Instance variables for name & owner to_json method
 *
 * @author ibancarz
 */
import com.google.gson.Gson;

public class ProvenanceResult {

    private String name;
    private String owner;

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the owner
     */
    public String getOwner() {
        return owner;
    }

    /**
     * @param owner the owner to set
     */
    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     *
     * @return provenance result in JSON format
     */
    public String toJson() {

        Gson gson = new Gson();
        return gson.toJson(this);

    }

    public ProvenanceResult(String name, String owner) {
        this.name = name;
        this.owner = owner;
    }

}
