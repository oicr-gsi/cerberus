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
 * Simple 'main' class to test creating objects from cerberus classes
 * 
 * @author ibancarz
 */


import java.io.IOException;
import java.util.ArrayList;

public class SimpleMain {
    
    public static void main(String[] args) throws IOException {
        
        ProvenanceResult resultFoo = new ProvenanceResult("foo", "bar");
        
        String output = "Behold the provenance result:\n"+resultFoo.toJson();
        
        System.out.println(output);
        
        QueryHandler qh = new QueryHandler();
        
        ArrayList<ProvenanceResult> results;
        
        results = qh.search(null, "Bob");
        for (ProvenanceResult result : results) { 
            System.out.println(result.toJson());
        }
        
        results = qh.search("Manitoba", null);
        for (ProvenanceResult result : results) { 
            System.out.println(result.toJson());
        }
        
        
    }
    
    
}
