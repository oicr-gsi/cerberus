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
public class SimpleMain {
    
    public static void main(String[] args) {
        
        ProvenanceResult result = new ProvenanceResult("foo", "bar");
        
        String output = "Behold the provenance result:\n"+result.toJson();
        
        System.out.println(output);
        
    }
    
    
}
