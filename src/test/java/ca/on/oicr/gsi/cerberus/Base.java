/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import static org.apache.commons.io.FileUtils.readFileToString;

/**
 * Base class with shared methods for tests
 *
 *
 * @author ibancarz
 */
public abstract class Base {

    /**
     * Load default Provider settings from file
     *
     * @return ArrayList
     * @throws IOException
     */
    protected ArrayList<Map> getTestProviderSettings() throws IOException {

        // load the Provider settings from file
        ClassLoader classLoader = getClass().getClassLoader();
        File providerFile = new File(classLoader.getResource("providerSettings.json").getFile());
        String providerString = readFileToString(providerFile);
        ObjectMapper om = new ObjectMapper();
        ArrayList<Map> providerSettings = new ArrayList<>();
        providerSettings = om.readValue(providerString, providerSettings.getClass());
        return providerSettings;
    }

}
