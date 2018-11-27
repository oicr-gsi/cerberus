/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ca.on.oicr.gsi.cerberus.util;

import ca.on.oicr.gsi.cerberus.util.CollectionReader;
import ca.on.oicr.gsi.provenance.model.AnalysisProvenance;
import ca.on.oicr.gsi.provenance.model.FileProvenance;
import ca.on.oicr.gsi.provenance.model.LaneProvenance;
import ca.on.oicr.gsi.provenance.model.SampleProvenance;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * Test the CollectionReader class
 *
 * @author ibancarz
 */
public class CollectionReaderTest {

    private static ClassLoader classLoader;

    public CollectionReaderTest() {
        classLoader = getClass().getClassLoader();
    }

    @Test
    public void TestAnalysisProvenance() throws FileNotFoundException, IOException {
        File apJsonFile = new File(classLoader.getResource("dummyAnalysisProvenance2.json").getFile());
        FileInputStream input = new FileInputStream(apJsonFile);
        CollectionReader reader = new CollectionReader(input);
        assertNotNull(reader);
        AnalysisProvenance ap;
        ap = reader.nextAnalysisProvenance();
        assertTrue(ap.getFileDescription().equals("A file output from the GenericMetadataSaver."));
        ap = reader.nextAnalysisProvenance();
        assertTrue(ap.getFileDescription().equals("Another file output from the GenericMetadataSaver."));
        ap = reader.nextAnalysisProvenance();
        assertNull(ap);
        ap = reader.nextAnalysisProvenance();
        assertNull(ap);
    }

    @Test
    public void TestFileProvenance() throws FileNotFoundException, IOException {
        File fpJsonFile = new File(classLoader.getResource("dummyFileProvenance.json").getFile());
        FileInputStream input = new FileInputStream(fpJsonFile);
        CollectionReader reader = new CollectionReader(input);
        assertNotNull(reader);
        FileProvenance fp;
        fp = reader.nextFileProvenance();
        assertTrue(fp.getStatusReason().equals("foo"));
        fp = reader.nextFileProvenance();
        assertTrue(fp.getStatusReason().equals("waffle"));
        fp = reader.nextFileProvenance();
        assertNull(fp);
        fp = reader.nextFileProvenance();
        assertNull(fp);
    }

    @Test
    public void TestLaneProvenance() throws FileNotFoundException, IOException {
        File lpJsonFile = new File(classLoader.getResource("dummyLaneProvenance2.json").getFile());
        FileInputStream input = new FileInputStream(lpJsonFile);
        CollectionReader reader = new CollectionReader(input);
        assertNotNull(reader);
        LaneProvenance lp;
        lp = reader.nextLaneProvenance();
        assertTrue(lp.getProvenanceId().equals("123456"));
        lp = reader.nextLaneProvenance();
        assertTrue(lp.getProvenanceId().equals("7890"));
        lp = reader.nextLaneProvenance();
        assertNull(lp);
        lp = reader.nextLaneProvenance();
        assertNull(lp);
    }

    @Test
    public void TestSampleProvenance() throws FileNotFoundException, IOException {
        File spJsonFile = new File(classLoader.getResource("dummySampleProvenance2.json").getFile());
        FileInputStream input = new FileInputStream(spJsonFile);
        CollectionReader reader = new CollectionReader(input);
        assertNotNull(reader);
        SampleProvenance sp;
        sp = reader.nextSampleProvenance();
        assertTrue(sp.getProvenanceId().equals("100001"));
        sp = reader.nextSampleProvenance();
        assertTrue(sp.getProvenanceId().equals("100002"));
        sp = reader.nextSampleProvenance();
        assertNull(sp);
        sp = reader.nextSampleProvenance();
        assertNull(sp);
    }

}
