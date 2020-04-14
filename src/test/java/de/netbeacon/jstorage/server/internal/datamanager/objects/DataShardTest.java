package de.netbeacon.jstorage.server.internal.datamanager.objects;

import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class DataShardTest {

    private DataShard dataShard;

    @BeforeEach
    void setUp() {
        dataShard = new DataShard("testdatabase", "testtable");
    }

    @AfterEach
    void tearDown() {
        File d = new File("./jstorage/data/testdatabase/testtable");
        if(d.exists()){ d.delete(); }
    }

    @Test
    void getDataSet() {
        try{
            DataSet gooddataSet = new DataSet("testdatabase", "testtable", "testid");
            try{dataShard.insertDataSet(gooddataSet);}catch (Exception ignore){fail();}
            assertTrue(dataShard.containsDataSet(gooddataSet.getIdentifier()));
            try{assertEquals(gooddataSet, dataShard.getDataSet("testid"));}catch (Exception e){fail();}
            try{dataShard.getDataSet("notfound");}catch (DataStorageException e){assertEquals(201, e.getType());}
        }catch (Exception e){
            fail();
        }
    }

    @Test
    void insertDataSet() {
        try{
            DataSet gooddataSet = new DataSet("testdatabase", "testtable", "testid");
            DataSet baddataSet = new DataSet("wrongdatabase", "wrongtable", "testidother");
            try{dataShard.insertDataSet(gooddataSet);}catch (Exception ignore){fail();}
            try{dataShard.insertDataSet(baddataSet); fail();}catch (DataStorageException e){assertEquals(220, e.getType());}
            assertTrue(dataShard.containsDataSet(gooddataSet.getIdentifier()));
            assertFalse(dataShard.containsDataSet(baddataSet.getIdentifier()));
        }catch (Exception e){
            fail();
        }
    }

    @Test
    void deleteDataSet() {
        DataSet gooddataSet = new DataSet("testdatabase", "testtable", "testid");
        try{dataShard.insertDataSet(gooddataSet);}catch (Exception ignore){fail();}
        assertTrue(dataShard.containsDataSet(gooddataSet.getIdentifier()));
        try{dataShard.deleteDataSet(gooddataSet.getIdentifier());}catch (Exception ignore){fail();}
        assertFalse(dataShard.containsDataSet(gooddataSet.getIdentifier()));
        try{dataShard.deleteDataSet("thisshouldnotexist"); fail();}catch (DataStorageException e){assertEquals(201, e.getType());}
    }

    @Test
    void containsDataSet() {
        DataSet gooddataSet = new DataSet("testdatabase", "testtable", "testid");
        try{dataShard.insertDataSet(gooddataSet);}catch (Exception ignore){fail();}
        assertTrue(dataShard.containsDataSet(gooddataSet.getIdentifier()));
        assertFalse(dataShard.containsDataSet("thisshouldnotexist"));
    }

    @Test
    void load_unload_Data() {
        DataSet gooddataSet = new DataSet("testdatabase", "testtable", "testid");
        try{dataShard.insertDataSet(gooddataSet);}catch (Exception ignore){fail();}
        assertTrue(dataShard.containsDataSet(gooddataSet.getIdentifier()));
        assertEquals(3, dataShard.getStatus());
        try{dataShard.unloadData(true, true, false);}catch (Exception e){fail();}
        assertFalse(dataShard.containsDataSet(gooddataSet.getIdentifier()));
        assertEquals(0, dataShard.getStatus());
        try{dataShard.loadData();}catch (Exception e){fail();}
        assertTrue(dataShard.containsDataSet(gooddataSet.getIdentifier()));
        assertEquals(3, dataShard.getStatus());
    }

}