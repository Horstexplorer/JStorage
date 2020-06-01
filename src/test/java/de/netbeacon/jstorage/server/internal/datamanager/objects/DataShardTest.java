package de.netbeacon.jstorage.server.internal.datamanager.objects;

import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class DataShardTest {

    private DataShard dataShard;
    private DataBase dataBase;
    private DataTable dataTable;

    @BeforeEach
    void setUp() {
        try{
            dataBase = new DataBase("testdatabase");
            dataTable = new DataTable(dataBase, "testtable");
            dataBase.insertTable(dataTable);
            dataShard = new DataShard(dataBase, dataTable);
        }catch (Exception e){
            fail();
        }
    }

    @AfterEach
    void tearDown() {
        try{
            File d = new File("./jstorage/data/db/");
            FileUtils.deleteDirectory(d);
        }catch (Exception e){
            fail();
        }
    }

    @Test
    void getDataSet() {
        try{
            DataSet gooddataSet = new DataSet(dataBase, dataTable, "testid");
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
            DataSet gooddataSet = new DataSet(dataBase, dataTable, "testid");
            DataBase wrong = new DataBase("wrongdb");
            DataTable wrongTable = new DataTable(wrong, "wrongtable");
            wrong.insertTable(wrongTable);
            DataSet baddataSet = new DataSet(wrong, wrongTable, "testidother");
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
        DataSet gooddataSet = new DataSet(dataBase, dataTable, "testid");
        try{dataShard.insertDataSet(gooddataSet);}catch (Exception ignore){fail();}
        assertTrue(dataShard.containsDataSet(gooddataSet.getIdentifier()));
        try{dataShard.deleteDataSet(gooddataSet.getIdentifier());}catch (Exception ignore){fail();}
        assertFalse(dataShard.containsDataSet(gooddataSet.getIdentifier()));
        try{dataShard.deleteDataSet("thisshouldnotexist"); fail();}catch (DataStorageException e){assertEquals(201, e.getType());}
    }

    @Test
    void containsDataSet() {
        DataSet gooddataSet = new DataSet(dataBase, dataTable, "testid");
        try{dataShard.insertDataSet(gooddataSet);}catch (Exception ignore){fail();}
        assertTrue(dataShard.containsDataSet(gooddataSet.getIdentifier()));
        assertFalse(dataShard.containsDataSet("thisshouldnotexist"));
    }

    @Test
    void load_unload_Data() {
        DataSet gooddataSet = new DataSet(dataBase, dataTable, "testid");
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