package de.netbeacon.jstorage.server.internal.datamanager.objects;

import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class DataTableTest {

    private DataTable dataTable;

    @BeforeEach
    void setUp() {
        try{
            DataBase dataBase = new DataBase("testdatabase");
            dataTable = new DataTable(dataBase, "testtable");
            DataSet dataSet = new DataSet(dataBase, dataTable, "testdataset");
            dataTable.insertDataSet(dataSet);
        } catch (Exception e){fail();}
    }

    @AfterEach
    void tearDown() {
        File d = new File("./jstorage/data/testdatabase/testtable");
        if(d.exists()){ d.delete(); }
        File e = new File("./jstorage/data/testdatabase/testtable_index");
        if(e.exists()){ e.delete(); }
    }

    @Test
    void getDataSet() {
        assertFalse(dataTable.containsDataSet("testds"));
        assertTrue(dataTable.containsDataSet("testdataset"));
        try{assertEquals("testdataset", dataTable.getDataSet("testdataset").getIdentifier());} catch (Exception e){ fail(); }
        try{dataTable.getDataSet("missing");}catch (DataStorageException e){ assertEquals(201, e.getType()); }
    }

    @Test
    void insertDataSet() {
        try{
            assertTrue(dataTable.containsDataSet("testdataset"));
            assertFalse(dataTable.containsDataSet("testidother"));
            try{dataTable.insertDataSet(new DataSet(dataTable.getDataBase(), dataTable, "testdatasettwo"));} catch (Exception e){fail();}
            try{dataTable.insertDataSet(new DataSet(dataTable.getDataBase(), dataTable, "testdatasettwo")); fail();} catch (DataStorageException e){assertEquals(211, e.getType());}
            try{dataTable.insertDataSet(new DataSet(dataTable.getDataBase(), new DataTable(dataTable.getDataBase(), "someunregisteredtablewichshouldntbeused"), "testdatasettwo")); fail();} catch (DataStorageException e){assertEquals(220, e.getType());}
            assertTrue(dataTable.containsDataSet("testdataset"));
            assertTrue(dataTable.containsDataSet("testdatasettwo"));
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
    }

    @Test
    void deleteDataSet() {
        assertTrue(dataTable.containsDataSet("testdataset"));
        try{dataTable.deleteDataSet("testdataset");} catch (Exception e){ fail(); }
        assertFalse(dataTable.containsDataSet("testdataset"));
        try{dataTable.deleteDataSet("testdataset"); fail(); }catch (DataStorageException e){ assertEquals(201, e.getType()); }
    }

    @Test
    void containsDataSet() {
        assertTrue(dataTable.containsDataSet("testdataset"));
        assertFalse(dataTable.containsDataSet("missing"));
    }

}