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
        try{dataTable = new DataTable("testdatabase", "testtable");} catch (Exception e){fail();}
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
        DataSet gooddataSet = new DataSet("testdatabase", "testtable", "testid");
        try{dataTable.insertDataSet(gooddataSet);} catch (Exception e){fail();}
        assertTrue(dataTable.containsDataSet("testid"));
        try{assertEquals(gooddataSet, dataTable.getDataSet("testid"));} catch (Exception e){ fail(); }
        try{dataTable.getDataSet("missing");}catch (DataStorageException e){ assertEquals(201, e.getType()); }
    }

    @Test
    void insertDataSet() {
        DataSet gooddataSet = new DataSet("testdatabase", "testtable", "testid");
        DataSet baddataSet = new DataSet("wrongdatabase", "wrongtable", "testidother");
        assertFalse(dataTable.containsDataSet("testid"));
        assertFalse(dataTable.containsDataSet("testidother"));
        try{dataTable.insertDataSet(gooddataSet);} catch (Exception e){fail();}
        try{dataTable.insertDataSet(gooddataSet); fail();} catch (DataStorageException e){assertEquals(211, e.getType());}
        try{dataTable.insertDataSet(baddataSet); fail();} catch (DataStorageException e){assertEquals(220, e.getType());}
        assertTrue(dataTable.containsDataSet("testid"));
        assertFalse(dataTable.containsDataSet("testidother"));
    }

    @Test
    void deleteDataSet() {
        DataSet gooddataSet = new DataSet("testdatabase", "testtable", "testid");
        try{dataTable.insertDataSet(gooddataSet);} catch (Exception e){fail();}
        assertTrue(dataTable.containsDataSet("testid"));
        try{dataTable.deleteDataSet("testid");} catch (Exception e){ fail(); }
        assertFalse(dataTable.containsDataSet("testid"));
        try{dataTable.deleteDataSet("testid"); fail(); }catch (DataStorageException e){ assertEquals(201, e.getType()); }
    }

    @Test
    void containsDataSet() {
        DataSet gooddataSet = new DataSet("testdatabase", "testtable", "testid");
        try{dataTable.insertDataSet(gooddataSet);} catch (Exception e){fail();}
        assertTrue(dataTable.containsDataSet("testid"));
        assertFalse(dataTable.containsDataSet("missing"));
    }

}