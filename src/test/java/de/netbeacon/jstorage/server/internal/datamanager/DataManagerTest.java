package de.netbeacon.jstorage.server.internal.datamanager;

import de.netbeacon.jstorage.server.internal.datamanager.objects.DataBase;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class DataManagerTest {

    @BeforeEach
    void setUp() {
        try{
            File f = new File("./jstorage/data/db/");
            FileUtils.deleteDirectory(f);
            new DataManager();
            DataManager.createDataBase("testdatabase");
        }catch (Exception e){fail();}
    }

    @AfterEach
    void tearDown() {
        try{
            DataManager.shutdown();
            File f = new File("./jstorage/data/db/");
            FileUtils.deleteDirectory(f);
        }catch (Exception e){fail();}
    }

    @Test
    void getDataBase() {
        try{assertEquals("testdatabase", DataManager.getDataBase("testdatabase").getDataBaseName());}catch (Exception e){fail();}
        try{DataManager.getDataBase("notexisting");}catch (DataStorageException e){assertEquals(204, e.getType());}
    }

    @Test
    void createDataBase() {
        assertFalse(DataManager.containsDataBase("newdb"));
        try{DataManager.createDataBase("newdb");}catch (Exception e){fail();}
        assertTrue(DataManager.containsDataBase("newdb"));
    }

    @Test
    void deleteDataBase() {
        assertTrue(DataManager.containsDataBase("testdatabase"));
        try{DataManager.deleteDataBase("testdatabase");}catch (Exception e){fail();}
        assertFalse(DataManager.containsDataBase("testdatabase"));
        assertFalse(DataManager.containsDataBase("notexisting"));
        try{DataManager.deleteDataBase("notexisting"); fail();}catch (DataStorageException e){assertEquals(204, e.getType());}
    }

    @Test
    void containsDataBase() {
        assertTrue(DataManager.containsDataBase("testdatabase"));
        assertTrue(DataManager.containsDataBase("testDAtabase"));
        assertFalse(DataManager.containsDataBase("notExistInG"));
    }

    @Test
    void insertDataBase() {
        try{
            DataBase dataBase = new DataBase("newDBnew");
            assertFalse(DataManager.containsDataBase("newDBnew"));
            DataManager.insertDataBase(dataBase);
            assertTrue(DataManager.containsDataBase("newDBnew"));
        }catch (Exception e){fail();}
        try{
            DataBase dataBase = new DataBase("newDBnew");
            DataManager.insertDataBase(dataBase);
        }catch (DataStorageException e){
            assertEquals(214, e.getType());
        }
    }

    @Test
    void shutdown() {
        assertTrue(DataManager.containsDataBase("testDataBase"));
        try{DataManager.shutdown(); new DataManager(); }catch (Exception e){fail();}
        assertTrue(DataManager.containsDataBase("testDataBase"));
    }
}