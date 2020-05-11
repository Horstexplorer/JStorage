package de.netbeacon.jstorage.server.internal.datamanager.objects;

import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class DataBaseTest {

    private DataBase dataBase;
    private DataTable dataTable;

    @BeforeEach
    void setUp() {
        try{dataBase = new DataBase("testdatabase");
            dataTable = new DataTable(dataBase, "testtable");
            dataBase.insertTable(dataTable);}catch (Exception e){
            e.printStackTrace();
            fail();}
    }

    @AfterEach
    void tearDown() {
        File d = new File("./jstorage/data/testdatabase");
        if(d.exists()){ d.delete(); }
    }

    @Test
    void getTable() {
        try{assertEquals("testtable", dataBase.getTable("testtable").getIdentifier());}catch (Exception e){fail();}
        try{dataBase.getTable("missingtable");} catch (DataStorageException e){assertEquals(203, e.getType());}
    }

    @Test
    void insertTable() {
        try{dataBase.insertTable(dataTable);}catch (DataStorageException e){assertEquals(213, e.getType());}
        try{dataBase.insertTable(new DataTable(new DataBase("wrongdb"), "testtable"));}catch (DataStorageException e){assertEquals(220, e.getType());}
    }

    @Test
    void deleteTable() {
        try{dataBase.deleteTable(dataTable.getIdentifier());}catch (DataStorageException e){fail();}
        try{dataBase.deleteTable(dataTable.getIdentifier()); fail();}catch (DataStorageException e){assertEquals(203, e.getType());}
    }

    @Test
    void containsDataTable() {
        assertTrue(dataBase.containsDataTable(dataTable.getIdentifier()));
        assertFalse(dataBase.containsDataTable("notexisting"));
    }
}