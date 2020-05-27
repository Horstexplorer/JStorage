package de.netbeacon.jstorage.server.internal.datamanager.objects;

import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.meta.UsageStatistics;
import org.json.JSONObject;
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

    @Test
    void fixedStructure(){
        JSONObject fstructure = new JSONObject().put("ida", new JSONObject().put("key", "value")).put("idb", new JSONObject().put("key", 120));
        assertTrue(dataTable.getDefaultStructure().isEmpty());
        dataTable.setDefaultStructure(fstructure);
        assertFalse(dataTable.getDefaultStructure().isEmpty());
        // create new Dataset
        DataSet bad = new DataSet(dataTable.getDataBase(), dataTable,"bad"); // does not match expected structure
        try{dataTable.insertDataSet(bad); fail();}catch (DataStorageException e){ assertEquals(221, e.getType());}
        DataSet good = null;
        try{good = new DataSet(dataTable.getDataBase(), dataTable, "good", fstructure.put("database", dataTable.getDataBase().getIdentifier()).put("table", dataTable.getIdentifier()).put("identifier", "good"));}catch (Exception e){fail();}
        try{dataTable.insertDataSet(good); }catch (Exception e){ e.printStackTrace(); fail(); }
        DataSet good2 = null;
        try{good2 = new DataSet(dataTable.getDataBase(), dataTable, "good2", fstructure.put("ida", new JSONObject().put("key", "othervalue")).put("database", dataTable.getDataBase().getIdentifier()).put("table", dataTable.getIdentifier()).put("identifier", "good2"));}catch (Exception e){fail();}
        try{dataTable.insertDataSet(good2); }catch (Exception e){ e.printStackTrace(); fail(); }
    }

    @Test
    void optimize(){
        // settings
        DataShard.setMaxDataSets(2);
        try{
            DataBase db = dataTable.getDataBase(); // we just reuse the db, nothing else
            DataTable table = new DataTable(db, "unoptimizedtable");
            db.insertTable(table);
            // create 4 datasets
            DataSet a = new DataSet(db, table, "a");
            DataSet b = new DataSet(db, table, "b");
            DataSet c = new DataSet(db, table, "c");
            DataSet d = new DataSet(db, table, "d");
            // insert them
            table.insertDataSet(a);
            table.insertDataSet(b);
            table.insertDataSet(c);
            table.insertDataSet(d);
            // check if they are correctly placed
            assertEquals(2, table.getDataPool().size());
            assertEquals(4, table.getIndexPool().size());
            // a & c should be on individual shards
            assertNotEquals(table.getIndexPool().get("a"), table.getIndexPool().get("c"));
            // fake statistics a & c
            UsageStatistics ad = table.getStatisticsFor("a");
            UsageStatistics cd = table.getStatisticsFor("c");
            ad.add(UsageStatistics.Usage.acquire_success);
            cd.add(UsageStatistics.Usage.acquire_success);
            // test stats
            assertEquals(1, ad.getCountFor(UsageStatistics.Usage.any));
            assertEquals(1, cd.getCountFor(UsageStatistics.Usage.any));
            // optimize
            table.optimize();
            // check if nothing exploded
            assertEquals(2, table.getDataPool().size());
            assertEquals(4, table.getIndexPool().size());
            // a & c should be on the same shard now
            assertEquals(table.getIndexPool().get("a"), table.getIndexPool().get("c"));
        }catch (Exception e){
            fail();
        }
        // reset settings just in case
        DataShard.setMaxDataSets(0);
    }
}