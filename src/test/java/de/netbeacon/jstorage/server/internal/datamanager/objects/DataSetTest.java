package de.netbeacon.jstorage.server.internal.datamanager.objects;

import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataSetTest {

    DataSet dataSet;

    @BeforeEach
    void setup(){
        JSONObject jsonObject = new JSONObject()
                .put("database", "testdb")
                .put("table", "testtable")
                .put("identifier", "testid")
                .put("testdatatype", new JSONObject().put("data", "somedata"))
                .put("testdatatype2", new JSONObject().put("data", "otherdata"));
        try{
            dataSet = new DataSet("testdb", "testtable", "testid", jsonObject);
        }catch (DataStorageException e){
            fail();
        }
    }

    @AfterEach
    void tearDown(){
        dataSet.onUnload();
    }

    @Test
    void get() {
        JSONObject result = dataSet.get("untesteddatatype", false);
        assertFalse(result.has("untesteddatatype"));
        JSONObject result2 = dataSet.get("testdatatype", false);
        assertTrue(result2.has("testdatatype"));
        // should contain the token
        JSONObject result3 = dataSet.get("testdatatype", true);
        assertTrue(result3.has("utoken"));
        // should not let u get the dataType
        JSONObject result4 = dataSet.get("testdatatype", true);
        assertFalse(result4.has("testdatatype"));
    }

    @Test
    void update() {
        JSONObject goodjsonObject = new JSONObject()
                .put("database", "testdb")
                .put("table", "testtable")
                .put("identifier", "testid")
                .put("testdatatype2", new JSONObject().put("data", "gooddata"));
        JSONObject badjsonObject = new JSONObject()
                .put("database", "testdb")
                .put("table", "testtable")
                .put("identifier", "testid")
                .put("untesteddatatype", new JSONObject().put("data", "baddata"));

        // modify without token
        assertNull(dataSet.update("testdatatype2", goodjsonObject));
        // modify with wrong data
        String token = dataSet.get("testdatatype", true).getString("utoken");
        assertFalse(dataSet.update("testdatatype", badjsonObject.put("utoken", token)));
        // modify correctly
        String token2 = dataSet.get("testdatatype2", true).getString("utoken");
        assertTrue(dataSet.update("testdatatype2", goodjsonObject.put("utoken", token2)));
        // check if update was successful
        assertEquals("gooddata", dataSet.get("testdatatype2", false).getJSONObject("testdatatype2").getString("data"));
    }

    @Test
    void insert() {
        JSONObject goodjsonObject = new JSONObject()
                .put("database", "testdb")
                .put("table", "testtable")
                .put("identifier", "testid")
                .put("othernewdatatype", new JSONObject().put("data", "gooddata"));
        assertNull(dataSet.insert("table"));
        assertFalse(dataSet.insert("testdatatype"));
        assertFalse(dataSet.insert("anotherdatatype", goodjsonObject));
        assertTrue(dataSet.insert("newdatatype"));
        assertTrue(dataSet.insert("othernewdatatype", goodjsonObject));
    }

    @Test
    void delete() {
        assertNull(dataSet.delete("table"));
        assertFalse(dataSet.delete("oldtestdatatype"));
        assertTrue(dataSet.delete("testdatatype"));
    }

    @Test
    void hasDataType() {
        assertTrue(dataSet.hasDataType("testDataType"));
        assertTrue(dataSet.hasDataType("testdatatype"));
        assertFalse(dataSet.hasDataType("untestedDataType"));
        assertFalse(dataSet.hasDataType("untesteddatatype"));
    }
}