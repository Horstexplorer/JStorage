package de.netbeacon.jstorage.server.internal.cachemanager.objects;

import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CacheTest {

    private Cache cache;

    @BeforeEach
    void setUp(){
        cache = new Cache("testCache");
        try{cache.loadData();}catch (Exception e){fail();}
    }

    @AfterEach
    void tearDown(){
        try{cache.unloadData(false, false, true);}catch (Exception e){fail();}
    }

    @Test
    void getCachedData() {
        try{ cache.getCachedData("someData"); fail(); }catch(DataStorageException e){ assertEquals(205, e.getType());}
        try{ cache.insertCachedData(new CachedData("testcAche", "somEdata", new JSONObject().put("somestring", "somestring")));} catch (Exception e){ fail(); }
        try{ assertEquals("somestring", cache.getCachedData("somedata").getData().getString("somestring")); }catch (Exception e){ fail(); }
    }

    @Test
    void insertCachedData() {
        CachedData cachedData = new CachedData("testcAche", "somEdata", new JSONObject().put("somestring", "somestring"));
        try{ cache.insertCachedData(cachedData);} catch (Exception e){ fail(); }
        try{ cache.insertCachedData(cachedData); fail(); } catch (DataStorageException e){ assertEquals(242, e.getType()); }

    }

    @Test
    void deleteCachedData() {
        CachedData cachedData = new CachedData("testcAche", "somEdata", new JSONObject().put("somestring", "somestring"));
        cachedData.setValidForDuration(10);
        try{ cache.deleteCachedData("sOmeData"); fail(); }catch (DataStorageException e){ assertEquals(205, e.getType()); }
        try{ cache.insertCachedData(cachedData);} catch (Exception e){ fail(); }
        try{ cache.deleteCachedData("SOMeData"); fail(); }catch (DataStorageException e){ assertEquals(242, e.getType()); }
        cachedData.setValidForDuration(-1);
        try{ cache.deleteCachedData("soMeData"); }catch (Exception e){ fail(); }
    }

    @Test
    void containsValidCachedData() {
        CachedData cachedData = new CachedData("testcAche", "somEdata", new JSONObject().put("somestring", "somestring"));
        cachedData.setValidForDuration(10);
        assertFalse(cache.containsValidCachedData("somedata"));
        try{ cache.insertCachedData(cachedData);} catch (Exception e){ fail(); }
        assertTrue(cache.containsValidCachedData("somedata"));
        cachedData.setValidForDuration(-1);
        assertTrue(cache.containsValidCachedData("somedata"));
        cachedData.setValidForDuration(1);
        try{TimeUnit.MILLISECONDS.sleep(1010);}catch (Exception e){fail();}
        assertFalse(cache.containsValidCachedData("somedata"));
    }
}