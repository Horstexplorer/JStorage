package de.netbeacon.jstorage.server.internal.cachemanager;

import de.netbeacon.jstorage.server.internal.cachemanager.objects.Cache;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class CacheManagerTest {

    @BeforeEach
    void setUp() {
        try{
            File f = new File("./jstorage/data/cache/");
            FileUtils.deleteDirectory(f);
            new CacheManager();
        }catch (Exception e){fail();}
    }

    @AfterEach
    void tearDown() {
        try{
            CacheManager.shutdown();
            File f = new File("./jstorage/data/cache/");
            FileUtils.deleteDirectory(f);
        }catch (Exception e){fail();}
    }


    @Test
    void getCache() {
        assertFalse(CacheManager.containsCache("tEstCache"));
        try{ CacheManager.createCache("testCache"); }catch (Exception e){fail();}
        assertTrue(CacheManager.containsCache("tEstCache"));
        try{ assertNotNull(CacheManager.getCache("testcAche")); }catch (Exception e){fail();}
        try{ CacheManager.getCache("notatestcAche"); fail(); }catch (DataStorageException e){assertEquals(206, e.getType());}
    }

    @Test
    void createCache() {
        assertFalse(CacheManager.containsCache("tEstCache"));
        try{ CacheManager.createCache("testCache"); }catch (Exception e){fail();}
        assertTrue(CacheManager.containsCache("tEstCache"));
    }

    @Test
    void deleteCache() {
        assertFalse(CacheManager.containsCache("tEstCache"));
        try{ CacheManager.createCache("testCache"); }catch (Exception e){fail();}
        assertTrue(CacheManager.containsCache("tEstCache"));
        try{ CacheManager.deleteCache("testCache"); }catch (Exception e){fail();}
        assertFalse(CacheManager.containsCache("tEstCache"));
        try{ CacheManager.deleteCache("testCache"); }catch (DataStorageException e){assertEquals(206, e.getType());}
    }

    @Test
    void insertCache() {
        assertFalse(CacheManager.containsCache("tEstCache"));
        try{ CacheManager.insertCache(new Cache("testcache")); }catch (Exception e){fail();}
        assertTrue(CacheManager.containsCache("tEstCache"));
        try{ CacheManager.insertCache(new Cache("testcache")); }catch (DataStorageException e){assertEquals(216, e.getType());}
    }
}