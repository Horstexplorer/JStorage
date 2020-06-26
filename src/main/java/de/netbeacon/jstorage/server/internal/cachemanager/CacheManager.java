/*
 *     Copyright 2020 Horstexplorer @ https://www.netbeacon.de
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.netbeacon.jstorage.server.internal.cachemanager;

import de.netbeacon.jstorage.server.internal.cachemanager.objects.Cache;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.SetupException;
import de.netbeacon.jstorage.server.tools.exceptions.ShutdownException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class takes care of preparing and accessing all Cache objects
 * <p>
 * The user should only access the objects through this class but should be able to create them outside of it
 * This works quite similar to the dataManager but the usage and protection should be a lot easier
 *
 * @author horstexplorer
 */
public class CacheManager {

    private static CacheManager instance;

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, Cache> caches = new ConcurrentHashMap<>();
    private ScheduledExecutorService ses;
    private Future<?> cleanTask;
    private Future<?> unloadTask;
    private Future<?> snapshotTask;
    private final Logger logger = LoggerFactory.getLogger(CacheManager.class);
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Used to create an instance of this class
     */
    private CacheManager(){}

    /**
     * Used to get the instance of this class without forcing initialization
     * @return CacheManager
     */
    public static CacheManager getInstance(){
        return getInstance(false);
    }

    /**
     * Used to get the instance of this class
     * <p>
     * Can be used to initialize the class if this didnt happened yet
     * @param initializeIfNeeded boolean
     * @return CacheManager
     */
    public static CacheManager getInstance(boolean initializeIfNeeded){
        if((instance == null && initializeIfNeeded)){
            instance = new CacheManager();
        }
        return instance;
    }

    /*                  OBJECT                  */

    /**
     * Used to get a cache from the manager
     *
     * @param identifier of the selected cache, converted to lowercase
     * @return Cache cache
     * @throws DataStorageException on exceptions such as the manager not being ready & the object not being found
     */
    public Cache getCache(String identifier) throws DataStorageException {
        if(!ready.get()){
            logger.error("Not Ready Yet");
            throw new DataStorageException(231, "CacheManager Not Ready Yet");
        }
        identifier = identifier.toLowerCase();
        if(!caches.containsKey(identifier)){
            logger.debug("Cache "+identifier+" Not Found");
            throw new DataStorageException(206, "CacheManager: Cache "+identifier+" Not Found.");
        }
        return caches.get(identifier);
    }

    /**
     * Used to create a cache
     *
     * @param identifier of the new cache
     * @return Cache cache
     * @throws DataStorageException on exceptions such as the manager not being ready & the object already existing
     */
    public Cache createCache(String identifier) throws DataStorageException {
        if(!ready.get()){
            logger.error("Not Ready Yet");
            throw new DataStorageException(231, "CacheManager Not Ready Yet");
        }
        identifier = identifier.toLowerCase();
        if(caches.containsKey(identifier)){
            logger.debug("Cache "+identifier+" Already Existing");
            throw new DataStorageException(216, "CacheManager: Cache "+identifier+" Already Existing.");
        }
        Cache cache = new Cache(identifier);
        caches.put(cache.getIdentifier(), cache);
        logger.debug("Cache "+identifier+" Created");
        return cache;
    }

    /**
     * Used to delete a cache from the manager
     *
     * @param identifier of the selected cache, converted to lowercase
     * @throws DataStorageException on exceptions such as the manager not being ready & the object not being found
     */
    public void deleteCache(String identifier) throws DataStorageException {
        if(!ready.get()){
            logger.error("Not Ready Yet");
            throw new DataStorageException(231, "CacheManager Not Ready Yet");
        }
        identifier = identifier.toLowerCase();
        if(!caches.containsKey(identifier)){
            logger.debug("Cache "+identifier+" Not Found");
            throw new DataStorageException(206, "CacheManager: Cache "+identifier+" Not Found.");
        }
        Cache cache = caches.get(identifier);
        caches.remove(identifier);
        cache.unloadData(false, false, true);
        logger.debug("Cache "+identifier+" Deleted");
    }

    /**
     * Used to check if a cache is existing
     *
     * @param identifier of the cache, converted to lowercase
     * @return boolean boolean
     */
    public boolean containsCache(String identifier){
        return caches.containsKey(identifier.toLowerCase());
    }

    /*                  ADVANCED                    */

    /**
     * Used to insert a cache created outside the manager
     *
     * @param cache which should be inserted
     * @throws DataStorageException on exceptions such as the manager not being ready & the object already existing
     */
    public void insertCache(Cache cache) throws DataStorageException {
        if(!ready.get()){
            throw new DataStorageException(231, "CacheManager Not Ready Yet");
        }
        if(caches.containsKey(cache.getIdentifier())){
            throw new DataStorageException(216, "CacheManager: Cache "+cache.getIdentifier()+" Already Existing.");
        }
        caches.put(cache.getIdentifier(), cache);
    }

    /*                  MISC                    */

    /**
     * Used for initial setup of the CacheManager
     * <p>
     * Creates all listed Cache objects. See {@link Cache}
     *
     * @throws SetupException on error
     */
    public void setup() throws SetupException{
        try{
            lock.lock();
            if(ready.get() || shutdown.get()){
                return;
            }
            File d = new File("./jstorage/data/cache/");
            if(!d.exists()){ d.mkdirs(); }
            File f = new File("./jstorage/data/cache/cachemanager");
            if(!f.exists()){ f.createNewFile(); }
            else{
                // read
                String content = new String(Files.readAllBytes(f.toPath()));
                if(!content.isEmpty()){
                    JSONObject jsonObject = new JSONObject(content);
                    JSONArray jsonArray = jsonObject.getJSONArray("caches");
                    // might contain other settings in the future
                    for(int i = 0; i < jsonArray.length(); i++){
                        try{
                            JSONObject jsonObject1 = jsonArray.getJSONObject(i);
                            String cID = jsonObject1.getString("identifier").toLowerCase();
                            boolean adl = jsonObject1.getBoolean("adaptiveLoad");
                            if(!caches.containsKey(cID)){
                                Cache cache = new Cache(cID);
                                cache.setAdaptiveLoading(adl);
                                if(adl){
                                    cache.loadDataAsync();
                                }
                                caches.put(cache.getIdentifier(), cache);
                            }
                        }catch (Exception e){
                            System.err.println("CacheManager: Setup: Error Creating Cache: "+jsonArray.getString(i).toLowerCase()+": "+e.getMessage());
                        }
                    }
                }
            }
            ses = Executors.newScheduledThreadPool(1);
            unloadTask = ses.scheduleAtFixedRate(() -> caches.entrySet().stream().filter(v->v.getValue().isAdaptive() && v.getValue().getLastAccess()+900000 < System.currentTimeMillis() && v.getValue().getStatus() == 3).forEach(e->e.getValue().unloadDataAsync(true, true, false)), 5, 5, TimeUnit.SECONDS);
            snapshotTask = ses.scheduleAtFixedRate(() -> caches.entrySet().stream().filter(v -> v.getValue().getLastAccess()+850000 > System.currentTimeMillis() && v.getValue().getStatus() == 3).forEach(e->e.getValue().unloadDataAsync(false, true, true)), 30, 30, TimeUnit.MINUTES);
            cleanTask = ses.scheduleAtFixedRate(() -> caches.entrySet().stream().filter(v ->v.getValue().getLastAccess()+850000 > System.currentTimeMillis() && v.getValue().getStatus() == 3).forEach(e->e.getValue().getDataPool().entrySet().stream().filter(v -> !v.getValue().isValid()).forEach(k-> {
                try {e.getValue().deleteCachedData(k.getValue().getIdentifier());} catch (DataStorageException ignore) {}
            })), 10, 10, TimeUnit.MINUTES);
            ready.set(true);
        }catch (Exception e){
            throw new SetupException("CacheManager: Setup Failed: "+e.getMessage());
        }finally {
            lock.unlock();
        }
    }

    /**
     * Used to store content on shutdown
     *
     * @throws ShutdownException on any error. This may result in data getting lost.
     */
    public void shutdown() throws ShutdownException {
        try{
            lock.lock();
            if(!ready.get()){
                return;
            }
            shutdown.set(true);
            ready.set(false);
            // build json
            JSONObject jsonObject = new JSONObject();
            JSONArray cachesJ = new JSONArray();
            caches.forEach((key, value)->{
                JSONObject cJ = new JSONObject()
                        .put("identifier", value.getIdentifier())
                        .put("adaptiveLoad", value.isAdaptive());
                cachesJ.put(cJ);
            });
            jsonObject.put("caches", cachesJ);
            // write to file
            File d = new File("./jstorage/data/cache/");
            if(!d.exists()){ d.mkdirs(); }
            File f = new File("./jstorage/data/cache/cachemanager");
            BufferedWriter writer = new BufferedWriter(new FileWriter(f));
            writer.write(jsonObject.toString());
            writer.newLine();
            writer.flush();
            writer.close();
            // shutdown & clear everything
            cleanTask.cancel(true);
            unloadTask.cancel(true);
            snapshotTask.cancel(true);
            ses.shutdown();
            caches.forEach((k, v)->{
                try{
                    v.unloadData(true, true, false);
                }catch (DataStorageException e){
                    System.out.println("Cache: "+caches+": Unloading Data From Cache"+v.getIdentifier()+" Failed, Data May Be Lost");
                }
            });
            caches.clear();
            // reset shutdown
            shutdown.set(false);
        }catch (Exception e){
            throw new ShutdownException("CacheManager: Shutdown Failed. Data May Be Lost: "+e.getMessage());
        }finally {
            lock.unlock();
        }
    }

    /*                  POOL                  */

    /**
     * Returns the internal storage object for all Cache objects
     *
     * @return ConcurrentHashMap<String, Cache> data pool
     */
    public ConcurrentHashMap<String, Cache> getDataPool() {
        return caches;
    }

}
