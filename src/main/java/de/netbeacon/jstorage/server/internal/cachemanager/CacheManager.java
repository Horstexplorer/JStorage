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
import de.netbeacon.jstorage.server.tools.ssl.SSLContextFactory;
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

/**
 * This class takes care of preparing and accessing all Cache objects
 * <p>
 * The user should only access the objects through this class but should be able to create them outside of it
 * This works quite similar to the dataManager but the usage and protection should be a lot easier
 *
 * @author horstexplorer
 */
public class CacheManager {

    private static final AtomicBoolean ready = new AtomicBoolean(false);
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);
    private static final ConcurrentHashMap<String, Cache> caches = new ConcurrentHashMap<>();
    private static ScheduledExecutorService ses;
    private static Future<?> cleanTask;
    private static Future<?> unloadTask;
    private static Future<?> snapshotTask;
    private static final Logger logger = LoggerFactory.getLogger(CacheManager.class);

    /**
     * Used to set up all CachedData objects
     *
     * @throws SetupException on setup() throwing an error
     */
    public CacheManager() throws SetupException{
        if(!ready.get() && !shutdown.get()){
            setup();
            ready.set(true);
        }
    }

    /*                  OBJECT                  */

    /**
     * Used to get a cache from the manager
     *
     * @param identifier of the selected cache, converted to lowercase
     * @return Cache cache
     * @throws DataStorageException on exceptions such as the manager not being ready & the object not being found
     */
    public static Cache getCache(String identifier) throws DataStorageException {
        if(ready.get()){
            identifier = identifier.toLowerCase();
            if(caches.containsKey(identifier)){
                return caches.get(identifier);
            }
            logger.debug("Cache "+identifier+" Not Found");
            throw new DataStorageException(206, "CacheManager: Cache "+identifier+" Not Found.");
        }
        logger.error("Not Ready Yet");
        throw new DataStorageException(231, "CacheManager Not Ready Yet");
    }

    /**
     * Used to create a cache
     *
     * @param identifier of the new cache
     * @return Cache cache
     * @throws DataStorageException on exceptions such as the manager not being ready & the object already existing
     */
    public static Cache createCache(String identifier) throws DataStorageException {
        if(ready.get()){
            identifier = identifier.toLowerCase();
            if(!caches.containsKey(identifier)){
                Cache cache = new Cache(identifier);
                caches.put(cache.getCacheIdentifier(), cache);
                logger.debug("Cache "+identifier+" Created");
                return cache;
            }
            logger.debug("Cache "+identifier+" Already Existing");
            throw new DataStorageException(216, "CacheManager: Cache "+identifier+" Already Existing.");
        }
        logger.error("Not Ready Yet");
        throw new DataStorageException(231, "CacheManager Not Ready Yet");
    }

    /**
     * Used to delete a cache from the manager
     *
     * @param identifier of the selected cache, converted to lowercase
     * @throws DataStorageException on exceptions such as the manager not being ready & the object not being found
     */
    public static void deleteCache(String identifier) throws DataStorageException {
        if(ready.get()){
            identifier = identifier.toLowerCase();
            if(caches.containsKey(identifier)){
                Cache cache = caches.get(identifier);
                caches.remove(identifier);
                cache.unloadData(false, false, true);
                logger.debug("Cache "+identifier+" Deleted");
                return;
            }
            logger.debug("Cache "+identifier+" Not Found");
            throw new DataStorageException(206, "CacheManager: Cache "+identifier+" Not Found.");
        }
        logger.error("Not Ready Yet");
        throw new DataStorageException(231, "CacheManager Not Ready Yet");
    }

    /**
     * Used to check if a cache is existing
     *
     * @param identifier of the cache, converted to lowercase
     * @return boolean boolean
     */
    public static boolean containsCache(String identifier){
        return caches.containsKey(identifier.toLowerCase());
    }

    /*                  ADVANCED                    */

    /**
     * Used to insert a cache created outside the manager
     *
     * @param cache which should be inserted
     * @throws DataStorageException on exceptions such as the manager not being ready & the object already existing
     */
    public static void insertCache(Cache cache) throws DataStorageException {
        if(ready.get()){
            if(!caches.containsKey(cache.getCacheIdentifier())){
                caches.put(cache.getCacheIdentifier(), cache);
                return;
            }
            throw new DataStorageException(216, "CacheManager: Cache "+cache.getCacheIdentifier()+" Already Existing.");
        }
        throw new DataStorageException(231, "CacheManager Not Ready Yet");
    }

    /*                  MISC                    */

    /**
     * Used for initial setup of the CacheManager
     * <p>
     * Creates all listed Cache objects. See {@link Cache}
     *
     * @throws SetupException on error
     */
    private void setup() throws SetupException{
        if(!ready.get() && !shutdown.get()){
            try{
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
                                    caches.put(cache.getCacheIdentifier(), cache);
                                }
                            }catch (Exception e){
                                System.err.println("CacheManager: Setup: Error Creating Cache: "+jsonArray.getString(i).toLowerCase()+": "+e.getMessage());
                            }
                        }
                    }
                }
                ses = Executors.newScheduledThreadPool(1);
                unloadTask = ses.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        caches.entrySet().stream().filter(v->v.getValue().isAdaptive() && v.getValue().getLastAccess()+900000 < System.currentTimeMillis() && v.getValue().getStatus() == 3).forEach(e->e.getValue().unloadDataAsync(true, true, false));
                    }
                }, 5, 5, TimeUnit.SECONDS);
                snapshotTask = ses.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        caches.entrySet().stream().filter(v -> v.getValue().getLastAccess()+850000 > System.currentTimeMillis() && v.getValue().getStatus() == 3).forEach(e->e.getValue().unloadDataAsync(false, true, true));
                    }
                }, 30, 30, TimeUnit.MINUTES);
                cleanTask = ses.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        caches.entrySet().stream().filter(v ->v.getValue().getLastAccess()+850000 > System.currentTimeMillis() && v.getValue().getStatus() == 3).forEach(e->e.getValue().getDataPool().entrySet().stream().filter(v -> !v.getValue().isValid()).forEach(k-> {
                            try {e.getValue().deleteCachedData(k.getValue().getIdentifier());} catch (DataStorageException ignore) {}
                        }));
                    }
                }, 10, 10, TimeUnit.MINUTES);
            }catch (Exception e){
                throw new SetupException("CacheManager: Setup Failed: "+e.getMessage());
            }
        }
    }

    /**
     * Used to store content on shutdown
     *
     * @throws ShutdownException on any error. This may result in data getting lost.
     */
    public static void shutdown() throws ShutdownException {
        if(ready.get()){
            shutdown.set(true);
            ready.set(false);
            // write config file
            try{
                // build json
                JSONObject jsonObject = new JSONObject();
                JSONArray cachesJ = new JSONArray();
                caches.forEach((key, value)->{
                    JSONObject cJ = new JSONObject()
                            .put("identifier", value.getCacheIdentifier())
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
                        System.out.println("Cache: "+caches+": Unloading Data From Cache"+v.getCacheIdentifier()+" Failed, Data May Be Lost");
                    }
                });
                caches.clear();
                // reset shutdown
                shutdown.set(false);
            }catch (Exception e){
                throw new ShutdownException("CacheManager: Shutdown Failed. Data May Be Lost: "+e.getMessage());
            }
        }
    }

    /*                  POOL                  */

    /**
     * Returns the internal storage object for all Cache objects
     *
     * @return ConcurrentHashMap<String, Cache> data pool
     */
    public static ConcurrentHashMap<String, Cache> getDataPool() {
        return caches;
    }

}
