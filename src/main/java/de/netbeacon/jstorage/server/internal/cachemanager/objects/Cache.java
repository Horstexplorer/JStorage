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

package de.netbeacon.jstorage.server.internal.cachemanager.objects;

import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class represents a cache for internal objects
 *
 * @author horstexplorer
 */
public class Cache {

    private final String identifier;
    private final ConcurrentHashMap<String, CachedData> cachedData = new ConcurrentHashMap<>();

    private final AtomicLong lastAccess = new AtomicLong();

    private final AtomicBoolean adaptiveLoad = new AtomicBoolean(false);
    private final AtomicInteger status = new AtomicInteger(0); // -2 - insufficient memory error | -1 - general_error | 0 - unloaded | 1 - unloading | 2 - loading | 3 - loaded/ready
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final Logger logger = LoggerFactory.getLogger(Cache.class);

    /**
     * Used to create a new cache
     *
     * @param identifier of the new cache, will be converted to lowercase
     */
    public Cache(String identifier){
        this.identifier = identifier.toLowerCase();
        logger.debug("Cache: New Cache Created: "+this.identifier);
    }

    /*                  OBJECT                  */

    /**
     * Returns the identifier of this object
     *
     * @return String string
     */
    public String getIdentifier(){ return identifier; }

    /**
     * Returns the timestamp of the last access for this object
     *
     * @return long long
     */
    public long getLastAccess(){ return lastAccess.get(); }

    /**
     * Returns the current size of the cache
     *
     * @return Size long
     */
    public long size(){ return cachedData.size(); }

    /**
     * Returns the current status of the object
     *
     * -2 - insufficient memory error | -1 - general_error | 0 - unloaded | 1 - unloading | 2 - loading | 3 - loaded/ready </br>
     *
     * @return int int
     */
    public int getStatus(){ return status.get(); }

    /**
     * Returns is this object is ready to be used
     *
     * @return boolean boolean
     */
    public boolean isAdaptive(){ return adaptiveLoad.get(); }

    /**
     * Used to set if this cache is able to be adaptive loaded
     *
     * @param value new value
     */
    public void setAdaptiveLoading(boolean value){ adaptiveLoad.set(value); }

    /*                  DATA                    */

    /**
     * Used to get a specific object from the cache by its identifier
     *
     * @param identifier of the target data, converted to lowercase
     * @return CachedData cached data
     * @throws DataStorageException on various errors such as the object not being found, loading issues and other
     */
    public CachedData getCachedData(String identifier) throws DataStorageException{
        try{
            lock.readLock().lock();
            lastAccess.set(System.currentTimeMillis());
            identifier = identifier.toLowerCase();
            if(status.get() <= 0) { // -2, -1 or 0
                lock.readLock().unlock();
                // try to load
                int lastStatus = status.get();
                loadData();
                if(status.get() == 3){
                    // success, retry
                    return getCachedData(identifier);
                }else{
                    // something went wrong
                    throw new DataStorageException(101,"Cache: "+this.identifier+": Loading Data Failed - Cant GET Data When Cache Is Unloaded. Last Status: "+lastStatus+" New Status: "+status.get());
                }
            }else if(status.get() < 3) { // 1 or 2
                // this should not occur as we are using locks
                throw new DataStorageException(110, "Cache: "+this.identifier+": Data Has Not Finished Loading Yet", "This Error Should Not Be Thrown");
            }
            // should be always false
            if(status.get() != 3) {
                throw new DataStorageException(0, "Cache: "+this.identifier+": Something Went Wrong - Data Neither Seems To Be Loaded Nor Unloaded Nor In Between.", "This Error Should Not Be Thrown");
            }
            if(!cachedData.containsKey(identifier)){
                throw new DataStorageException(205, "Cache: "+this.identifier+": Data Not Found.");
            }
            return cachedData.get(identifier);
        }catch (DataStorageException e){
            throw e;
        }catch (Exception | Error e){
            logger.error("Cache: "+this.identifier+": An Unknown Error Occurred Getting Data From This Cache", e);
            throw new DataStorageException(0, "Cache: "+this.identifier+": An Unknown Error Occurred Deleting This Cache: "+e.getMessage());
        }finally {
            try{lock.readLock().unlock();}catch (Exception | Error ignore){} // this happens if the data had to be loaded (unlock before return to allow write lock for this thread)
        }
    }

    /**
     * Used to insert data to a cache
     *
     * Will throw an exception if the data already exists but will replace it when its no longer valid </br>
     *
     * @param data which should be inserted
     * @throws DataStorageException on various errors such as the object already existing, loading issues and other
     */
    public void insertCachedData(CachedData data) throws DataStorageException{
        try{
            lock.readLock().lock();
            lastAccess.set(System.currentTimeMillis());
            if(status.get() <= 0) { // -2, -1 or 0
                lock.readLock().unlock();
                // try to load
                int lastStatus = status.get();
                loadData();
                if(status.get() == 3){
                    // success, retry
                    insertCachedData(data);
                    return;
                }else{
                    // something went wrong
                    throw new DataStorageException(101,"Cache: "+this.identifier+": Loading Data Failed - Cant GET Data When Cache Is Unloaded. Last Status: "+lastStatus+" New Status: "+status.get());
                }
            }else if(status.get() < 3) { // 1 or 2
                // this should not occur as we are using locks
                throw new DataStorageException(110, "Cache: "+this.identifier+": Data Has Not Finished Loading Yet", "This Error Should Not Be Thrown");
            }
            // should be always false
            if(status.get() != 3) {
                throw new DataStorageException(0, "Cache: " + this.identifier + ": Something Went Wrong - Data Neither Seems To Be Loaded Nor Unloaded Nor In Between.", "This Error Should Not Be Thrown");
            }
            if(!this.identifier.equals(data.getCacheIdentifier())){
                throw new DataStorageException(220, "Cache: "+this.identifier+": Data For Cache "+data.getCacheIdentifier()+" Does Not Fit Here");
            }
            if(cachedData.containsKey(data.getIdentifier()) && cachedData.get(data.getIdentifier()).isValid()){
                throw new DataStorageException(242, "Cache: "+this.identifier+" Data "+data.getIdentifier()+" Is Still Valid");
            }
            cachedData.put(data.getIdentifier(), data);
        }catch (DataStorageException e){
            throw e;
        }catch (Exception | Error e){
            logger.error("Cache: "+this.identifier+": An Unknown Error Occurred Inserting Into This Cache", e);
            throw new DataStorageException(0, "Cache: "+this.identifier+": An Unknown Error Occurred Deleting This Cache: "+e.getMessage());
        }finally {
            try{lock.readLock().unlock();}catch (Exception | Error ignore){} // this happens if the data had to be loaded (unlock before return to allow write lock for this thread)
        }
    }

    /**
     * Used to remove data from the cache
     *
     * This works only for data which is no longer valid or which has no expiration set </br>
     * Using the read lock instead of the write lock performance is more important than data consistency here </br>
     *
     * @param identifier of the target cached data, converted to lowercase
     * @throws DataStorageException on various errors such as the object not being found, loading issues and other
     */
    public void deleteCachedData(String identifier) throws DataStorageException{
        try{
            lock.readLock().lock();
            lastAccess.set(System.currentTimeMillis());
            identifier = identifier.toLowerCase();
            if(status.get() <= 0) { // -2, -1 or 0
                lock.readLock().unlock();
                // try to load
                int lastStatus = status.get();
                loadData();
                if(status.get() == 3){
                    // success, retry
                    deleteCachedData(identifier);
                    return;
                }else{
                    // something went wrong
                    throw new DataStorageException(101,"Cache: "+this.identifier+": Loading Data Failed - Cant DELETE Data When Cache Is Unloaded. Last Status: "+lastStatus+" New Status: "+status.get());
                }
            }else if(status.get() < 3) { // 1 or 2
                // this should not occur as we are using locks
                throw new DataStorageException(110, "Cache: "+this.identifier+": Data Has Not Finished Loading Yet", "This Error Should Not Be Thrown");
            }
            // should be always true
            if(status.get() != 3) {
                throw new DataStorageException(0, "Cache: " +this.identifier+ ": Something Went Wrong - Data Neither Seems To Be Loaded Nor Unloaded Nor In Between.", "This Error Should Not Be Thrown");
            }
            if(!cachedData.containsKey(identifier)) {
                throw new DataStorageException(205, "Cache: "+this.identifier+": Data Not Found.");
            }
            if(!(!cachedData.get(identifier).isValid() || cachedData.get(identifier).isValidUntil() < 0)) {
                throw new DataStorageException(242, "Cache: "+this.identifier+": Data "+identifier+" Cant Be Removed.");
            }
            cachedData.remove(identifier);
        }catch (DataStorageException e){
            throw e;
        }catch (Exception | Error e){
            logger.error("Cache: "+this.identifier+": An Unknown Error Occurred Deleting This Cache", e);
            throw new DataStorageException(0, "Cache: "+this.identifier+": An Unknown Error Occurred Deleting This Cache: "+e.getMessage());
        }finally {
            try{lock.readLock().unlock();}catch (Exception | Error ignore){} // this happens if the data had to be loaded (unlock before return to allow write lock for this thread)
        }
    }

    /**
     * Used to check if the cache contains an object with the given identifier
     *
     * @param identifier of the object, converted to lowercase
     * @return boolean boolean
     */
    public boolean containsValidCachedData(String identifier){
        lock.readLock().lock();
        boolean hasValid = cachedData.containsKey(identifier.toLowerCase()) && cachedData.get(identifier.toLowerCase()).isValid();
        lock.readLock().unlock();
        return hasValid;
    }

    /*                  MISC                    */

    /**
     * Used to (re)load the content of this object from a file.
     *
     * @throws DataStorageException if data failed to load for any reasons. Data may be lost
     */
    public void loadData() throws DataStorageException{
        boolean lockedBefore = false;
        try{
            if (!lock.isWriteLockedByCurrentThread()) {
                lock.writeLock().lock();
            } else { lockedBefore = true; }
            if(status.get() <= 0) {
                status.set(2); // set loading
                logger.debug("Cache: "+this.identifier+": Loading Data");
                // check files
                File d = new File("./jstorage/data/cache/");
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/data/cache/"+this.identifier+"_cache");
                if(!f.exists()){ f.createNewFile();}
                else{
                    // check if file can be loaded to memory
                    if(((Runtime.getRuntime().freeMemory()/100)*80) < f.length()){
                        // file probably to large to load
                        status.set(-2); // error
                    }else{
                        BufferedReader br = new BufferedReader(new FileReader(f));
                        String line;
                        while((line = br.readLine()) != null) {
                            if (!line.isEmpty()) {
                                try{
                                    JSONObject jsonObject = new JSONObject(line);
                                    String cid = jsonObject.getString("cacheIdentifier").toLowerCase();
                                    String id = jsonObject.getString("identifier").toLowerCase();
                                    long validUntil = jsonObject.getLong("validUntil");
                                    JSONObject data = jsonObject.getJSONObject("data");
                                    if(this.identifier.equals(cid) && !cachedData.containsKey(cid)){
                                        CachedData cData = new CachedData(cid, id, data);
                                        if(validUntil > 0){
                                            cData.setValidForDuration(validUntil-System.currentTimeMillis());
                                        }
                                        if(cData.isValid()){
                                            cachedData.put(cData.getIdentifier(), cData);
                                        }
                                    }
                                }catch (Exception ignore){}
                            }
                        }
                        br.close();
                    }
                }
                // set loaded
                status.set(3);
                logger.debug("Cache: "+this.identifier+": Loading Data Finished: New Status: "+status.get());
            }
        }catch (Exception | Error e){
            status.set(-1);
            logger.debug("Cache: "+this.identifier+": Loading Data Finished: New Status: "+status.get());
            throw new DataStorageException(101,"Cache: "+this.identifier+": Loading Data Failed: "+e.getMessage());
        }finally {
            if(!lockedBefore){
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Async call loadData() from another thread.
     */
    public void loadDataAsync(){
        if(status.get() <= 0){
            Thread t = new Thread(() -> {
                try {
                    loadData();
                } catch (DataStorageException e) {
                    e.printStackTrace();
                }
            });
            t.setDaemon(true);
            t.start();
        }
    }

    /**
     * Used to unload the content of this object to a file.
     *
     * This function should be used to delete the cache and its CachedData {@link CachedData} child </br>
     *
     * @param unload      if the data should be removed from the object
     * @param saveToFile  if the data should be saved to a file
     * @param deleteTable if the data should be deleted
     * @throws DataStorageException if data failed to unload for any reasons. Data may be lost
     */
    public void unloadData(boolean unload, boolean saveToFile, boolean deleteTable) throws DataStorageException {
        boolean lockedBefore = false;
        try{
            if (!lock.isWriteLockedByCurrentThread()) {
                lock.writeLock().lock();
            } else { lockedBefore = true; }
            if(status.get() == 3) {
                status.set(1); // set unloading
                logger.debug("Cache: "+this.identifier+": Unloading Data: u="+unload+" s="+saveToFile+" d="+deleteTable);
                // check for delete - ignore others
                if(deleteTable){
                    // clear
                    cachedData.clear();
                    // remove file if exists
                    File f = new File("./jstorage/data/cache/"+this.identifier+"_cache");
                    f.delete();
                    status.set(0);
                }else if(saveToFile){
                    File d = new File("./jstorage/data/cache/");
                    if(!d.exists()){ d.mkdirs(); }
                    File f = new File("./jstorage/data/cache/"+this.identifier+"_cache");
                    BufferedWriter writer = new BufferedWriter(new FileWriter(f));
                    for(Map.Entry<String, CachedData> entry : cachedData.entrySet()){
                        if(entry.getKey().equals(entry.getValue().getIdentifier())){
                            writer.write(entry.getValue().export().toString());
                            writer.newLine();
                        }
                    }
                    writer.flush();
                    writer.close();
                    if(unload){
                        // clear content
                        cachedData.clear();
                    }
                }else{
                    if(unload){
                        // clear content
                        cachedData.clear();
                    }
                }
                if(!unload && !saveToFile && !deleteTable){
                    status.set(3); // set back as no changes have been made
                }else{
                    status.set(0);
                }
            }else if(status.get() <= 0 && deleteTable) {// cache can be deleted even if not loaded
                status.set(1); // set unloading
                logger.debug("Cache: "+this.identifier+": Unloading Data: u="+unload+" s="+saveToFile+" d="+deleteTable);
                // clear
                cachedData.clear();
                // remove file if exists
                File f = new File("./jstorage/data/cache/"+this.identifier+"_cache");
                f.delete();
                status.set(0);
            }
            logger.debug("Cache: "+this.identifier+": Unloading Data: u="+unload+" s="+saveToFile+" d="+deleteTable+" finished. New Status: "+status.get());
        }catch (Exception | Error e){
            status.set(-1);
            logger.debug("Cache: "+this.identifier+": Unloading Data: u="+unload+" s="+saveToFile+" d="+deleteTable+" finished. New Status: "+status.get());
            throw new DataStorageException(102,"Cache: "+this.identifier+": Unloading Data Failed, Data May Be Lost: "+e.getMessage());
        }finally {
            if(!lockedBefore){ // dont unlock is it has been locked before; the other part of the code might still be sensitive
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Async call unloadData() from another thread
     *
     * @param unload      if the data should be removed from the object
     * @param saveToFile  if the data should be saved to a file
     * @param deleteTable if the data should be deleted
     */
    public void unloadDataAsync(boolean unload, boolean saveToFile, boolean deleteTable){
        if(status.get() == 3){
            Thread t = new Thread(() -> {
                try {
                    unloadData(unload, saveToFile, deleteTable);
                } catch (DataStorageException e) {
                    e.printStackTrace();
                }
            });
            t.setDaemon(true);
            t.start();
        }
    }

    /*                  POOL                */

    /**
     * Returns the internal storage object for all CachedData objects.
     *
     * @return ConcurrentHashMap<String, CachedData> data pool
     */
    public ConcurrentHashMap<String, CachedData> getDataPool() {
        return cachedData;
    }

}
