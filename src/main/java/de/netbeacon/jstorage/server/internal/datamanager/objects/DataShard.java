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

package de.netbeacon.jstorage.server.internal.datamanager.objects;

import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.SetupException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class is used to break down the whole data of a DataTable into multiple smaller objects & files
 * <p>
 * This tries to load only required object groups to reduce the memory consumption
 * Used for internal data management only. User should not have direct interactions with this class
 *
 * @author horstexplorer
 */
public class DataShard {

    private final DataBase dataBase;
    private final DataTable table;
    private String shardID;

    private final ConcurrentHashMap<String, DataSet> dataSetPool = new ConcurrentHashMap<String, DataSet>();
    private static int maxDataSets = 10000; // maximum number of DataSets within one shard
    private final AtomicLong lastAccess = new AtomicLong();
    // status
    private final AtomicInteger status = new AtomicInteger(0); // -2 - insufficient memory error | -1 - general_error | 0 - unloaded | 1 - unloading | 2 - loading | 3 - loaded/ready
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final static HashSet<String> occupiedIDs = new HashSet<>();

    private final Logger logger = LoggerFactory.getLogger(DataShard.class);


    /**
     * Creates new DataShard
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param dataBase the superordinate DataBase {@link DataBase} object
     * @param table    the parent DataTable {@link DataTable} object
     */
    protected DataShard(DataBase dataBase, DataTable table){
        this.shardID = String.valueOf(Math.abs(new SecureRandom().nextLong()));
        while (occupiedIDs.contains(this.shardID)){
            this.shardID = String.valueOf(Math.abs(new SecureRandom().nextLong()));
        }
        occupiedIDs.add(this.shardID);
        this.dataBase = dataBase;
        this.table = table;
        this.lastAccess.set(System.currentTimeMillis());

        logger.debug("Created New Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" )");
    }

    /**
     * Creates new DataShard
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param dataBase     the superordinate DataBase {@link DataBase} object
     * @param table    the parent DataTable {@link DataTable} object
     * @param shardID      target id of this shard
     * @throws SetupException when shardID is already in use
     */
    protected DataShard(DataBase dataBase, DataTable table, String shardID) throws SetupException {
        if(occupiedIDs.contains(shardID)){
            logger.error("Failed To Create New Shard, ID Already In Use");
            throw new SetupException("ShardID Already In Use");
        }
        this.shardID = shardID;
        occupiedIDs.add(this.shardID);
        this.dataBase = dataBase;
        this.table = table;
        this.lastAccess.set(System.currentTimeMillis());

        logger.debug("Created New Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" )");
    }

    /*                  STATIC                  */

    /**
     * Sets the maximum number of DataSets a DataShard should contain
     * <p>
     * Values smaller than 0 will set it to -1 or infinite, equal 0 to the default (10000) and all values above 0 to their respective value
     *
     * @param value maximum number of DataSets per shard
     */
    public static void setMaxDataSets(int value){
        if(value < 0){
            maxDataSets = -1; // set to inf
        }else if(value > 0){
            maxDataSets = value;
        }else{ // value = 0 -> set to default
            maxDataSets = 10000;
        }
    }

    /**
     * Returns the current setting for the maximum number of DataSets
     *
     * @return int int
     */
    public static int getMaxDataSetCountStatic(){
        return maxDataSets;
    }

    /*                  OBJECT                  */

    /**
     * Returns the number of DataSets currently stored in this shard
     *
     * @return int number of DataSets
     */
    protected int getCurrentDataSetCount(){
        return dataSetPool.size();
    }

    /**
     * Returns the current value for the maximum number of DataSets
     * Returns current+1 if value is set to infinite so there is always 1 slot empty
     *
     * @return int int
     */
    protected int getMaxDataSetCount(){
        if(maxDataSets == -1){ // could be used to easily deactivate sharding the data
            return dataSetPool.size()+1;
        }
        return maxDataSets;
    }

    /**
     * Returns the current id of the shard
     *
     * @return String shardID
     */
    protected String getShardID() {
        return shardID;
    }

    /*              ACCESS              */

    /**
     * Used to get a DataSet with the matching identifier from the shard
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier of the target DataSet
     * @return DataSet {@link DataSet}
     * @throws DataStorageException on various errors such as the object not being found, loading issues and other
     */
    protected DataSet getDataSet(String identifier) throws DataStorageException {
        lock.readLock().lock();
        lastAccess.set(System.currentTimeMillis());
        identifier = identifier.toLowerCase();
        if(status.get() <= 0) { // -2, -1 or 0
            lock.readLock().unlock();
            int lastStatus = status.get();
            loadData();
            if(status.get() == 3){
                // success, retry
                return getDataSet(identifier);
            }else{
                // something went wrong
                logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Loading Data Failed. Cant Get DataSet When Shard Is Unloaded. Last: "+lastStatus+" New: "+status.get());
                throw new DataStorageException(101,"DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Loading Data Failed - Cant Get DataSet When Shard Is Unloaded. Last Status: "+lastStatus+" New Status: "+status.get());
            }
        }else if(status.get() < 3){ // 1 or 2
            // this should not occur as we are using locks
            lock.readLock().unlock();
            logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Data Has Not Finished Loading Yet. Something Major Broke");
            throw new DataStorageException(110, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Data Has Not Finished Loading Yet", "This Error Should Not Be Thrown");
        }
        // should be always true
        if(status.get() == 3){
            // the data is loaded
            if(dataSetPool.containsKey(identifier)){
                // get
                DataSet dataSet = dataSetPool.get(identifier);
                lock.readLock().unlock();
                return dataSet;
            }
            lock.readLock().unlock();
            logger.debug("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) DataSet "+identifier+" Not Found");
            throw new DataStorageException(201, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": DataSet "+identifier+" Not Found.");
        }
        lock.readLock().unlock();
        logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Something Went Wrong - Data Neither Seems To Be Loaded Nor Unloaded Nor In Between - Something Major Broke");
        throw new DataStorageException(0, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Something Went Wrong - Data Neither Seems To Be Loaded Nor Unloaded Nor In Between.", "This Error Should Not Be Thrown");
    }

    /**
     * Used to insert a DataSet to the shard
     *
     * @param dataSet The DataSet which should be inserted
     * @throws DataStorageException on various errors such as an object already existing with the same identifier, loading issues and other
     */
    protected void insertDataSet(DataSet dataSet) throws DataStorageException{
        lock.writeLock().lock();
        lastAccess.set(System.currentTimeMillis());
        if(status.get() <= 0) { // -2, -1 or 0
            lock.writeLock().unlock();
            int lastStatus = status.get();
            loadData();
            if(status.get() == 3){
                // success, retry
                insertDataSet(dataSet);
                return;
            }else{
                // something went wrong
                logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Loading Data Failed. Cant Insert DataSet When Shard Is Unloaded. Last: "+lastStatus+" New: "+status.get());
                throw new DataStorageException(101,"DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Loading Data Failed - Cant Insert DataSet When Shard Is Unloaded.");
            }
        }else if(status.get() < 3){ // 1 or 2
            // this should not occur as we are using locks
            lock.writeLock().unlock();
            logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Data Has Not Finished Loading Yet. Something Major Broke");
            throw new DataStorageException(110, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Data Has Not Finished Loading Yet", "This Error Should Not Be Thrown");
        }
        // should be always true
        if(status.get() == 3){
            // the data is loaded
            if(dataSetPool.size() < getMaxDataSetCount()){
                if(dataBase.getIdentifier().equals(dataSet.getDataBase().getIdentifier()) && table.getIdentifier().equals(dataSet.getTable().getIdentifier())){
                    if(!dataSetPool.containsKey(dataSet.getIdentifier())){
                        // insert
                        dataSetPool.put(dataSet.getIdentifier(), dataSet);
                        lock.writeLock().unlock();
                        return;
                    }
                    lock.writeLock().unlock();
                    logger.debug("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) DataSet "+dataSet.getIdentifier()+" Already Existing");
                    throw new DataStorageException(211, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": DataSet "+dataSet.getIdentifier()+" Already Existing.");
                }
                lock.writeLock().unlock();
                logger.debug("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) DataSet "+dataSet.getIdentifier()+" Does Not Fit Here");
                throw new DataStorageException(220, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": DataSet "+dataSet.getIdentifier()+" ("+dataSet.getDataBase().getIdentifier()+">"+dataSet.getTable().getIdentifier()+") Does Not Fit Here.");
            }
            lock.writeLock().unlock();
            logger.debug("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) DataSet Could Not Be Inserted, Shard Is Full");
            throw new DataStorageException(220, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Shard Is Full");
        }
        lock.writeLock().unlock();
        logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Something Went Wrong - Data Neither Seems To Be Loaded Nor Unloaded Nor In Between - Something Major Broke");
        throw new DataStorageException(0, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Something Went Wrong - Data Neither Seems To Be Loaded Nor Unloaded Nor In Between.", "This Error Should Not Be Thrown");

    }

    /**
     * Used to delete a DataSet from the shard
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier of the target DataSet
     * @throws DataStorageException on various errors such as the object not being found, loading issues and other
     */
    protected void deleteDataSet(String identifier) throws DataStorageException {
        lock.writeLock().lock();
        lastAccess.set(System.currentTimeMillis());
        identifier = identifier.toLowerCase();
        // check status
        if(status.get() <= 0) { // -2, -1 or 0
            lock.writeLock().unlock();
            int lastStatus = status.get();
            loadData();
            if(status.get() == 3){
                // success, retry
                deleteDataSet(identifier);
                return;
            }else{
                // something went wrong
                logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Loading Data Failed. Cant Delete DataSet When Shard Is Unloaded. Last: "+lastStatus+" New: "+status.get());
                throw new DataStorageException(101,"DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Loading Data Failed - Cant Delete DataSet When Shard Is Unloaded.");
            }
        }else if(status.get() < 3){ // 1 or 2
            // this should not occur as we are using locks
            lock.writeLock().unlock();
            logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Data Has Not Finished Loading Yet. Something Major Broke");
            throw new DataStorageException(110, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Data Has Not Finished Loading Yet", "This Error Should Not Be Thrown");
        }
        // should be always true
        if(status.get() == 3){
            // the data is loaded
            if(dataSetPool.containsKey(identifier)){
                // get & remove
                DataSet dataSet = dataSetPool.remove(identifier);
                dataSet.onUnload();
                lock.writeLock().unlock();
                return;
            }
            lock.writeLock().unlock();
            logger.debug("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) DataSet "+identifier+" Not Found");
            throw new DataStorageException(201, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": DataSet "+identifier+" Not Found.");
        }
        lock.writeLock().unlock();
        logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Something Went Wrong - Data Neither Seems To Be Loaded Nor Unloaded Nor In Between - Something Major Broke");
        throw new DataStorageException(0, "DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Something Went Wrong - Data Neither Seems To Be Loaded Nor Unloaded Nor In Between.", "This Error Should Not Be Thrown");
    }

    /**
     * Used to check if the shard contains a specific DataSet
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier identifier of the DataSet. See {@link DataSet} for further information.
     * @return boolean boolean
     */
    protected boolean containsDataSet(String identifier){
        lock.readLock().lock();
        identifier = identifier.toLowerCase();
        boolean contains = dataSetPool.containsKey(identifier);
        lock.readLock().unlock();
        return contains;
    }

    /*              STATUS              */

    /**
     * Returns the current status of this object
     * <p>
     * 3 - ready
     * 2 - loading
     * 1 - unloading
     * 0 - unloaded
     * -1 - general error
     * -2 - out of memory interception
     *
     * @return int current status
     */
    protected int getStatus(){ return status.get(); }

    /**
     * Returns the time of the last read or write access to this object
     *
     * @return long long
     */
    protected long getLastAccess(){ return lastAccess.get(); }

    /*              LOAD/UNLOAD              */

    /**
     * Used to (re)load the content of this object from a file.
     *
     * @throws DataStorageException if data failed to load for any reasons. Data may be lost
     */
    protected void loadData() throws DataStorageException {
        try{
            lock.writeLock().lock();
            if(status.get() <= 0){
                status.set(2); // set loading
                logger.debug("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Loading Data");
                // check files
                File d = new File("./jstorage/data/db/"+dataBase.getIdentifier()+"/"+table.getIdentifier());
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/data/db/"+dataBase.getIdentifier()+"/"+table.getIdentifier()+"/"+table.getIdentifier()+"_"+shardID);
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
                                    // parse important values
                                    String gdb = jsonObject.getString("database").toLowerCase();
                                    String ctable = jsonObject.getString("table").toLowerCase();
                                    String identifier = jsonObject.getString("identifier").toLowerCase();
                                    if(gdb.equals(dataBase.getIdentifier()) && ctable.equals(table.getIdentifier()) && !dataSetPool.containsKey(identifier)){
                                        dataSetPool.put(identifier, new DataSet(dataBase, table, identifier));
                                    }
                                    this.lastAccess.set(System.currentTimeMillis()); // update for each so it wont get unloaded
                                }catch (Exception ignore){}
                            }
                        }
                    }
                }
                // set loaded
                status.set(3);
                logger.debug("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Loaded Data. New Status: "+status.get());
            }
        }catch (Exception e){
            status.set(-1);
            logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Loaded Data. New Status: "+status.get());
            lock.writeLock().unlock();
            throw new DataStorageException(101,"DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Loading Data Failed: "+e.getMessage());
        }finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Async call loadData() from another thread.
     */
    protected void loadDataAsync(){
        if(status.get() <= 0){
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        loadData();
                    } catch (DataStorageException e) {
                        e.printStackTrace();
                    }
                }
            });
            t.setDaemon(true);
            t.start();
        }
    }

    /**
     * Used to unload the content of this object to a file.
     * <p>
     * This function should be used to delete the shard and its child DataSets {@link DataSet} when their parent {@link DataTable} is getting deleted.
     *
     * @param unload      if the data should be removed from the object
     * @param saveToFile  if the data should be saved to a file
     * @param deleteTable if the data should be deleted
     * @throws DataStorageException if data failed to unload for any reasons. Data may be lost
     */
    protected void unloadData(boolean unload, boolean saveToFile, boolean deleteTable) throws DataStorageException {
        try{
            lock.writeLock().lock();
            if(status.get() == 3){
                status.set(1); // set unloading
                logger.debug("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Unloading Data With Params: u="+unload+" s="+saveToFile+" d="+deleteTable);
                // check for delete - ignore others
                if(deleteTable){
                    // clear content
                    dataSetPool.forEach((key, value) -> {value.onUnload();});
                    dataSetPool.clear();
                    // remove file if exists
                    File f = new File("./jstorage/data/db/"+dataBase.getIdentifier()+"/"+table.getIdentifier()+"/"+dataBase.getIdentifier()+"_"+shardID);
                    f.delete();
                    status.set(0);
                }else if(saveToFile){
                    File d = new File("./jstorage/data/db/"+dataBase.getIdentifier()+"/"+table.getIdentifier());
                    if(!d.exists()){ d.mkdirs(); }
                    File f = new File("./jstorage/data/db/"+dataBase.getIdentifier()+"/"+table.getIdentifier()+"/"+table.getIdentifier()+"_"+shardID);
                    BufferedWriter writer = new BufferedWriter(new FileWriter(f));
                    for(Map.Entry<String, DataSet> entry : dataSetPool.entrySet()){
                        if(entry.getKey().equals(entry.getValue().getIdentifier())){
                            writer.write(entry.getValue().getFullData().toString());
                            writer.newLine();
                        }
                    }
                    writer.flush();
                    writer.close();
                    if(unload){
                        // clear content
                        dataSetPool.forEach((key, value) -> {value.onUnload();});
                        dataSetPool.clear();
                    }
                }else{
                    if(unload){
                        // clear content
                        dataSetPool.forEach((key, value) -> {value.onUnload();});
                        dataSetPool.clear();
                    }
                }
                if(!unload && !saveToFile && !deleteTable){
                    status.set(3); // set back as no changes have been made
                }else{
                    status.set(0);
                }
            }else if(status.get() <= 0 && deleteTable){// table can be deleted even if not loaded
                status.set(1); // set unloading
                logger.debug("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Unloading Data With Params: u="+unload+" s="+saveToFile+" d="+deleteTable);
                // clear content
                dataSetPool.forEach((key, value) -> {value.onUnload();});
                dataSetPool.clear();
                // remove file if exists
                File f = new File("./jstorage/data/db/"+dataBase.getIdentifier()+"/"+table.getIdentifier()+"/"+dataBase.getIdentifier()+"_"+shardID);
                f.delete();
                status.set(0);
            }
            logger.debug("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Unloaded Data. New Status: "+status.get());
        }catch (Exception e){
            status.set(-1);
            logger.error("Shard ( Chain "+this.dataBase.getIdentifier()+", "+this.table.getIdentifier()+"#"+this.shardID+"; Hash "+hashCode()+" ) Unloaded Data. New Status: "+status.get());
            lock.writeLock().unlock();
            throw new DataStorageException(102,"DataShard: "+dataBase.getIdentifier()+">"+table.getIdentifier()+">"+shardID+": Unloading Data Failed, Data May Be Lost: "+e.getMessage());
        }finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Async call unloadData() from another thread
     *
     * @param unload      if the data should be removed from the object
     * @param saveToFile  if the data should be saved to a file
     * @param deleteTable if the data should be deleted
     */
    protected void unloadDataAsync(boolean unload, boolean saveToFile, boolean deleteTable){
        if(status.get() == 3){
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        unloadData(unload, saveToFile, deleteTable);
                    } catch (DataStorageException e) {
                        e.printStackTrace();
                    }
                }
            });
            t.setDaemon(true);
            t.start();
        }
    }

    /*              POOL              */

    /**
     * Returns the internal storage object for all DataSet objects.
     *
     * @return ConcurrentHashMap<String, DataSet> data pool
     */
    public ConcurrentHashMap<String, DataSet> getDataPool() {
        return dataSetPool;
    }

}
