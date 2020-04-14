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
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class represents an object within a database
 * <p>
 * This is used to to make data affiliation clearer
 *
 * @author horstexplorer
 */
public class DataTable {

    private final String dataBaseName;
    private final String tableName;
    private final ConcurrentHashMap<String, String> indexPool = new ConcurrentHashMap<String, String>();
    private final ConcurrentHashMap<String, DataShard> shardPool = new ConcurrentHashMap<String, DataShard>();

    private final AtomicBoolean adaptiveLoad = new AtomicBoolean(false);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean dataInconsistency = new AtomicBoolean(false);
    private final ScheduledExecutorService sES = Executors.newScheduledThreadPool(1);
    private Future<?> sESUnloadTask;
    private Future<?> sESSnapshotTask;
    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final Logger logger = LoggerFactory.getLogger(DataTable.class);

    /**
     * Creates a new DataTable
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param dataBaseName the name of the superordinate DataBase {@link DataBase} object.
     * @param tableName    the name of the this object.
     * @throws DataStorageException when running setup() fails.
     */
    public DataTable(String dataBaseName, String tableName) throws DataStorageException{
        this.dataBaseName = dataBaseName.toLowerCase();
        this.tableName = tableName.toLowerCase();
        setup();
        ready.set(true);

        logger.debug("Created New Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+")");
    }

    /*                  OBJECT                  */

    /**
     * Returns the name of the superordinate DataBase {@link DataBase} object.
     *
     * @return String string
     */
    public String getDatabaseName(){ return dataBaseName; }

    /**
     * Returns the name of the current table
     *
     * @return String string
     */
    public String getTableName(){ return tableName; }

    /**
     * Returns if this object supports adaptive loading
     *
     * @return boolean boolean
     */
    public boolean isAdaptive(){ return adaptiveLoad.get(); }

    /**
     * Used to set if this table is able to be adaptive loaded
     *
     * @param value new value
     */
    public void setAdaptiveLoading(boolean value){
        adaptiveLoad.set(value);
    }

    /**
     * Returns is this object is ready to be used
     *
     * @return boolean boolean
     */
    public boolean isReady(){ return ready.get(); }

    /**
     * Returns is this object has been shut down
     *
     * @return boolean boolean
     */
    public boolean isShutdown(){ return shutdown.get(); }

    /*                  ACCESS                  */

    /**
     * Used to get a DataSet with the matching identifier from the DataTable
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier of the target DataSet
     * @return DataSet data set
     * @throws DataStorageException on various errors such as the object not being found, loading issues and other
     */
    public DataSet getDataSet(String identifier) throws DataStorageException{
        if(ready.get()){
            lock.readLock().lock();
            identifier = identifier.toLowerCase();
            try{
                if(indexPool.containsKey(identifier)){
                    // get the value > get the shard
                    String shardID = indexPool.get(identifier);
                    if(shardPool.containsKey(shardID)){
                        DataShard dataShard = shardPool.get(shardID);
                        try{
                            DataSet dataSet = dataShard.getDataSet(identifier);
                            lock.readLock().unlock();
                            return dataSet;
                        }catch (DataStorageException e){
                            switch(e.getType()){
                                case 201:
                                    // data not found but listed in index
                                    dataInconsistency.set(true);
                                    logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") DataSet "+identifier+" Not Found But Indexed. Possible Data Inconsistency Detected");
                                    throw new DataStorageException(201, "DataTable: "+dataBaseName+">"+tableName+": DataSet "+identifier+" Not Found But Listed In Index. Possible Data Inconsistency.", "Locking DataSet Insert Until Resolved.");
                                default:
                                    logger.debug("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") DataSet "+identifier+" Causes An Exception", e);
                                    throw e;
                            }
                        }
                    }
                    // shard not found but listed in index
                    dataInconsistency.set(true);
                    logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Shard "+shardID+" Not Found. Possible Data Inconsistency Detected");
                    throw new DataStorageException(202, "DataTable: "+dataBaseName+">"+tableName+": DataShard "+shardID+" Not Found But Listed In Index. Possible Data Inconsistency.", "Locking DataSet Insert Until Resolved.");
                }
                logger.debug("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") DataSet "+identifier+" Not Found");
                throw new DataStorageException(201, "DataTable: "+dataBaseName+">"+tableName+": DataSet "+identifier+" Not Found.");
            }catch (DataStorageException e){
                lock.readLock().unlock();
                throw e;
            }catch (Exception e){
                lock.readLock().unlock();
                logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Unknown Error", e);
                throw new DataStorageException(0, "DataTable: "+dataBaseName+">"+tableName+": Unknown Error: "+e.getMessage());
            }
        }
        logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Not Ready");
        throw new DataStorageException(231, "DataTable: "+dataBaseName+">"+tableName+": Object Not Ready");
    }

    /**
     * Used to insert a DataSet to the DataTable
     *
     * @param dataSet The DataSet which should be inserted
     * @throws DataStorageException on various errors such as an object already existing with the same identifier, loading issues and other
     */
    public void insertDataSet(DataSet dataSet) throws DataStorageException{
        if(ready.get()){
            lock.writeLock().lock();
            if(!dataInconsistency.get()){
                try{
                    // check if the dataset fits to this
                    if(dataBaseName.equals(dataSet.getDataBaseName()) && tableName.equals(dataSet.getTableName())){
                        // check if we dont have an object with the current id
                        if(!indexPool.containsKey(dataSet.getIdentifier())){
                            // try to put this object in some shard
                            String validShardID = null;
                            // check if we have a shard ready which is active & has enough space
                            Optional<Map.Entry<String, DataShard>> o = shardPool.entrySet().stream().filter(e->e.getValue().getStatus() == 3 && e.getValue().getCurrentDataSetCount()<e.getValue().getMaxDataSetCount()).findAny();
                            if(o.isPresent()){
                                validShardID = o.get().getValue().getShardID();
                            }
                            // if validShardID is still null, check if any would have space at all, ignoring shards loading/unloading
                            if(validShardID == null){
                                o = shardPool.entrySet().stream().filter(e->e.getValue().getStatus() <=0 && e.getValue().getCurrentDataSetCount()<e.getValue().getMaxDataSetCount()).findAny();
                                if(o.isPresent()){
                                    validShardID = o.get().getValue().getShardID();
                                }
                            }
                            // if there is no shard available, just create a new one
                            if(validShardID == null){
                                DataShard dataShard = new DataShard(dataBaseName, tableName);
                                shardPool.put(dataShard.getShardID(), dataShard);
                                validShardID = dataShard.getShardID();
                            }
                            // we should have a shardID set here
                            DataShard dataShard = shardPool.get(validShardID);
                            // try to insert
                            dataShard.insertDataSet(dataSet);
                            // write to index
                            indexPool.put(dataSet.getIdentifier(), dataShard.getShardID());
                            // unlock
                            lock.writeLock().unlock();
                            return;
                        }
                        logger.debug("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") DataSet "+dataSet.getIdentifier()+" Already Existing");
                        throw new DataStorageException(211, "DataShard: "+dataBaseName+">"+tableName+": DataSet "+dataSet.getIdentifier()+" Already Existing.");
                    }
                    logger.debug("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") DataSet "+dataSet.getIdentifier()+" Does Not Fit Here");
                    throw new DataStorageException(220, "DataTable: "+dataBaseName+">"+tableName+">: DataSet "+dataSet.getIdentifier()+" ("+dataSet.getDataBaseName()+">"+dataSet.getTableName()+") Does Not Fit Here.");
                }catch (DataStorageException e){
                    lock.writeLock().unlock();
                    throw e;
                }catch (Exception e){
                    lock.writeLock().unlock();
                    logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Unknown Error", e);
                    throw new DataStorageException(0, "DataTable: "+dataBaseName+">"+tableName+": Unknown Error: "+e.getMessage());
                }
            }
            lock.writeLock().unlock();
            throw new DataStorageException(300, "DataTable: "+dataBaseName+">"+tableName+": Data Inconsistency Needs To Be Resolved Before Inserting New Objects.");
        }
        logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Not Ready");
        throw new DataStorageException(231, "DataTable: "+dataBaseName+">"+tableName+": Object Not Ready");
    }

    /**
     * Used to delete a DataSet from the DataTable
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier of the target DataSet
     * @throws DataStorageException on various errors such as the object not being found, loading issues and other
     */
    public void deleteDataSet(String identifier) throws DataStorageException {
        if(ready.get()){
            lock.writeLock().lock();
            identifier = identifier.toLowerCase();
            try{
                if(indexPool.containsKey(identifier)){
                    // get dataShard
                    String shardID = indexPool.get(identifier);
                    if(shardPool.containsKey(shardID)){
                        DataShard dataShard = shardPool.get(shardID);
                        // try to delete
                        try{
                            dataShard.deleteDataSet(identifier);
                            // remove from index
                            indexPool.remove(identifier);
                            // check if shard is empty, then we just remove it
                            if(dataShard.getCurrentDataSetCount() == 0){
                                dataShard.unloadData(false, false, false);
                                shardPool.remove(dataShard.getShardID());
                            }
                            // unlock
                            lock.writeLock().unlock();
                            return;
                        }catch (DataStorageException e){
                            switch(e.getType()){
                                case 201:
                                    dataInconsistency.set(true);
                                    logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") DataSet "+identifier+" Not Found But Indexed. Possible Data Inconsistency Detected");
                                    throw new DataStorageException(201, "DataTable: "+dataBaseName+">"+tableName+": DataSet "+identifier+" Not Found But Listed In Index. Possible Data Inconsistency.", "Locking DataSet Insert Until Resolved.");
                                default:
                                    logger.debug("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") DataSet "+identifier+" Causes An Exception", e);
                                    throw e;
                            }
                        }
                    }
                    // shard not found but listed in index
                    dataInconsistency.set(true);
                    logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Shard "+shardID+" Not Found. Possible Data Inconsistency Detected");
                    throw new DataStorageException(202, "DataTable: "+dataBaseName+">"+tableName+": DataShard "+shardID+" Not Found But Listed In Index. Possible Data Inconsistency.", "Locking DataSet Insert Until Resolved.");
                }
                logger.debug("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") DataSet "+identifier+" Not Found");
                throw new DataStorageException(201, "DataTable: "+dataBaseName+">"+tableName+": DataSet "+identifier+" Not Found.");
            }catch (DataStorageException e){
                lock.writeLock().unlock();
                throw e;
            }catch (Exception e){
                lock.writeLock().unlock();
                logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Unknown Error", e);
                throw new DataStorageException(0, "DataTable: "+dataBaseName+">"+tableName+": Unknown Error: "+e.getMessage());
            }
        }
        logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Not Ready");
        throw new DataStorageException(231, "DataTable: "+dataBaseName+">"+tableName+": Object Not Ready");
    }

    /**
     * Used to check if the DataTable contains a specific DataSet
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier identifier of the DataSet. See {@link DataSet} for further information.
     * @return boolean boolean
     */
    public boolean containsDataSet(String identifier){
        lock.readLock().lock();
        identifier = identifier.toLowerCase();
        boolean contains = indexPool.containsKey(identifier);
        lock.readLock().unlock();
        return contains;
    }

    /*                  DATA_OPERATIONS                   */

    /**
     * Used to automatically resolve data inconsistency errors
     * <p>
     * ! Using this function may result in data loss. Manual correction recommended.
     * There are different modes to choose from:
     * 0 - do nothing but remove the lock
     * 1 - remove entries from index with non existing shards only
     * 2 - remove entries from index with non existing shards & check if the shard contains the other objects
     * 3 - rebuild index from loaded shards & all files inside the storage dir
     *
     * @param mode selection of the action
     */
    public void resolveDataInconsistency(int mode){
        if(ready.get()){
            lock.writeLock().lock();
            switch (mode){
                case 0:
                    // do nothing but remove lock
                    logger.info("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Data Consistency Restored");
                    dataInconsistency.set(false);
                    break;
                case 1:
                    // remove entries from index with non existing shards only
                    indexPool.entrySet().stream().filter(e->!shardPool.containsKey(e.getValue())).forEach(e->indexPool.remove(e.getKey()));
                    logger.info("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Data Consistency Restored");
                    dataInconsistency.set(false);
                    break;
                case 2:
                    // remove entries from index with non existing shards & check if the shard contains the other objects
                    indexPool.entrySet().stream().filter(e->!shardPool.containsKey(e.getValue())).forEach(e->indexPool.remove(e.getKey()));
                    indexPool.entrySet().stream().filter(e->!shardPool.get(e.getValue()).containsDataSet(e.getKey())).forEach(e->indexPool.remove(e.getKey()));
                    logger.info("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Data Consistency Restored");
                    dataInconsistency.set(false);
                    break;
                case 3:
                    // rebuild index from loaded shards & all files inside the storage dir
                    HashMap<String, DataSet> dataSets = new HashMap<>();
                    // get all dataSets from all running dataShards
                    shardPool.entrySet().stream().filter(e->e.getValue().getStatus() == 3).forEach(e-> {e.getValue().getDataPool().forEach((key, value) -> dataSets.put(value.getIdentifier(),value));});
                    // get all datasets from existing files except those with ids in processedShardIDs
                    File d = new File("./jstorage/data/"+dataBaseName);
                    if(!d.exists()){ d.mkdirs(); }
                    File[] files = d.listFiles();
                    if(files != null){
                        for(File f : files){
                            if(f.isFile()){
                                try{
                                    BufferedReader bufferedReader = new BufferedReader(new FileReader(f));
                                    String line;
                                    while((line = bufferedReader.readLine()) != null){
                                        if(!line.isEmpty()){
                                            // try building DataSets
                                            try{
                                                JSONObject jsonObject = new JSONObject(line);
                                                String gdb = jsonObject.getString("database").toLowerCase();
                                                String ctable = jsonObject.getString("table").toLowerCase();
                                                String identifier = jsonObject.getString("identifier").toLowerCase();
                                                if(dataBaseName.equals(gdb) && tableName.equals(ctable) && !dataSets.containsKey(identifier)){
                                                    dataSets.put(identifier, new DataSet(gdb, ctable, identifier));
                                                }else{
                                                    logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Creating DataSet From File "+f.getName()+" Failed. Data Does Not Fit To This Table/Database Or Does Already Exist.");
                                                }
                                            }catch (Exception e){
                                                logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Creating DataSet From File "+f.getName()+" Failed. Data May Be Lost", e);
                                            }
                                        }
                                    }
                                    bufferedReader.close();
                                }catch (Exception e){
                                    // could not read
                                    logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Loading Data From File "+f.getName()+" Failed. Data May Be Lost", e);
                                }
                            }
                        }
                    }
                    try{
                        FileUtils.cleanDirectory(d);
                    }catch (Exception e){
                        logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Could Not Remove All Files. Manual Deletion Required", e);
                    }
                    // unload all shards by deletion & clear & delete index
                    indexPool.clear();
                    shardPool.forEach((key, value) -> value.unloadDataAsync(false, false, true));
                    shardPool.clear();
                    new File("./jstorage/data/"+dataBaseName+"/"+tableName+"_index").delete();
                    // rebuild index & shards
                    List<DataSet> buffer = new ArrayList<>();
                    int processed = 0;
                    for(Map.Entry<String, DataSet> entry : dataSets.entrySet()){
                        processed++;
                        buffer.add(entry.getValue());
                        if(buffer.size() == DataShard.getMaxDataSetCountStatic() || processed == dataSets.size()) { // if we have enough objects to fill a shard or this is the last object we have
                            // create new Shard
                            DataShard dataShard = new DataShard(dataBaseName, tableName);
                            // get ID
                            String shardID = dataShard.getShardID();
                            // add to pool
                            shardPool.put(shardID, dataShard);
                            // add all objects
                            for(DataSet dataSet1 : buffer){
                                try{
                                    indexPool.put(dataSet1.getIdentifier(), shardID);
                                    dataShard.insertDataSet(dataSet1);
                                }catch (DataStorageException e){
                                    logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") An Error Occurred While Rebuilding Index & Shards. DataSet Will Be Deleted", e);
                                    indexPool.remove(dataSet1.getIdentifier());
                                }
                            }
                            // create async snapshot
                            dataShard.unloadDataAsync(false, true, false);
                            // clear the buffer
                            buffer.clear();
                        }
                    }
                    // clean up
                    dataSets.clear();
                    logger.info("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Data Consistency Restored");
                    dataInconsistency.set(false);
                    break;
                default:
                    // do nothing
                    break;
            }
            lock.writeLock().unlock();
        }
    }

    /*
    public void optimize(){

        // future feature:
        //
        // used to optimize adaptive loading by sorting often used data sets in the same shard
        // - required for this to work:
        //    something counting how often an object is used
        //    some magic

    }
    */ // optimize() -> placeholder for upcoming feature

    /*                    SETUP                   */

    /**
     * Used for initial setup of the DataTable object
     * <p>
     * Loads index from a file and rebuilds and initializes shards (if necessary)
     *
     * @throws DataStorageException on error
     */
    private void setup() throws DataStorageException {
        if(!ready.get() && !shutdown.get()){
            // build index & prepare shards
            logger.debug("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Loading Data");
            try{
                // read from file
                File d = new File("./jstorage/data/db/"+dataBaseName);
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/data/db/"+dataBaseName+"/"+tableName+"_index");
                if(!f.exists()){ f.createNewFile(); }
                else{
                    // read
                    String content = new String(Files.readAllBytes(f.toPath()));
                    if(!content.isEmpty()){
                        JSONObject jsonObject = new JSONObject(content);
                        String dbn = jsonObject.getString("database").toLowerCase();
                        String tbn = jsonObject.getString("table").toLowerCase();
                        adaptiveLoad.set(jsonObject.getBoolean("adaptiveLoad"));
                        if(dataBaseName.equals(dbn) && tableName.equals(tbn)){
                            JSONArray shards = jsonObject.getJSONArray("shards");
                            for(int i = 0; i < shards.length(); i++){
                                JSONObject shard = shards.getJSONObject(i);
                                String shardID = shard.getString("shardID");
                                JSONArray index = shard.getJSONArray("dataSets");
                                if(!index.isEmpty()){
                                    // create shard
                                    DataShard dataShard = new DataShard(dataBaseName, tableName, shardID);
                                    shardPool.put(dataShard.getShardID(), dataShard);
                                    // fill index
                                    for(int o = 0; o < index.length(); o++){
                                        indexPool.put(index.getString(o), dataShard.getShardID());
                                    }
                                }
                            }
                        }else{
                            throw new Exception("Index Content Does Not Match Expectations");
                        }
                    }
                }
            }catch (Exception e){
                logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Loading Data Failed. Data May Be Lost", e);
                throw new DataStorageException(101, "DataTable: "+dataBaseName+">"+tableName+": Loading Data Failed, Data May Be Lost: "+e.getMessage());
            }
            // start scheduled worker
            sESUnloadTask = sES.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    if(adaptiveLoad.get()){
                        shardPool.entrySet().stream().filter(e->(((e.getValue().getLastAccess()+900000) < System.currentTimeMillis()) && (e.getValue().getStatus() == 3))).forEach(e->e.getValue().unloadDataAsync(true, true, false));
                    }
                }
            }, 5, 5, TimeUnit.SECONDS);
            sESSnapshotTask = sES.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    shardPool.entrySet().stream().filter(e->(((e.getValue().getLastAccess()+850000) > System.currentTimeMillis()) && (e.getValue().getStatus() == 3))).forEach(e->e.getValue().unloadDataAsync(false, true, false));
                }
            }, 30, 30, TimeUnit.MINUTES);
            // initialize content if necessary
            if(!adaptiveLoad.get()){
                for(Map.Entry<String, DataShard> entry : shardPool.entrySet()){
                    entry.getValue().loadData();
                }
            }
        }
    }

    /**
     * Used to store content on shutdown
     * <p>
     * This will instruct all loaded shards inside the table to write their information to a file, export its own index and then clear everything.
     *
     * @throws DataStorageException on any error. This may result in data getting lost.
     */
    protected void shutdown() throws DataStorageException{
        if(ready.get()){
            shutdown.set(true);
            ready.set(false);
            logger.debug("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Shutdown");
            // write index & shard data to file
            try{
                // build json object
                JSONObject jsonObject = new JSONObject()
                        .put("database", dataBaseName)
                        .put("table", tableName)
                        .put("adaptiveLoad", adaptiveLoad.get());
                JSONArray shards = new JSONArray();
                shardPool.forEach((key, value) -> {
                    JSONObject shard = new JSONObject()
                            .put("shardID", value.getShardID());
                    JSONArray data = new JSONArray();
                    indexPool.entrySet().stream().filter(e -> value.getShardID().equals(e.getValue())).forEach(e->data.put(e.getKey()));
                    shard.put("dataSets", data);
                    if(!data.isEmpty()){ // we dont need to keep track of empty shards
                        shards.put(shard);
                    }
                });
                jsonObject.put("shards", shards);
                // write to file
                File d = new File("./jstorage/data/db/"+dataBaseName);
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/data/db/"+dataBaseName+"/"+tableName+"_index");
                BufferedWriter writer = new BufferedWriter(new FileWriter(f));
                writer.write(jsonObject.toString());
                writer.newLine();
                writer.flush();
                writer.close();
                // shutdown & clear everything
                sESUnloadTask.cancel(true);
                sESSnapshotTask.cancel(true);
                sES.shutdown();
                shardPool.forEach((k, v)-> {
                    try{
                        v.unloadData(true, true, false);
                    } catch (DataStorageException ignored){}
                });
                shardPool.clear();
                indexPool.clear();
            }catch (Exception e){
                logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Shutdown Failed. Data May Be Lost", e);
                throw new DataStorageException(102, "DataTable: "+dataBaseName+">"+tableName+": Unloading Data Failed, Data May Be Lost: "+e.getMessage());
            }
            // dont reset shutdown atomic. this object should not be used further
        }
    }

    /**
     * Used to safely delete this object and its content
     * <p>
     * This will make this object unusable
     */
    protected void delete(){
        shutdown.set(true);
        ready.set(false);
        // shutdown & clear everything
        sESUnloadTask.cancel(true);
        sESSnapshotTask.cancel(true);
        sES.shutdown();
        shardPool.forEach((k, v)-> {
            try{
                v.unloadData(false, false, true);
            } catch (DataStorageException e){}
        });
        shardPool.clear();
        indexPool.clear();
        // delete files
        try{
            File d = new File("./jstorage/data/db/"+dataBaseName+"/"+tableName);
            FileUtils.deleteDirectory(d);
            File f = new File("./jstorage/data/db/"+dataBaseName+"/"+tableName+"_index");
            if(f.exists()){ f.delete(); }
        }catch (Exception e){
            logger.error("Table ( Chain "+this.dataBaseName+", "+this.tableName+"; Hash "+hashCode()+") Deleting Files Failed. Manual Actions May Be Required.", e);
        }
        // dont reset shutdown atomic. this object should not be used further
    }

    /*              POOL              */

    /**
     * Returns the internal storage object for all DataShard objects.
     *
     * @return ConcurrentHashMap<String, DataShard> data pool
     */
    public ConcurrentHashMap<String, DataShard> getDataPool() {
        return shardPool;
    }

    /**
     * Returns the internal index
     *
     * @return ConcurrentHashMap<String, String> concurrent hash map
     */
    public ConcurrentHashMap<String, String> getIndexPool(){
        return indexPool;
    }

}
