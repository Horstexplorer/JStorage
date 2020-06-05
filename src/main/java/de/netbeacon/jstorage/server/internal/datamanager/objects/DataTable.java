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
import de.netbeacon.jstorage.server.tools.jsonmatcher.JSONMatcher;
import de.netbeacon.jstorage.server.tools.meta.UsageStatistics;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.stream.Collectors.toMap;

/**
 * This class represents an object within a database
 * <p>
 * This is used to to make data affiliation clearer
 *
 * @author horstexplorer
 */
public class DataTable {

    private final DataBase dataBase;
    private final String identifier;

    private final ConcurrentHashMap<String, String> indexPool = new ConcurrentHashMap<String, String>();
    private final ConcurrentHashMap<String, DataShard> shardPool = new ConcurrentHashMap<String, DataShard>();
    private final ConcurrentHashMap<String, UsageStatistics> statisticsPool = new ConcurrentHashMap<>();

    private JSONObject defaultStructure = new JSONObject();
    private final AtomicBoolean adaptiveLoad = new AtomicBoolean(false);
    private final AtomicBoolean autoOptimization = new AtomicBoolean(false);
    private final AtomicInteger autoResolveDataInconsistency = new AtomicInteger(-1);
    private final AtomicBoolean dataInconsistency = new AtomicBoolean(false);
    private final UsageStatistics usageStatistic = new UsageStatistics();

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ScheduledExecutorService sES = Executors.newScheduledThreadPool(1);
    private Future<?> sESUnloadTask;
    private Future<?> sESSnapshotTask;
    private Future<?> sESBackgroundTask;

    private final Logger logger = LoggerFactory.getLogger(DataTable.class);

    /**
     * Creates a new DataTable
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param dataBase  the superordinate DataBase {@link DataBase} object.
     * @param identifier the name of the this object.
     * @throws DataStorageException when running setup() fails.
     */
    public DataTable(DataBase dataBase, String identifier) throws DataStorageException{
        this.dataBase = dataBase;
        this.identifier = identifier.toLowerCase();
        setup();
        ready.set(true);

        logger.debug("Created New Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+")");
    }

    /*                  OBJECT                  */

    /**
     * Returns the name of the superordinate DataBase {@link DataBase} object.
     *
     * @return DataBase string
     */
    public DataBase getDataBase(){ return dataBase; }

    /**
     * Returns the name of the current table
     *
     * @return String string
     */
    public String getIdentifier(){ return identifier; }

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

    /**
     * Used to enable or disable auto optimization
     *
     * @param value value
     */
    public void setAutoOptimization(boolean value){
        autoOptimization.set(true);
    }

    /**
     * Returns if this object is optimizing itself
     *
     * @return boolean
     */
    public boolean autoOptimizationEnabled(){
        return autoOptimization.get();
    }

    /**
     * Used to set the mode for automatically repairing data inconsistencies
     * <p>
     * Set to -1 to disable; See {@link DataTable#resolveDataInconsistency(int)} for other modes
     * @param mode int
     */
    public void setAutoResolveDataInconsistency(int mode){
        autoResolveDataInconsistency.set( (-1 <= mode && mode < 4) ? mode : -1);
    }

    /**
     * Returns the mode this object uses to repair data inconsistencies on its own (each 24h)
     * <p>
     * Returns -1 if disabled
     *
     * @return int
     */
    public int autoResolveDataInconsistencyMode(){
        return autoResolveDataInconsistency.get();
    }

    /**
     * Sets the target default structure for all objects in this table
     * <p>
     * this does not influence already existing objects
     * will only set the jsonobject as structure if it matches the correct specifications for a dataset (only jsonObjects at layer 0; except id keys (they will be ignored))
     * @param jsonObject representing target and or default structure
     */
    public void setDefaultStructure(JSONObject jsonObject){
        // make sure this structure has the right dataset format
        for(String s : jsonObject.keySet()){
            if(!(s.equals("identifier") || s.equals("table") || s.equals("database"))){ // ignore default dataset keys
                if(jsonObject.get(s).getClass() != JSONObject.class){
                    return;
                }
            }
        }
        // has the right format, make sure all level 0 keys are lowercase
        JSONObject defaultSNorm = new JSONObject();
        for(String s: jsonObject.keySet()){
            if(!(s.equals("identifier") || s.equals("table") || s.equals("database"))){ // remove default dataset keys (as they are not needed and will be set for checking anyway)
                defaultSNorm.put(s.toLowerCase(), jsonObject.get(s));
            }
        }
        this.defaultStructure = defaultSNorm;
    }

    /**
     * Returns a serialized copy of the default structure
     *
     * @return JSONObject
     */
    public JSONObject getDefaultStructure(){
        return new JSONObject(defaultStructure.toString());
    }

    /**
     * Used to check whether a given dataset meets the requirements of the table
     *
     * @param dataSet DataSet
     * @return boolean
     */
    private boolean matchesDefaultStructure(DataSet dataSet){
        if(!defaultStructure.isEmpty()){
            return JSONMatcher.structureMatch(defaultStructure.put("database", "").put("table", "").put("identifier", ""), dataSet.getFullData());
        }
        return true;
    }

    /**
     * Used to determine if this table requires a specific structure;
     *
     * @return boolean
     */
    public boolean fixedStructure(){
        return !defaultStructure.isEmpty();
    }

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
        try{
            lock.readLock().lock();
            if(!ready.get()){
                logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Not Ready");
                throw new DataStorageException(231, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": Object Not Ready");
            }
            identifier = identifier.toLowerCase();
            if(!indexPool.containsKey(identifier)){
                logger.debug("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") DataSet "+identifier+" Not Found");
                throw new DataStorageException(201, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": DataSet "+identifier+" Not Found.");
            }
            // get the value > get the shard
            String shardID = indexPool.get(identifier);
            if(!shardPool.containsKey(shardID)){
                // shard not found but listed in index
                dataInconsistency.set(true);
                logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Shard "+shardID+" Not Found. Possible Data Inconsistency Detected");
                throw new DataStorageException(202, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": DataShard "+shardID+" Not Found But Listed In Index. Possible Data Inconsistency.", "Locking DataSet Insert Until Resolved.");
            }
            DataShard dataShard = shardPool.get(shardID);
            try{
                DataSet dataSet = dataShard.getDataSet(identifier);
                usageStatistic.add(UsageStatistics.Usage.get_success);
                return dataSet;
            }catch (DataStorageException e){
                switch(e.getType()){
                    case 201:
                        // data not found but listed in index
                        dataInconsistency.set(true);
                        logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") DataSet "+identifier+" Not Found But Indexed. Possible Data Inconsistency Detected");
                        throw new DataStorageException(201, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": DataSet "+identifier+" Not Found But Listed In Index. Possible Data Inconsistency.", "Locking DataSet Insert Until Resolved.");
                    default:
                        logger.debug("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") DataSet "+identifier+" Causes An Exception", e);
                        throw e;
                }
            }
        }catch (DataStorageException e){
            usageStatistic.add(UsageStatistics.Usage.get_failure);
            throw e;
        }catch (Exception | Error e){
            logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Unknown Error", e);
            usageStatistic.add(UsageStatistics.Usage.get_failure);
            throw new DataStorageException(0, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": Unknown Error: "+e.getMessage());
        }finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Used to insert a DataSet to the DataTable
     *
     * @param dataSet The DataSet which should be inserted
     * @throws DataStorageException on various errors such as an object already existing with the same identifier, loading issues and other
     */
    public void insertDataSet(DataSet dataSet) throws DataStorageException{
        try{
            lock.writeLock().lock();
            if(!ready.get()){
                logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Not Ready");
                throw new DataStorageException(231, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": Object Not Ready");
            }
            if(dataInconsistency.get()){
                throw new DataStorageException(300, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": Data Inconsistency Needs To Be Resolved Before Inserting New Objects.");
            }
            // check if the dataset fits to this
            if(!(dataBase.getIdentifier().equals(dataSet.getDataBase().getIdentifier()) && identifier.equals(dataSet.getTable().getIdentifier()))){
                logger.debug("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") DataSet "+dataSet.getIdentifier()+" Does Not Fit Here");
                throw new DataStorageException(220, "DataTable: "+dataBase.getIdentifier()+">"+identifier+">: DataSet "+dataSet.getIdentifier()+" ("+dataSet.getDataBase().getIdentifier()+">"+dataSet.getTable().getIdentifier()+") Does Not Fit Here.");
            }
            // check if we dont have an object with the current id
            if(indexPool.containsKey(dataSet.getIdentifier())){
                logger.debug("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") DataSet "+dataSet.getIdentifier()+" Already Existing");
                throw new DataStorageException(211, "DataShard: "+dataBase.getIdentifier()+">"+identifier+": DataSet "+dataSet.getIdentifier()+" Already Existing.");
            }
            // check if the object matches a specific structure
            if(fixedStructure() && !JSONMatcher.structureMatch(defaultStructure.put("identifier", "").put("table", "").put("database", ""), dataSet.getFullData())){
                logger.debug("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") DataSet "+dataSet.getIdentifier()+" Does Not Match Required Structure");
                throw new DataStorageException(221, "DataShard: "+dataBase.getIdentifier()+">"+identifier+": DataSet "+dataSet.getIdentifier()+" Does Not Match Required Structure");
            }
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
                DataShard dataShard = new DataShard(dataBase, this);
                shardPool.put(dataShard.getShardID(), dataShard);
                validShardID = dataShard.getShardID();
            }
            // we should have a shardID set here
            DataShard dataShard = shardPool.get(validShardID);
            // try to insert
            dataShard.insertDataSet(dataSet);
            // write to index
            indexPool.put(dataSet.getIdentifier(), dataShard.getShardID());
            // add statistics
            statisticsPool.put(dataSet.getIdentifier(), new UsageStatistics());
            usageStatistic.add(UsageStatistics.Usage.insert_success);
        }catch (DataStorageException e){
            usageStatistic.add(UsageStatistics.Usage.insert_failure);
            throw e;
        }catch (Exception | Error e){
            logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Unknown Error", e);
            usageStatistic.add(UsageStatistics.Usage.insert_failure);
            throw new DataStorageException(0, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": Unknown Error: "+e.getMessage());
        }finally {
            lock.writeLock().unlock();
        }
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
        try{
            lock.writeLock().lock();
            if(!ready.get()){
                logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Not Ready");
                throw new DataStorageException(231, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": Object Not Ready");
            }
            identifier = identifier.toLowerCase();
            if(!indexPool.containsKey(identifier)){
                logger.debug("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") DataSet "+identifier+" Not Found");
                throw new DataStorageException(201, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": DataSet "+identifier+" Not Found.");
            }
            // get dataShard
            String shardID = indexPool.get(identifier);
            if(!shardPool.containsKey(shardID)){
                // shard not found but listed in index
                dataInconsistency.set(true);
                logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Shard "+shardID+" Not Found. Possible Data Inconsistency Detected");
                throw new DataStorageException(202, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": DataShard "+shardID+" Not Found But Listed In Index. Possible Data Inconsistency.", "Locking DataSet Insert Until Resolved.");
            }
            DataShard dataShard = shardPool.get(shardID);
            // try to delete
            try{
                dataShard.deleteDataSet(identifier);
                // remove from index
                indexPool.remove(identifier);
                // remove statistics
                statisticsPool.remove(identifier);
                // check if shard is empty, then we just remove it
                if(dataShard.getCurrentDataSetCount() == 0){
                    dataShard.unloadData(false, false, false);
                    shardPool.remove(dataShard.getShardID());
                }
                usageStatistic.add(UsageStatistics.Usage.delete_success);
            }catch (DataStorageException e){
                switch(e.getType()){
                    case 201:
                        dataInconsistency.set(true);
                        logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") DataSet "+identifier+" Not Found But Indexed. Possible Data Inconsistency Detected");
                        throw new DataStorageException(201, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": DataSet "+identifier+" Not Found But Listed In Index. Possible Data Inconsistency.", "Locking DataSet Insert Until Resolved.");
                    default:
                        logger.debug("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") DataSet "+identifier+" Causes An Exception", e);
                        throw e;
                }
            }
        }catch (DataStorageException e){
            usageStatistic.add(UsageStatistics.Usage.delete_failure);
            throw e;
        }catch (Exception | Error e){
            logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Unknown Error", e);
            usageStatistic.add(UsageStatistics.Usage.delete_failure);
            throw new DataStorageException(0, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": Unknown Error: "+e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
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

    /*                  Statistics                   */

    /**
     * Returns the usage statistics for this object
     *
     * @return UsageStatistics
     */
    public UsageStatistics getStatistics(){
        return usageStatistic;
    }

    /**
     * Used to get the statistics for the selected dataset
     *
     * This function should only be used as loopback from datasets within this datatable
     * @param identifier of the dataset
     * @return DataSetMetaStatistics
     */
    public UsageStatistics getStatisticsFor(String identifier){
        if(statisticsPool.containsKey(identifier.toLowerCase())){
            return statisticsPool.get(identifier.toLowerCase());
        }
        return null;
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
                    logger.info("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Data Consistency Restored");
                    dataInconsistency.set(false);
                    break;
                case 1:
                    // remove entries from index with non existing shards only
                    indexPool.entrySet().stream().filter(e->!shardPool.containsKey(e.getValue())).forEach(e->indexPool.remove(e.getKey()));
                    logger.info("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Data Consistency Restored");
                    dataInconsistency.set(false);
                    break;
                case 2:
                    // remove entries from index with non existing shards & check if the shard contains the other objects
                    indexPool.entrySet().stream().filter(e->!shardPool.containsKey(e.getValue())).forEach(e->indexPool.remove(e.getKey()));
                    indexPool.entrySet().stream().filter(e->!shardPool.get(e.getValue()).containsDataSet(e.getKey())).forEach(e->indexPool.remove(e.getKey()));
                    logger.info("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Data Consistency Restored");
                    dataInconsistency.set(false);
                    break;
                case 3:
                    // rebuild index from loaded shards & all files inside the storage dir
                    HashMap<String, DataSet> dataSets = new HashMap<>();
                    // get all dataSets from all running dataShards
                    shardPool.entrySet().stream().filter(e->e.getValue().getStatus() == 3).forEach(e-> {e.getValue().getDataPool().forEach((key, value) -> dataSets.put(value.getIdentifier(),value));});
                    // get all datasets from existing files except those with ids in processedShardIDs
                    File d = new File("./jstorage/data/"+dataBase.getIdentifier());
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
                                                if(dataBase.getIdentifier().equals(gdb) && identifier.equals(ctable) && !dataSets.containsKey(identifier)){
                                                    dataSets.put(identifier, new DataSet(dataBase, this, identifier));
                                                }else{
                                                    logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Creating DataSet From File "+f.getName()+" Failed. Data Does Not Fit To This Table/Database Or Does Already Exist.");
                                                }
                                            }catch (Exception e){
                                                logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Creating DataSet From File "+f.getName()+" Failed. Data May Be Lost", e);
                                            }
                                        }
                                    }
                                    bufferedReader.close();
                                }catch (Exception e){
                                    // could not read
                                    logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Loading Data From File "+f.getName()+" Failed. Data May Be Lost", e);
                                }
                            }
                        }
                    }
                    try{
                        FileUtils.cleanDirectory(d);
                    }catch (Exception e){
                        logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Could Not Remove All Files. Manual Deletion Required", e);
                    }
                    // unload all shards by deletion & clear & delete index
                    indexPool.clear();
                    shardPool.forEach((key, value) -> {
                        value.getDataPool().clear(); // clear cuz we want to use the datasets later again
                        value.unloadDataAsync(false, false, true); // this would otherwise call unload on them
                    });
                    shardPool.clear();
                    new File("./jstorage/data/"+dataBase.getIdentifier()+"/"+identifier+"_index").delete();
                    // rebuild index & shards
                    List<DataSet> buffer = new ArrayList<>();
                    int processed = 0;
                    for(Map.Entry<String, DataSet> entry : dataSets.entrySet()){
                        processed++;
                        buffer.add(entry.getValue());
                        if(buffer.size() == DataShard.getMaxDataSetCountStatic() || processed == dataSets.size()) { // if we have enough objects to fill a shard or this is the last object we have
                            // create new Shard
                            DataShard dataShard = new DataShard(dataBase, this);
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
                                    logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") An Error Occurred While Rebuilding Index & Shards. DataSet Will Be Deleted", e);
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
                    logger.info("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Data Consistency Restored");
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

        public void fixDefault(){

        // future feature:
        //
        // used to normalize all datassets to match default preset

    }

     */ // fixDefault() -> placeholder for upcoming feature

    /**
     * Optimizes utilisation of shards by grouping frequently used data sets
     * <p>
     * ! Using this function may result in data loss. Manual correction recommended.
     */
    public void optimize(){
        // enable lock
        lock.writeLock().lock();
        try{
            logger.warn("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Optimizing Shards - This May Result In Data Loss");
            // create hashmap with <datasetkey, long>
            HashMap<String, Long> unsorted = new HashMap<>();
            statisticsPool.forEach((key, value) -> unsorted.put(key, value.getCountFor(UsageStatistics.Usage.any)));
            // sort
            HashMap<String, Long> sorted = unsorted.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
            // check if both the index size & sorted list match in size
            if(!(sorted.keySet().containsAll(Collections.list(indexPool.keys())) && sorted.size() == indexPool.size())){
                throw new Exception("Meta Data Does Not Match Objects From Index");
            }
            // load all datasets (this is scary)
            HashMap<String, DataSet> datasetTransferCache = new HashMap<>();
            for(String key : sorted.keySet()){ // we do not get the dataset pool from each shard as they may not be loaded
                // load dataset
                DataShard ds = shardPool.get(indexPool.get(key));
                if(ds == null){
                    logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Error Optimizing Shards - DataShard "+indexPool.get(key)+" Not Found For "+key+", Dropping DataSet");
                    continue;
                }
                DataSet dataSet = ds.getDataSet(key);
                if(dataSet == null){
                    logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Error Optimizing Shards - DataShard "+indexPool.get(key)+" Does Not Contain"+key+", Dropping DataSet");
                    continue;
                }
                datasetTransferCache.put(key, dataSet);
            }
            // clear all references
            logger.warn("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Optimizing Shards - Clearing All DataShards & Index");
            shardPool.forEach((k,v)->{
                try {
                    v.getDataPool().clear(); // we clear it here so that those objects dont know they get moved
                    v.unloadData(false, false, true); // as this would signal them that they got unloaded
                } catch (DataStorageException e) {
                    logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Optimizing Shards - Failed To Delete Shard "+v.getShardID());
                }
            });
            shardPool.clear();
            indexPool.clear();
            logger.warn("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Optimizing Shards - Rebuilding Index & Restoring DataSets");
            int processed = 0;
            int dsc = DataShard.getMaxDataSetCountStatic();
            DataShard dataShard = null;
            for(String selection : sorted.keySet()){
                processed++;
                if(dataShard == null || (processed % (dsc+1) == 0)){
                    dataShard = new DataShard(dataBase, this);
                    shardPool.put(dataShard.getShardID(), dataShard);
                }
                // get the dataset
                DataSet dataSet = datasetTransferCache.get(selection);
                if(dataSet == null){
                    logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Optimizing Shards - Failed To Get DataSet From Transfer Cache ");
                    continue;
                }
                // remove it from the transfer cache
                datasetTransferCache.remove(selection);
                // insert it into the new DataShard
                try{
                    dataShard.insertDataSet(dataSet);
                    // add to index
                    indexPool.put(dataSet.getIdentifier(), dataShard.getShardID());
                }catch (DataStorageException e){
                    logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Optimizing Shards - Failed To Insert DataSet Into New Shard, Dropping DataSet");
                }
            }
            // check if we have any datasets left over (we shouldn't as we checked before)
            if(datasetTransferCache.size() > 0){
                logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Optimizing Shards - Not All DataSet Have Been Inserted, Trying To Insert Them");
                for(Map.Entry<String, DataSet> entry : datasetTransferCache.entrySet()){
                    processed++;
                    if(dataShard == null || (processed % (dsc+1) == 0)){
                        dataShard = new DataShard(dataBase, this);
                        shardPool.put(dataShard.getShardID(), dataShard);
                    }
                    try{
                        dataShard.insertDataSet(entry.getValue());
                        indexPool.put(entry.getValue().getIdentifier(), dataShard.getShardID());
                    }catch (DataStorageException e){
                        logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Optimizing Shards - Failed To Insert DataSet Into New Shard, Dropping DataSet");
                    }
                }
            }
            // clean up
            datasetTransferCache.clear();
            unsorted.clear();
            sorted.clear();
            logger.info("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Optimizing Shards - Finished");
        }catch (DataStorageException e){
            logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Error Optimizing Shards", e);
        }catch (Exception e){
            logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Error Optimizing Shards", e);
        }
        // unlock
        lock.writeLock().unlock();
    }

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
            logger.debug("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Loading Data");
            try{
                // read from file
                File d = new File("./jstorage/data/db/"+dataBase.getIdentifier());
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/data/db/"+dataBase.getIdentifier()+"/"+identifier+"_index");
                if(!f.exists()){ f.createNewFile(); }
                else{
                    // read
                    String content = new String(Files.readAllBytes(f.toPath()));
                    if(!content.isEmpty()){
                        JSONObject jsonObject = new JSONObject(content);
                        String dbn = jsonObject.getString("database").toLowerCase();
                        String tbn = jsonObject.getString("table").toLowerCase();
                        defaultStructure = jsonObject.getJSONObject("defaultStructure");
                        adaptiveLoad.set(jsonObject.getBoolean("adaptiveLoad"));
                        autoOptimization.set(jsonObject.getBoolean("autoOptimize"));
                        int a = jsonObject.getInt("autoResolveDataInconsistency");
                        autoResolveDataInconsistency.set( (-1 <= a && a < 4) ? a : -1);
                        if(dataBase.getIdentifier().equals(dbn) && identifier.equals(tbn)){
                            JSONArray shards = jsonObject.getJSONArray("shards");
                            for(int i = 0; i < shards.length(); i++){
                                JSONObject shard = shards.getJSONObject(i);
                                String shardID = shard.getString("shardID");
                                JSONArray index = shard.getJSONArray("dataSets");
                                if(!index.isEmpty()){
                                    // create shard
                                    DataShard dataShard = new DataShard(dataBase, this, shardID);
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
                logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Loading Data Failed. Data May Be Lost", e);
                throw new DataStorageException(101, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": Loading Data Failed, Data May Be Lost: "+e.getMessage());
            }
            // start scheduled worker
            sESUnloadTask = sES.scheduleAtFixedRate(() -> {
                if(adaptiveLoad.get()){
                    shardPool.entrySet().stream().filter(e->(((e.getValue().getLastAccess()+900000) < System.currentTimeMillis()) && (e.getValue().getStatus() == 3))).forEach(e->e.getValue().unloadDataAsync(true, true, false));
                }
            }, 5, 5, TimeUnit.SECONDS);
            sESSnapshotTask = sES.scheduleAtFixedRate(() -> shardPool.entrySet().stream().filter(e->(((e.getValue().getLastAccess()+850000) > System.currentTimeMillis()) && (e.getValue().getStatus() == 3))).forEach(e->e.getValue().unloadDataAsync(false, true, false)), 30, 30, TimeUnit.MINUTES);
            sESBackgroundTask = sES.scheduleAtFixedRate(() -> {
                if(autoOptimization.get()){ optimize(); }
                if(autoResolveDataInconsistency.get() >= 0 && dataInconsistency.get()){ resolveDataInconsistency(autoResolveDataInconsistency.get()); }
            }, Duration.between(LocalDateTime.now(), LocalDateTime.now().plusDays(1).toLocalDate().atStartOfDay()).toMinutes(),24*60, TimeUnit.MINUTES);
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
            logger.debug("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Shutdown");
            // write index & shard data to file
            try{
                // build json object
                JSONObject jsonObject = new JSONObject()
                        .put("database", dataBase.getIdentifier())
                        .put("table", identifier)
                        .put("adaptiveLoad", adaptiveLoad.get())
                        .put("defaultStructure", defaultStructure)
                        .put("autoOptimize", autoOptimization.get())
                        .put("autoResolveDataInconsistency", autoResolveDataInconsistency.get());
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
                File d = new File("./jstorage/data/db/"+dataBase.getIdentifier());
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/data/db/"+dataBase.getIdentifier()+"/"+identifier+"_index");
                BufferedWriter writer = new BufferedWriter(new FileWriter(f));
                writer.write(jsonObject.toString());
                writer.newLine();
                writer.flush();
                writer.close();
                // shutdown & clear everything
                sESUnloadTask.cancel(true);
                sESSnapshotTask.cancel(true);
                sESBackgroundTask.cancel(true);
                sES.shutdown();
                shardPool.forEach((k, v)-> {
                    try{
                        v.unloadData(true, true, false);
                    } catch (DataStorageException ignored){}
                });
                shardPool.clear();
                indexPool.clear();
                statisticsPool.clear();
            }catch (Exception e){
                logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Shutdown Failed. Data May Be Lost", e);
                throw new DataStorageException(102, "DataTable: "+dataBase.getIdentifier()+">"+identifier+": Unloading Data Failed, Data May Be Lost: "+e.getMessage());
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
        statisticsPool.clear();
        // delete files
        try{
            File d = new File("./jstorage/data/db/"+dataBase.getIdentifier()+"/"+identifier);
            FileUtils.deleteDirectory(d);
            File f = new File("./jstorage/data/db/"+dataBase.getIdentifier()+"/"+identifier+"_index");
            if(f.exists()){ f.delete(); }
        }catch (Exception e){
            logger.error("Table ( Chain "+this.dataBase.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+") Deleting Files Failed. Manual Actions May Be Required.", e);
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

    /**
     * Returns the statistics of all datasets inside this table
     *
     * @return ConcurrentHashMap<String, UsageStatistics> concurrent hash map
     */
    public ConcurrentHashMap<String, UsageStatistics> getStatisticsPool() {
        return statisticsPool;
    }
}
