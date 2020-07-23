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
import de.netbeacon.jstorage.server.tools.jsonmatcher.JSONMatcher;
import de.netbeacon.jstorage.server.tools.meta.UsageStatistics;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Instances of this class can be used to store data in JSON format. It also provides a save environment to perform changes to the data
 * <p>
 * Information on data management:
 * data has always to be stored in this format
 * { "database":STRING, "table":STRING, "identifier":STRING, DATATYPE:{ DATA }, DATATYPE:{ ... }, ... }
 * get() returns
 * { "database":STRING, "table":STRING, "identifier":STRING, "uToken":STRING, DATATYPE:{ DATA } } // may or may not contain uToken - depending on result of acquire
 * insert() requires
 * dataType not already inserted
 * dataType, { "database":STRING, "table":STRING, "identifier":STRING, NEWDATATYPE:{ NEWDATA } }
 * update() requires
 * dataType already existing
 * dataType, { "database":STRING, "table":STRING, "identifier":STRING, "uToken":STRING, DATATYPE:{ NEWDATA } }
 *
 * @author horstexplorer
 */
public class DataSet{

    // data
    private final String identifier;
    private final DataTable table;
    private final DataBase database;
    private final JSONObject data;
    // access management
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantLock updatePermissionLock = new ReentrantLock();
    private final ConcurrentHashMap<String, DataUpdateObject> updatePermissions = new ConcurrentHashMap<>();
    private final static ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
    private final static ScheduledExecutorService updatePMSES = scheduledThreadPoolExecutor;
    private final static AtomicInteger dataSetsPerThread = new AtomicInteger(7500);
    private final static AtomicLong dataSets = new AtomicLong(0);
    private final static AtomicInteger maxSTPEThreads = new AtomicInteger(256);
    // statistics
    private final Consumer<UsageStatistics.Usage> statistics = new Consumer<UsageStatistics.Usage>() {
        @Override
        public void accept(UsageStatistics.Usage usage) {
            UsageStatistics dsms = table.getStatisticsFor(identifier);
            if(dsms != null){
                dsms.add(usage);
            }else{
                logger.debug("DataSet ( Chain "+database.getIdentifier()+", "+table.getIdentifier()+", "+identifier+"; Hash "+hashCode()+" ) - Access To Table Statistics Failed. This Object Might Not Be Part Of The Table Yet / Anymore");
            }
        }
    };
    private final Logger logger = LoggerFactory.getLogger(DataSet.class);

    /**
     * Creates a new DataSet
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param database   the superordinate DataBase {@link DataBase} object
     * @param table      the parent DataTable {@link DataTable} object
     * @param identifier the identifier of the current DataSet, this may be unique inside each DataTable
     */
    public DataSet(DataBase database, DataTable table, String identifier) {
        // actual object
        this.identifier = identifier.toLowerCase();
        this.table = table;
        this.database = database;
        this.data = new JSONObject().put("database", this.database.getIdentifier()).put("table", this.table.getIdentifier()).put("identifier", this.identifier);

        // update scheduledThreadPoolExecutor
        dataSets.getAndIncrement();
        int i = (int)(Math.min(Math.max((dataSets.get()/dataSetsPerThread.get()),1),maxSTPEThreads.get()));
        if(i != scheduledThreadPoolExecutor.getCorePoolSize()){
            scheduledThreadPoolExecutor.setCorePoolSize((int) Math.min(Math.max((dataSets.get()/dataSetsPerThread.get()),1),maxSTPEThreads.get()));
        }

        logger.debug("Created New DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" )");
    }

    /**
     * Creates a new DataSet object with the given parameter
     * <p>
     * This function allows you to insert data on object creation but will throw a SetupException if the data doesnt match the given database, table or identifier parameter.
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param database   the name of the superordinate DataBase {@link DataBase} object
     * @param table      the name of the parent DataTable {@link DataTable} object
     * @param identifier the identifier of the current DataSet, this may be unique inside each DataTable
     * @param data       JSONObject containing the data. See {@link DataSet} for the expected format.
     * @throws DataStorageException if the data doesnt match the given database, table or identifier parameter {@link SetupException}
     */
    public DataSet(DataBase database, DataTable table, String identifier, JSONObject data) throws DataStorageException {
        // actual object
        this.identifier = identifier.toLowerCase();
        this.table = table;
        this.database = database;
        if(!this.identifier.equals(data.getString("identifier")) || !this.table.getIdentifier().equals(data.getString("table")) || !this.database.getIdentifier().equals(data.getString("database"))){
            if(this.identifier.equals(data.getString("identifier").toLowerCase()) && this.table.getIdentifier().equals(data.getString("table").toLowerCase()) && this.database.getIdentifier().equals(data.getString("database").toLowerCase())){
                // just convert to lower case
                data.put("identifier", data.getString("identifier").toLowerCase())
                        .put("table", data.getString("table").toLowerCase())
                        .put("database", data.getString("database").toLowerCase());
            }else{
                logger.error("Creation Failed. Data Does Not Match This Object");
                throw new DataStorageException(220, "DataSet: Identifier/Table/DataBase mismatch");
            }
        }
        // convert to lowercase
        List<String> list = new ArrayList<>();
        for(String s : new HashSet<>(data.keySet())){
            if(!s.toLowerCase().equals(s)){
                data.put(s.toLowerCase(), data.get(s));
                list.add(s);
            }
        }
        list.forEach(data::remove);
        this.data = data;

        // update scheduledThreadPoolExecutor
        dataSets.getAndIncrement();
        int i = (int)(Math.min(Math.max((dataSets.get()/dataSetsPerThread.get()),1),maxSTPEThreads.get()));
        if(i != scheduledThreadPoolExecutor.getCorePoolSize()){
            scheduledThreadPoolExecutor.setCorePoolSize((int) Math.min(Math.max((dataSets.get()/dataSetsPerThread.get()),1),maxSTPEThreads.get()));
        }

        logger.debug("Created New DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" )");
    }

    /*                  STATIC                    */

    /**
     * Sets the limit of threads the update scheduler may use.
     * <p>
     * The update scheduler is shared between all existing DataObjects
     * Allowed values are between 1 to n where 0 can be used to easily restore the default value (256)
     *
     * @param value maximum number of threads
     */
    public static void setMaxSTPEThreads(int value){
        value = Math.abs(value);
        if(value == 0){
            // set to default 256
            maxSTPEThreads.set(256);
        }else{
            maxSTPEThreads.set(value);
        }
    }

    /**
     * Sets the number of datasets processed by each thread.
     * <p>
     * Allowed values are between 1 to n where 0 can be used to easily restore the default value (5000)
     *
     * @param value number of datasets per thread
     */
    public static void setDataSetsPerThread(int value){
        value = Math.abs(value);
        if(value == 0){
            // set to default 5000
            dataSetsPerThread.set(5000);
        }else{
            dataSetsPerThread.set(value);
        }
        scheduledThreadPoolExecutor.setCorePoolSize((int)Math.min(Math.max((dataSets.get()/dataSetsPerThread.get()),1),maxSTPEThreads.get()));
    }

    /**
     * Gets the limit of threads the update scheduler may use.
     *
     * @return int int
     */
    public static int getMaxSTPEThreads(){
        return maxSTPEThreads.get();
    }

    /**
     * Get the number of datasets processed by each thread.
     *
     * @return int int
     */
    public static int getDataSetsPerThread(){
        return dataSetsPerThread.get();
    }

    /**
     * Get the total number of active datasets
     *
     * @return int long
     */
    public static long getDataSetCount(){
        return dataSets.get();
    }

    /**
     * Used to set the dataset count to a specific value
     * <p>
     * THIS IS MEANT TO BE USED FOR INTERNAL ACTIONS CORRECTING THE COUNT AT RUNTIME ONLY
     * This will take the absolute value of the input
     *
     * @param value int
     */
    public static void setDataSetCount(long value){
        dataSets.set(Math.abs(value));
    }

    /*                  OBJECT                    */

    /**
     * Returns the identifier of the current DataSet
     *
     * @return String string
     */
    public String getIdentifier(){ return this.identifier; }

    /**
     * Returns the name of the  of the parent DataTable {@link DataTable} object
     *
     * @return DataTable string
     */
    public DataTable getTable(){ return this.table; }

    /**
     * Returns the name of the  of the parent DataTable {@link DataBase} object
     *
     * @return DataBase data base name
     */
    public DataBase getDataBase() { return this.database; }


    /*                  DATA                    */

    /**
     * Returns a serialized copy of the stored data
     *
     * @return JSONObject containing a serialized copy of the stored data. See {@link DataSet} for the expected format.
     */
    public JSONObject getFullData(){
        // lock
        lock.readLock().lock();
        // get
        JSONObject dataCopy = new JSONObject(this.data.toString());
        // unlock
        lock.readLock().unlock();
        // stats
        statistics.accept(UsageStatistics.Usage.get_success);
        // return
        return dataCopy;
    } // suitable for getting the data for storage

    /**
     * Returns a serialized copy of the data for a specific dataType key, offers the possibility to lock this type for updating purposes
     * <p>
     * If acquire is set to true the dataType then an attempt is made to lock the data type for the next ~10 seconds to a specific token
     * Should this be successful the response contains the additional key "utoken" with the token as string. If this was not successful or the table does not force secure inserts the key will be missing.
     * Every String type input will be converted to lowercase only to simplify handling.
     * <p>
     * May return null if the object does not contain the specific dataType or this type is currently locked for updating purposes
     *
     * @param dataType represents the key of a json object
     * @param acquire  tries to lock the requested dataType to perform updates to the data within the next ~10 seconds
     * @return JSONObject containing a serialized copy of the stored data. See {@link DataSet} for the expected format.
     */
    public JSONObject get(String dataType, boolean acquire) {
        dataType = dataType.toLowerCase();
        try{
            // prepare response
            JSONObject responseData = new JSONObject()
                    .put("identifier", identifier);
            // get data
            lock.readLock().lock();
            if(this.data.has(dataType) && !updatePermissions.containsKey(dataType)){
                responseData.put(dataType, new JSONObject(this.data.getJSONObject(dataType).toString()));
                statistics.accept(UsageStatistics.Usage.get_success);
            }else{
                statistics.accept(UsageStatistics.Usage.get_failure);
            }
            lock.readLock().unlock();
            // check acquire
            if(acquire && table.hasSecureInsertEnabled()){
                updatePermissionLock.lock();
                if(!updatePermissions.containsKey(dataType) && !(dataType.equals("identifier") || dataType.equals("table") || dataType.equals("database"))){
                    String uToken = new String(Base64.getEncoder().encode(String.valueOf(new SecureRandom().nextLong()).getBytes()));
                    String finalDataType = dataType;
                    updatePermissions.put(dataType, new DataUpdateObject(uToken, updatePMSES.schedule(new Runnable() {
                        @Override
                        public void run() {
                            updatePermissions.remove(finalDataType);
                        }
                    }, 11, TimeUnit.SECONDS)));
                    responseData.put("utoken", uToken);
                    statistics.accept(UsageStatistics.Usage.acquire_success);
                }else{
                    statistics.accept(UsageStatistics.Usage.acquire_failure);
                }
                updatePermissionLock.unlock();
            }
            // response
            return responseData;
        }catch (Exception e){
            logger.error("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Get Operation Failed for DataType "+dataType, e);
            return null;
        }
    }

    /**
     * Used to insert/update data of a specific dataType
     * <p>
     * The data must include the matching dataType and utoken to verify if this action is allowed.
     * The utoken is only neccessary if the table forces secure inserts
     * Will return null if the dataType cant be updated or the token is invalid, false of the data does not match this object, is for an invalid dataType or the data does not contain the dataType, true on success.
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param dataType represents the key of a json object
     * @param data     JSONObject containing the specific data. See {@link DataSet} for the expected format.
     * @return Boolean boolean
     */
    public Boolean update(String dataType, JSONObject data){
        dataType = dataType.toLowerCase();
        try{
            if(table.hasSecureInsertEnabled()){
                // check if utoken is valid
                if(!updatePermissions.containsKey(dataType) || !updatePermissions.get(dataType).getuToken().equals(data.getString("utoken"))){
                    statistics.accept(UsageStatistics.Usage.update_failure);
                    return null;
                }
                // remove scheduled task
                updatePermissions.get(dataType).getScheduledFuture().cancel(true);
            }
            // check if dataset already has this type of data stored
            if(!this.data.has(dataType)){
                statistics.accept(UsageStatistics.Usage.update_failure);
                return null;
            }
            // check for invalid types
            if(dataType.equals("identifier") || dataType.equals("table") || dataType.equals("database")){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table+", "+this.identifier+"; Hash "+hashCode()+" ) - Update Operation Failed for DataType "+dataType+": Modification Of Critical Types");
                statistics.accept(UsageStatistics.Usage.update_failure);
                return false;
            }
            // check if data may be valid
            if(!data.getString("identifier").equals(this.identifier) || !data.getString("table").equals(this.table.getIdentifier()) || !data.getString("database").equals(this.database.getIdentifier()) || !data.has(dataType)){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Update Operation Failed for DataType "+dataType+": Data Does Not Match Specifications");
                statistics.accept(UsageStatistics.Usage.update_failure);
                return false;
            }
            // check if structure matches
            if(table.hasDefaultStructure()){
                if(!JSONMatcher.structureMatch(table.getDefaultStructure().getJSONObject(dataType), data.getJSONObject(dataType))){
                    logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Update Operation Failed for DataType "+dataType+": DataType Not Contain Required Structure");
                    statistics.accept(UsageStatistics.Usage.update_failure);
                    return false;
                }
            }
            // lock
            lock.writeLock().lock();
            // insert data
            this.data.put(dataType, new JSONObject(data.getJSONObject(dataType).toString()));
            // unlock & remove uToken
            lock.writeLock().lock();
            updatePermissions.remove(dataType);
            // stats
            statistics.accept(UsageStatistics.Usage.update_success);
            // return
            return true;
        }catch (Exception e){
            // unlock
            lock.writeLock().unlock();
            // stats
            statistics.accept(UsageStatistics.Usage.update_failure);
            // return
            logger.error("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Update Operation Failed for DataType "+dataType, e);
            return false;
        }
    }

    /**
     * Used to add new dataTypes
     * <p>
     * Will return null if the dataType already exists, false if its invalid and true on success
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param dataType represents the key of a json object
     * @return Boolean boolean
     */
    public Boolean insert(String dataType){
        dataType = dataType.toLowerCase();
        try{
            // check for invalid types
            if(dataType.equals("identifier") || dataType.equals("table") || dataType.equals("database")){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+": Modification Of Critical Types");
                statistics.accept(UsageStatistics.Usage.insert_failure);
                return null;
            }
            // check if data doesnt already contain this type of data
            if(this.data.has(dataType)){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+": DataType Already Existing");
                statistics.accept(UsageStatistics.Usage.insert_failure);
                return false;
            }
            // check if required / allowed by structure
            if(table.hasDefaultStructure() && !table.getDefaultStructure().has(dataType)){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+": DataType Not Required By Default Structure");
                statistics.accept(UsageStatistics.Usage.insert_failure);
                return false;
            }
            // lock
            lock.writeLock().lock();
            // insert
            this.data.put(dataType, new JSONObject());
            // unlock
            lock.writeLock().unlock();
            // return
            return true;
        }catch (Exception e){
            // unlock
            lock.writeLock().unlock();
            // stats
            statistics.accept(UsageStatistics.Usage.insert_success);
            // return
            logger.error("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType, e);
            return false;
        }
    }

    /**
     * Used to add new dataTypes and insert data to it
     * <p>
     * Will return null if the dataType already exists, false if the type or the data are invalid and true on success
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param dataType represents the key of a json object
     * @param data     the data
     * @return Boolean boolean
     */
    public Boolean insert(String dataType, JSONObject data){
        dataType = dataType.toLowerCase();
        try{
            // check for invalid types
            if(dataType.equals("identifier") || dataType.equals("table") || dataType.equals("database")){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+": Modification Of Critical Types");
                statistics.accept(UsageStatistics.Usage.insert_failure);
                return false;
            }
            // check if data doesnt already contain this type of data
            if(this.data.has(dataType)){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+": DataType Already Existing");
                statistics.accept(UsageStatistics.Usage.insert_failure);
                return null;
            }
            // check if the data is valid
            if(!data.getString("identifier").equals(this.identifier) || !data.getString("table").equals(this.table.getIdentifier()) || !data.getString("database").equals(this.database.getIdentifier()) || !data.has(dataType)){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+": Data Does Not Meet Requirements");
                statistics.accept(UsageStatistics.Usage.insert_failure);
                return false;
            }
            // check if required / allowed by structure
            if(table.hasDefaultStructure()){
                if(!table.getDefaultStructure().has(dataType)){
                    logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+": DataType Not Required By Default Structure");
                    statistics.accept(UsageStatistics.Usage.insert_failure);
                    return false;
                }else{
                    if(!JSONMatcher.structureMatch(table.getDefaultStructure().getJSONObject(dataType), data.getJSONObject(dataType))){ // check if both match the same structure; we can only do this because of out default structure having only jsonObjects at layer 0
                        logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+": DataType Not Contain Required Structure");
                        statistics.accept(UsageStatistics.Usage.insert_failure);
                        return false;
                    }
                }
            }
            // lock
            lock.writeLock().lock();
            // insert
            this.data.put(dataType, new JSONObject(data.getJSONObject(dataType).toString()));
            // unlock
            lock.writeLock().unlock();
            // stats
            statistics.accept(UsageStatistics.Usage.insert_success);
            // return
            return true;
        }catch (Exception e){
            // unlock
            lock.writeLock().unlock();
            // return
            logger.error("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+" + Data", e);
            return false;
        }
    }

    /**
     * Used to delete a specific dataType with its underlying data
     * <p>
     * Will return null if the dataType does not exist, false if its an invalid dataType and true on success.
     *
     * @param dataType represents the key of a json object
     * @return Boolean boolean
     */
    public Boolean delete(String dataType){
        dataType = dataType.toLowerCase();
        try{
            if(dataType.equals("identifier") || dataType.equals("table") || dataType.equals("database")){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Delete Operation Failed for DataType "+dataType+": Modification Of Critical Types");
                statistics.accept(UsageStatistics.Usage.delete_failure);
                return null;
            }
            // check if data contains this dataType
            if(!data.has(dataType)){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Delete Operation Failed for DataType "+dataType+": DataType Not Existing");
                statistics.accept(UsageStatistics.Usage.delete_failure);
                return false;
            }
            // check if not locked by any updates
            if(updatePermissions.containsKey(dataType)){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+": DataType Still In Use");
                statistics.accept(UsageStatistics.Usage.delete_failure);
                return false;
            }
            // check if not required by structure
            if(table.hasDefaultStructure() && table.getDefaultStructure().has(dataType)){
                logger.debug("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Insert Operation Failed for DataType "+dataType+": DataType Required By Default Structure");
                statistics.accept(UsageStatistics.Usage.delete_failure);
                return false;
            }
            // lock
            lock.writeLock().lock();
            // remove
            this.data.remove(dataType);
            // unlock
            lock.writeLock().unlock();
            // return
            return true;
        }catch (Exception e){
            // unlock
            lock.writeLock().unlock();
            // stats
            statistics.accept(UsageStatistics.Usage.delete_success);
            // return
            logger.error("DataSet ( Chain "+this.database.getIdentifier()+", "+this.table.getIdentifier()+", "+this.identifier+"; Hash "+hashCode()+" ) - Delete Operation Failed for DataType "+dataType, e);
            return true;
        }
    }

    /**
     * Used to check if the data contains a specific dataType
     *
     * @param dataType represents the key of a json object
     * @return boolean boolean
     */
    public boolean hasDataType(String dataType){
        dataType = dataType.toLowerCase();
        lock.readLock().lock();
        boolean has = data.has(dataType);
        lock.readLock().unlock();
        return has;
    }

    /**
     * Should be called when the object is supposed to be deleted / unloaded
     */
    protected void onUnload(){
        // decrease
        dataSets.getAndDecrement();
        if(dataSets.get() < 0){ dataSets.set(0); }
        if(Math.min(Math.max((dataSets.get()/dataSetsPerThread.get()),1),maxSTPEThreads.get()) != scheduledThreadPoolExecutor.getCorePoolSize()){
            scheduledThreadPoolExecutor.setCorePoolSize((int) Math.min(Math.max((dataSets.get()/dataSetsPerThread.get()),1),maxSTPEThreads.get())); // min 1 - max 256 threads (0 to 960000 datasets - larger values still share the same number of threads)
        }
    }

    /**
     * Used for storing information to queued updates of dataTypes
     * <p>
     * This class contains no further documentation as this gets rather replaced than modified
     */
    private static class DataUpdateObject{
        private final String uToken;
        private final ScheduledFuture<?> scheduledFuture;

        /**
         * Instantiates a new Data update object.
         *
         * @param uToken          the u token
         * @param scheduledFuture the scheduled future
         */
        DataUpdateObject(String uToken, ScheduledFuture<?> scheduledFuture){
            this.uToken = uToken;
            this.scheduledFuture = scheduledFuture;
        }

        /**
         * Gets scheduled future.
         *
         * @return the scheduled future
         */
        public ScheduledFuture<?> getScheduledFuture() { return scheduledFuture; }

        /**
         * Gets token.
         *
         * @return the token
         */
        public String getuToken() { return uToken; }
    }
}
