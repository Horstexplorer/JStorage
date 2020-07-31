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

import de.netbeacon.jstorage.server.internal.datamanager.DataManager;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.meta.UsageStatistics;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class represents the lowest classification level of the data assignment
 * <p>
 * This is used to to store DataSets {@link DataSet} within tables {@link DataTable} by their identifier
 *
 * @author horstexplorer
 */
public class DataBase {

    private final String identifier;
    private final ConcurrentHashMap<String, DataTable> dataTablePool = new ConcurrentHashMap<>(); // huehuehue :D
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final UsageStatistics usageStatistic = new UsageStatistics();
    private final AtomicBoolean encrypted = new AtomicBoolean(false);

    private final Logger logger = LoggerFactory.getLogger(DataBase.class);

    /**
     * Creates a new DataBase object
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier the identifier of this object
     * @throws DataStorageException when running setup() fails.
     */
    public DataBase(String identifier) throws DataStorageException{
        this.identifier = identifier.toLowerCase();
        setup();
        ready.set(true);

        logger.debug("Created New DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" )");
    }

    /*                  OBJECT                  */

    /**
     * Returns the name of the current object
     *
     * @return String string
     */
    public String getIdentifier(){ return identifier; }

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
     * Returns the usage statistics for this object
     *
     * @return UsageStatistics
     */
    public UsageStatistics getStatistics(){
        return usageStatistic;
    }

    /**
     * Returns if this database is supposed to be encrypted or not
     *
     * @return boolean
     */
    public boolean encrypted(){
        return encrypted.get();
    }

    /**
     * Used to enable or disable encryption for this database
     * <p>
     * Changes will only take effect on shards the next time they are loaded
     * This will only encrypt the data storage files itself, not the table/shard index
     * ! If this is enabled encryption will use the global encryption password - if this is not set or wrong data may be lost during en or decryption tries
     *
     * @param value enable/disable boolean
     * @throws CryptException on exception such as the CryptTool not being set up
     */
    public void setEncryption(boolean value) throws CryptException {
        if(!DataManager.getInstance().getJs2CryptTool().isReady()){
            throw new CryptException(1, "Cant Modify Encryption While The Tool Being Not Set Up");
        }
        encrypted.set(value);
    }

    /*                  ACCESS                  */

    /**
     * Used to get a DataTable with the matching identifier from the DataBase
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier of the target DataTable
     * @return DataTable table
     * @throws DataStorageException on various errors such as the object not being found
     */
    public DataTable getTable(String identifier) throws DataStorageException {
        try{
            lock.readLock().lock();
            if(!ready.get()){
                logger.error("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) Not Ready Yet");
                throw new DataStorageException(231, "DataBase: "+identifier+": Object Not Ready");
            }
            identifier = identifier.toLowerCase();
            if(!dataTablePool.containsKey(identifier)){
                logger.debug("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) DataTable "+identifier+" Not Found");
                throw new DataStorageException(203, "DataBase: "+identifier+": DataTable "+identifier+" Not Found.");
            }
            DataTable dataTable = dataTablePool.get(identifier);
            usageStatistic.add(UsageStatistics.Usage.get_success);
            return dataTable;
        }catch (DataStorageException e){
            usageStatistic.add(UsageStatistics.Usage.get_failure);
            throw e;
        }catch (Exception | Error e){
            logger.error("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) Unknown Error Requesting "+identifier, e);
            usageStatistic.add(UsageStatistics.Usage.get_failure);
            throw new DataStorageException(0, "DataBase: "+identifier+": Unknown Error: "+e.getMessage());
        }finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Used to insert a DataTable to the DataBase
     *
     * @param table the DataTable which should be inserted
     * @throws DataStorageException on various errors such as an object already existing with the same identifier
     */
    public void insertTable(DataTable table) throws DataStorageException {
        try{
            lock.writeLock().lock();
            if(!ready.get()){
                logger.error("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) Not Ready Yet");
                throw new DataStorageException(231, "DataBase: "+identifier+": Object Not Ready");
            }
            if(!identifier.equals(table.getDataBase().getIdentifier())){
                logger.debug("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) DataTable "+table.getIdentifier()+" Does Not Fit Here");
                throw new DataStorageException(220, "DataBase: "+identifier+": DataTable "+table.getIdentifier()+" ("+table.getDataBase().identifier+">"+table.getIdentifier()+") Does Not Fit Here.");
            }
            if(dataTablePool.containsKey(table.getIdentifier())){
                logger.debug("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) DataTable "+table.getIdentifier()+" Already Existing");
                throw new DataStorageException(213, "DataBase: "+identifier+": DataTable "+table.getIdentifier()+" Already Exists.");
            }
            dataTablePool.put(table.getIdentifier(), table);
            usageStatistic.add(UsageStatistics.Usage.insert_success);
        }catch (DataStorageException e){
            usageStatistic.add(UsageStatistics.Usage.insert_failure);
            throw e;
        }catch (Exception | Error e){
            logger.error("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) Unknown Error Requesting "+identifier, e);
            usageStatistic.add(UsageStatistics.Usage.insert_failure);
            throw new DataStorageException(0, "DataBase: "+identifier+": Unknown Error: "+e.getMessage());
        }finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Used to delete a DataTable from the DataBase
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier of the target DataTable
     * @throws DataStorageException on various errors such as the object not being found
     */
    public void deleteTable(String identifier) throws DataStorageException{
        try{
            lock.writeLock().lock();
            if(!ready.get()){
                logger.error("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) Not Ready Yet");
                throw new DataStorageException(231, "DataBase: "+identifier+": Object Not Ready");
            }
            identifier = identifier.toLowerCase();
            if(!dataTablePool.containsKey(identifier)){
                logger.debug("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) DataTable "+identifier+" Not Found");
                throw new DataStorageException(203, "DataBase: "+identifier+": DataTable "+identifier+" Not Found.");
            }
            dataTablePool.get(identifier).delete();
            dataTablePool.remove(identifier);
            usageStatistic.add(UsageStatistics.Usage.delete_success);
        }catch (DataStorageException e){
            usageStatistic.add(UsageStatistics.Usage.delete_failure);
            throw e;
        }catch (Exception | Error e){
            logger.error("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) Unknown Error Requesting "+identifier, e);
            usageStatistic.add(UsageStatistics.Usage.delete_failure);
            throw new DataStorageException(0, "DataBase: "+identifier+": Unknown Error: "+e.getMessage());
        }finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Used to check if the DataBase contains a specific DataTable
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier identifier of the DataTable. See {@link DataTable} for further information.
     * @return boolean boolean
     */
    public boolean containsDataTable(String identifier){
        lock.readLock().lock();
        identifier = identifier.toLowerCase();
        boolean contains = dataTablePool.containsKey(identifier);
        lock.readLock().unlock();
        return contains;
    }


    /*                  MISC                    */

    /**
     * Used for initial setup of the DataBase object
     * <p>
     * Creates all listed DataTables. See {@link DataTable}
     *
     * @throws DataStorageException on error
     */
    private void setup() throws DataStorageException{
        if(!ready.get() && !shutdown.get()){
            try{
                // read from file
                File d = new File("./jstorage/data/db/"+identifier);
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/data/db/"+identifier+"/"+identifier+"_settings");
                if(!f.exists()){ f.createNewFile(); }
                else{
                    // read
                    String content = new String(Files.readAllBytes(f.toPath()));
                    if(!content.isEmpty()){
                        JSONObject jsonObject = new JSONObject(content);
                        String dbn = jsonObject.getString("database").toLowerCase();
                        JSONArray tbns = jsonObject.getJSONArray("tables");
                        encrypted.set(jsonObject.getBoolean("encrypted"));
                        // might contain other settings in the future
                        if(identifier.equals(dbn)){
                            // create tables
                            for(int i = 0; i < tbns.length(); i++){
                                String tableName = tbns.getString(i).toLowerCase();
                                if(!dataTablePool.containsKey(tableName)){
                                    // try to create a new object; we dont try to catch an exception as we dont want to loose whole tables of data
                                    DataTable dataTable = new DataTable(this, tableName);
                                    dataTablePool.put(dataTable.getIdentifier(), dataTable);
                                }
                            }
                        }else{
                            throw new Exception("Content Does Not Match Expectations");
                        }
                    }
                }
            }catch (Exception e){
                logger.error("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) Loading Data Failed", e);
                throw new DataStorageException(101, "DataBase: "+identifier+": Loading Data Failed: "+e.getMessage());
            }
        }
    }

    /**
     * Used to store content on shutdown
     *
     * @throws DataStorageException on any error. This may result in data getting lost.
     */
    public void shutdown() throws DataStorageException {
        if(ready.get()){
            shutdown.set(true);
            ready.set(false);
            // shutdown tables
            for (Map.Entry<String, DataTable> entry : dataTablePool.entrySet()){
                try{
                    entry.getValue().shutdown();
                }catch (DataStorageException ignore){}
            }
            // write to file
            try{
                // build json
                JSONObject jsonObject = new JSONObject()
                        .put("database", identifier);
                JSONArray jsonArray = new JSONArray();
                dataTablePool.forEach((k, v)-> jsonArray.put(v.getIdentifier()));
                jsonObject.put("tables", jsonArray).put("encrypted", encrypted.get());
                // write to file
                File d = new File("./jstorage/data/db/"+identifier);
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/data/db/"+identifier+"/"+identifier+"_settings");
                BufferedWriter writer = new BufferedWriter(new FileWriter(f));
                writer.write(jsonObject.toString());
                writer.newLine();
                writer.flush();
                writer.close();
            }catch (Exception e){
                logger.error("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) Unloading Data Failed", e);
                throw new DataStorageException(102, "DataBase: "+identifier+": Failed To Write Configuration File, Data May Be Lost: "+e.getMessage());
            }
            // clear
            dataTablePool.clear();
            // dont reset shutdown atomic. this object should not be used further
        }
    }

    /**
     * Used to safely delete this object and its content
     * <p>
     * This will make this object unusable
     */
    public void delete(){
        shutdown.set(true);
        ready.set(false);
        // shutdown & clear everything
        dataTablePool.forEach((k, v)-> v.delete());
        dataTablePool.clear();
        // delete files
        try{
            File d = new File("./jstorage/data/"+identifier);
            FileUtils.deleteDirectory(d);
        }catch (Exception e){
            logger.error("DataBase ( Chain "+this.identifier+"; Hash "+hashCode()+" ) Deleting Files Failed. Manual Actions May Be Required", e);
            System.err.println("DataTable: "+identifier+": Deleting Files Failed. Manual Actions May Be Required. "+e.getMessage());
        }
        // dont reset shutdown atomic. this object should not be used further
    }

    /*              POOL              */

    /**
     * Returns the internal storage object for all DataTable objects.
     *
     * @return ConcurrentHashMap<String, DataTable> data pool
     */
    public ConcurrentHashMap<String, DataTable> getDataPool() {
        return dataTablePool;
    }

}
