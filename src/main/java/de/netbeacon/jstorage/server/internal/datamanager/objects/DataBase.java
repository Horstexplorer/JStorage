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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class represents the lowest classification level of the data assignment
 * <p>
 * This is used to to store DataSets {@link DataSet} within tables {@link DataTable} by their identifier
 *
 * @author horstexplorer
 */
public class DataBase {

    private final String dataBaseName;
    private final ConcurrentHashMap<String, DataTable> dataTablePool = new ConcurrentHashMap<>(); // huehuehue :D
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private final Logger logger = LoggerFactory.getLogger(DataBase.class);

    /**
     * Creates a new DataBase object
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param dataBase the identifier of this object
     * @throws DataStorageException when running setup() fails.
     */
    public DataBase(String dataBase) throws DataStorageException{
        this.dataBaseName = dataBase.toLowerCase();
        setup();
        ready.set(true);

        logger.debug("Created New DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" )");
    }

    /*                  OBJECT                  */

    /**
     * Returns the name of the current object
     *
     * @return String string
     */
    public String getDataBaseName(){ return dataBaseName; }

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
     * Used to get a DataTable with the matching identifier from the DataBase
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier of the target DataTable
     * @return DataTable table
     * @throws DataStorageException on various errors such as the object not being found
     */
    public DataTable getTable(String identifier) throws DataStorageException {
        if(ready.get()){
            lock.readLock().lock();
            identifier = identifier.toLowerCase();
            try{
                if(dataTablePool.containsKey(identifier)){
                    DataTable dataTable = dataTablePool.get(identifier);
                    lock.readLock().unlock();
                    return dataTable;
                }
                logger.debug("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) DataTable "+identifier+" Not Found");
                throw new DataStorageException(203, "DataBase: "+dataBaseName+": DataTable "+identifier+" Not Found.");
            }catch (DataStorageException e){
                lock.readLock().unlock();
                throw e;
            }catch (Exception e){
                lock.readLock().unlock();
                logger.error("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) Unknown Error Requesting "+identifier, e);
                throw new DataStorageException(0, "DataBase: "+dataBaseName+": Unknown Error: "+e.getMessage());
            }
        }
        logger.error("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) Not Ready Yet");
        throw new DataStorageException(231, "DataBase: "+dataBaseName+": Object Not Ready");
    }

    /**
     * Used to insert a DataTable to the DataBase
     *
     * @param table the DataTable which should be inserted
     * @throws DataStorageException on various errors such as an object already existing with the same identifier
     */
    public void insertTable(DataTable table) throws DataStorageException {
        if(ready.get()){
            lock.writeLock().lock();
            try{
                if(dataBaseName.equals(table.getDatabaseName())){
                    if(!dataTablePool.containsKey(table.getTableName())){
                        dataTablePool.put(table.getTableName(), table);
                        lock.writeLock().unlock();
                        return;
                    }
                    logger.debug("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) DataTable "+table.getTableName()+" Already Existing");
                    throw new DataStorageException(213, "DataBase: "+dataBaseName+": DataTable "+table.getTableName()+" Already Exists.");
                }
                logger.debug("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) DataTable "+table.getTableName()+" Does Not Fit Here");
                throw new DataStorageException(220, "DataBase: "+dataBaseName+": DataTable "+table.getTableName()+" ("+table.getDatabaseName()+">"+table.getTableName()+") Does Not Fit Here.");
            }catch (DataStorageException e){
                lock.writeLock().unlock();
                throw e;
            }catch (Exception e){
                lock.writeLock().unlock();
                logger.error("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) Unknown Error Inserting "+table.getTableName(), e);
                throw new DataStorageException(0, "DataBase: "+dataBaseName+": Unknown Error: "+e.getMessage());
            }
        }
        logger.error("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) Not Ready Yet");
        throw new DataStorageException(231, "DataBase: "+dataBaseName+": Object Not Ready");
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
        if(ready.get()){
            lock.writeLock().lock();
            identifier = identifier.toLowerCase();
            try{
                if(dataTablePool.containsKey(identifier)){
                    dataTablePool.get(identifier).delete();
                    dataTablePool.remove(identifier);
                    lock.writeLock().unlock();
                    return;
                }
                logger.debug("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) DataTable "+identifier+" Not Found");
                throw new DataStorageException(203, "DataBase: "+dataBaseName+": DataTable "+identifier+" Not Found.");
            }catch (DataStorageException e){
                lock.writeLock().unlock();
                throw e;
            }catch (Exception e){
                lock.writeLock().unlock();
                logger.error("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) Unknown Error Deleting "+identifier, e);
                throw new DataStorageException(0, "DataBase: "+dataBaseName+": Unknown Error: "+e.getMessage());
            }
        }
        logger.error("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) Not Ready Yet");
        throw new DataStorageException(231, "DataBase: "+dataBaseName+": Object Not Ready");
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
                File d = new File("./jstorage/data/db/"+dataBaseName);
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/data/db/"+dataBaseName+"/"+dataBaseName+"_settings");
                if(!f.exists()){ f.createNewFile(); }
                else{
                    // read
                    String content = new String(Files.readAllBytes(f.toPath()));
                    if(!content.isEmpty()){
                        JSONObject jsonObject = new JSONObject(content);
                        String dbn = jsonObject.getString("database").toLowerCase();
                        JSONArray tbns = jsonObject.getJSONArray("tables");
                        // might contain other settings in the future
                        if(dataBaseName.equals(dbn)){
                            // create tables
                            for(int i = 0; i < tbns.length(); i++){
                                String tableName = tbns.getString(i).toLowerCase();
                                if(!dataTablePool.containsKey(tableName)){
                                    // try to create a new object; we dont try to catch an exception as we dont want to loose whole tables of data
                                    DataTable dataTable = new DataTable(dataBaseName, tableName);
                                    dataTablePool.put(dataTable.getTableName(), dataTable);
                                }
                            }
                        }else{
                            throw new Exception("Content Does Not Match Expectations");
                        }
                    }
                }
            }catch (Exception e){
                logger.error("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) Loading Data Failed", e);
                throw new DataStorageException(101, "DataBase: "+dataBaseName+": Loading Data Failed: "+e.getMessage());
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
                        .put("database", dataBaseName);
                JSONArray jsonArray = new JSONArray();
                dataTablePool.forEach((k, v)->{ jsonArray.put(v.getTableName()); });
                jsonObject.put("tables", jsonArray);
                // write to file
                File d = new File("./jstorage/data/db/"+dataBaseName);
                if(!d.exists()){ d.mkdirs(); }
                File f = new File("./jstorage/data/db/"+dataBaseName+"/"+dataBaseName+"_settings");
                BufferedWriter writer = new BufferedWriter(new FileWriter(f));
                writer.write(jsonObject.toString());
                writer.newLine();
                writer.flush();
                writer.close();
            }catch (Exception e){
                logger.error("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) Unloading Data Failed", e);
                throw new DataStorageException(102, "DataBase: "+dataBaseName+": Failed To Write Configuration File, Data May Be Lost: "+e.getMessage());
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
        dataTablePool.forEach((k, v)-> {
            v.delete();
        });
        dataTablePool.clear();
        // delete files
        try{
            File d = new File("./jstorage/data/"+dataBaseName);
            FileUtils.deleteDirectory(d);
        }catch (Exception e){
            logger.error("DataBase ( Chain "+this.dataBaseName+"; Hash "+hashCode()+" ) Deleting Files Failed. Manual Actions May Be Required", e);
            System.err.println("DataTable: "+dataBaseName+": Deleting Files Failed. Manual Actions May Be Required. "+e.getMessage());
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
