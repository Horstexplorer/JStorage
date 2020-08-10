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

package de.netbeacon.jstorage.server.internal.datamanager;

import de.netbeacon.jstorage.server.internal.datamanager.objects.DataBase;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataSet;
import de.netbeacon.jstorage.server.tools.crypt.JS2CryptTool;
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
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class takes care of preparing and accessing all DataBase objects
 *
 * The user should only access the objects through this class but should be able to create them outside of it </br>
 *
 * @author horstexplorer
 */
public class DataManager {

    private static DataManager instance;

    private final AtomicBoolean ready = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, DataBase> dataBasePool = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private ScheduledExecutorService ses;
    private Future<?> counterTask; // the fix has been planted
    private JS2CryptTool js2CryptTool;
    private final ReentrantLock lock2 = new ReentrantLock();
    private final Logger logger = LoggerFactory.getLogger(DataManager.class);

    /**
     * Used to create an instance of this class
     */
    private DataManager(){}

    /**
     * Used to get the instance of this class without forcing initialization
     *
     * @return DataManager
     */
    public static DataManager getInstance(){
        return getInstance(false);
    }

    /**
     * Used to get the instance of this class
     *
     * Can be used to initialize the class if this didnt happened yet </br>
     *
     * @param initializeIfNeeded boolean
     * @return DataManager
     */
    public static DataManager getInstance(boolean initializeIfNeeded){
        if((instance == null && initializeIfNeeded)){
            instance = new DataManager();
        }
        return instance;
    }


    /*                  DATA                    */

    /**
     * Used to get a DataBase with the matching identifier from the DataManager
     *
     * Every String type input will be converted to lowercase only to simplify handling. </br>
     *
     * @param identifier of the target DataBase
     * @return DataBase data base
     * @throws DataStorageException on various errors such as the object not existing
     */
    public DataBase getDataBase(String identifier) throws DataStorageException {
        try{
            lock.readLock().lock();
            identifier = identifier.toLowerCase();
            if(!ready.get()){
                logger.error("Not Ready Yet");
                throw new DataStorageException(231, "DataManager Not Ready Yet");
            }
            if(!dataBasePool.containsKey(identifier)){
                logger.debug("DataBase "+identifier+" Not Found");
                throw new DataStorageException(204, "DataManager: DataBase "+identifier+" Not Found.");
            }
            return dataBasePool.get(identifier);
        }finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Used to create a DataBase within the DataManager
     *
     * Every String type input will be converted to lowercase only to simplify handling. </br>
     *
     * @param identifier of the new DataBase
     * @return DataBase data base
     * @throws DataStorageException on various errors such as an object already existing with the same identifier
     */
    public DataBase createDataBase(String identifier) throws DataStorageException {
        try{
            lock.writeLock().lock();
            identifier = identifier.toLowerCase();
            if(!ready.get()){
                logger.error("Not Ready Yet");
                throw new DataStorageException(231, "DataManager Not Ready Yet");
            }
            if(dataBasePool.containsKey(identifier)){
                logger.debug("DataBase "+identifier+" Already Existing");
                throw new DataStorageException(214, "DataManager: DataBase "+identifier+" Already Existing.");
            }
            DataBase dataBase = new DataBase(identifier);
            dataBasePool.put(dataBase.getIdentifier(), dataBase);
            return dataBase;
        }finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Used to delete a DataBase from the DataManager
     *
     * Every String type input will be converted to lowercase only to simplify handling. </br>
     *
     * @param identifier of the target DataBase
     * @throws DataStorageException on various errors such as the object not existing
     */
    public void deleteDataBase(String identifier) throws DataStorageException {
        try{
            lock.writeLock().lock();
            identifier = identifier.toLowerCase();
            if(!ready.get()){
                logger.error("Not Ready Yet");
                throw new DataStorageException(231, "DataManager Not Ready Yet");
            }
            if(!dataBasePool.containsKey(identifier)){
                logger.debug("DataBase "+identifier+" Not Found");
                throw new DataStorageException(204, "DataManager: DataBase "+identifier+" Not Found.");
            }
            DataBase dataBase = dataBasePool.get(identifier);
            dataBase.delete();
            dataBasePool.remove(identifier);
        }finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Used to check if the DataManager contains a specific DataBase
     *
     * Every String type input will be converted to lowercase only to simplify handling. </br>
     *
     * @param identifier identifier of the DataBase. See {@link DataBase} for further information.
     * @return boolean boolean
     */
    public boolean containsDataBase(String identifier){
        lock.readLock().lock();
        identifier = identifier.toLowerCase();
        boolean contains = dataBasePool.containsKey(identifier);
        lock.readLock().unlock();
        return contains;
    }

    /*                  ADVANCED                    */

    /**
     * Used to insert a DataBase to the DataManager
     *
     * @param dataBase the DataBase which should be inserted
     * @throws DataStorageException on various errors such as an object already existing with the same identifier
     */
    public void insertDataBase(DataBase dataBase) throws DataStorageException {
        try{
            lock.writeLock().lock();
            if(!ready.get()){
                logger.error("Not Ready Yet");
                throw new DataStorageException(231, "DataManager Not Ready Yet");
            }
            if(dataBasePool.containsKey(dataBase.getIdentifier())){
                logger.debug("DataBase "+dataBase.getIdentifier()+" Already Existing");
                throw new DataStorageException(214, "DataManager: DataBase "+dataBase.getIdentifier()+" Already Existing.");
            }
            dataBasePool.put(dataBase.getIdentifier(), dataBase);
        }catch (DataStorageException e){
            lock.writeLock().unlock();
            throw e;
        }
    }

    /**
     * Used to get the crypt tool
     *
     * @return JS2CryptTool
     */
    public JS2CryptTool getJs2CryptTool(){
        return js2CryptTool;
    }

    /*                  MISC                    */

    /**
     * Used for initial setup of the DataManager
     *
     * Creates all listed DataBase objects. See {@link DataBase} </br>
     *
     * @throws SetupException on error
     */
    public void setup(boolean setupEncryptionNow) throws SetupException{
        try{
            lock2.lock();
            if(ready.get() || shutdown.get()){
                return;
            }

            js2CryptTool = new JS2CryptTool("./jstorage/config/js2crypt", setupEncryptionNow);

            File d = new File("./jstorage/data/db/");
            if(!d.exists()){ d.mkdirs(); }
            File f = new File("./jstorage/data/db/datamanager");
            if(!f.exists()){ f.createNewFile(); }
            else{
                // read
                String content = new String(Files.readAllBytes(f.toPath()));
                if(!content.isEmpty()){
                    JSONObject jsonObject = new JSONObject(content);
                    JSONObject jsonObject1 = jsonObject.getJSONObject("dataSetSettings");
                    try{
                        DataSet.setDataSetsPerThread(jsonObject1.getInt("dataSetsPerThread"));
                        DataSet.setMaxSTPEThreads(jsonObject1.getInt("maxSTPEThreads"));
                    }catch (Exception e){
                        logger.error("DataSet Configuration Failed", e);
                    }
                    JSONArray jsonArray = jsonObject.getJSONArray("databases");
                    // might contain other settings in the future
                    for(int i = 0; i < jsonArray.length(); i++){
                        try{
                            String dbn = jsonArray.getString(i).toLowerCase();
                            DataBase dataBase = new DataBase(dbn);
                            if(!dataBasePool.containsKey(dataBase.getIdentifier())){
                                dataBasePool.put(dataBase.getIdentifier(), dataBase);
                            }else{
                                throw new DataStorageException(214, "DataManager: Setup: DataBase "+dataBase.getIdentifier()+" Already Existing.");
                            }
                        }catch (DataStorageException e){
                            logger.error("Failed To Create DataBase During Setup", e);
                        }
                    }
                }
            }
            // setup task & ses
            ses = Executors.newScheduledThreadPool(1);
            counterTask = ses.scheduleAtFixedRate(() -> {
                AtomicLong sets = new AtomicLong();
                dataBasePool.values().forEach(v->v.getDataPool().values().forEach(c-> sets.addAndGet(c.getIndexPool().size())));
                long l = Math.abs(DataSet.getDataSetCount()-sets.get());
                if(l > 0){
                    if(l < 15){
                        logger.info("DataManager: Difference Between Expected & Indexed DataSets Too Large ("+l+"). Updating Values.");
                        DataSet.setDataSetCount(sets.get());
                    }else{
                        logger.info("DataManager: Difference Between Expected & Indexed DataSets Detected ("+l+"). No Actions Required.");
                    }
                }
            }, 10, 10, TimeUnit.MINUTES);

            ready.set(true);
        }catch (Exception e){
            throw new SetupException("DataManager: Setup Failed: "+e.getMessage());
        }finally {
            lock2.unlock();
        }
    }

    /**
     * Used to store content on shutdown
     *
     * @throws ShutdownException on any error. This may result in data getting lost.
     */
    public void shutdown() throws ShutdownException{
        try{
            lock2.lock();
            shutdown.set(true);
            ready.set(false);
            // shutdown task & ses
            counterTask.cancel(true);
            ses.shutdown();
            // build json while shutdown databases
            JSONObject jsonObject = new JSONObject();
            JSONArray jsonArray = new JSONArray();
            for(Map.Entry<String, DataBase> entry : dataBasePool.entrySet()){
                try{
                    entry.getValue().shutdown();
                }catch (Exception ignore){}finally {
                    jsonArray.put(entry.getValue().getIdentifier());
                }
            }
            jsonObject.put("databases", jsonArray).put("dataSetSettings", new JSONObject().put("dataSetsPerThread", DataSet.getDataSetsPerThread()).put("maxSTPEThreads", DataSet.getMaxSTPEThreads()));
            // write to file
            File d = new File("./jstorage/data/db/");
            if(!d.exists()){ d.mkdirs(); }
            File f = new File("./jstorage/data/db/datamanager");
            BufferedWriter writer = new BufferedWriter(new FileWriter(f));
            writer.write(jsonObject.toString());
            writer.newLine();
            writer.flush();
            writer.close();
            // clear
            dataBasePool.clear();
            // shutdown js2
            js2CryptTool.shutdown();
        }catch (Exception e){
            logger.error("Shutdown Failed. Data May Be Lost", e);
            throw new ShutdownException("DataManager: Shutdown Failed. Data May Be Lost: "+e.getMessage());
        }finally {
            shutdown.set(false);
            lock2.unlock();
        }
    }

    /*              POOL              */

    /**
     * Returns the internal storage object for all DataBase objects
     *
     * @return ConcurrentHashMap<String, DataBase> data pool
     */
    public ConcurrentHashMap<String, DataBase> getDataPool() {
        return dataBasePool;
    }
}
