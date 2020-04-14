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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class takes care of preparing and accessing all DataBase objects
 * <p>
 * The user should only access the objects through this class but should be able to create them outside of it
 *
 * @author horstexplorer
 */
public class DataManager {

    private static final AtomicBoolean ready = new AtomicBoolean(false);
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);
    private static final ConcurrentHashMap<String, DataBase> dataBasePool = new ConcurrentHashMap<>();
    private static final ReadWriteLock lock = new ReentrantReadWriteLock();
    private static ScheduledExecutorService ses;
    private static Future<?> counterTask; // the fix has been planted

    private static final Logger logger = LoggerFactory.getLogger(DataManager.class);

    /**
     * Used to set up all DataBase objects
     *
     * @throws SetupException on setup() throwing an error
     */
    public DataManager() throws SetupException {
        if(!ready.get() && !shutdown.get()){
            setup();
            ready.set(true);
        }
    }

    /*                  DATA                    */

    /**
     * Used to get a DataBase with the matching identifier from the DataManager
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier of the target DataBase
     * @return DataBase data base
     * @throws DataStorageException on various errors such as the object not existing
     */
    public static DataBase getDataBase(String identifier) throws DataStorageException {
        try{
            identifier = identifier.toLowerCase();
            lock.readLock().lock();
            if(ready.get()){
                if(dataBasePool.containsKey(identifier)){
                    DataBase dataBase = dataBasePool.get(identifier);
                    lock.readLock().unlock();
                    return dataBase;
                }
                logger.debug("DataBase "+identifier+" Not Found");
                throw new DataStorageException(204, "DataManager: DataBase "+identifier+" Not Found.");
            }
            logger.error("Not Ready Yet");
            throw new DataStorageException(231, "DataManager Not Ready Yet");
        }catch (DataStorageException e){
            lock.readLock().unlock();
            throw e;
        }
    }

    /**
     * Used to create a DataBase within the DataManager
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier of the new DataBase
     * @return DataBase data base
     * @throws DataStorageException on various errors such as an object already existing with the same identifier
     */
    public static DataBase createDataBase(String identifier) throws DataStorageException {
        try{
            identifier = identifier.toLowerCase();
            lock.writeLock().lock();
            if(ready.get()){
                if(!dataBasePool.containsKey(identifier)){
                    DataBase dataBase = new DataBase(identifier);
                    dataBasePool.put(dataBase.getDataBaseName(), dataBase);
                    lock.writeLock().unlock();
                    return dataBase;
                }
                logger.debug("DataBase "+identifier+" Already Existing");
                throw new DataStorageException(214, "DataManager: DataBase "+identifier+" Already Existing.");
            }
            logger.error("Not Ready Yet");
            throw new DataStorageException(231, "DataManager Not Ready Yet");
        }catch (DataStorageException e){
            lock.writeLock().unlock();
            throw e;
        }
    }

    /**
     * Used to delete a DataBase from the DataManager
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier of the target DataBase
     * @throws DataStorageException on various errors such as the object not existing
     */
    public static void deleteDataBase(String identifier) throws DataStorageException {
        try{
            identifier = identifier.toLowerCase();
            lock.writeLock().lock();
            if(ready.get()){
                if(dataBasePool.containsKey(identifier)){
                    DataBase dataBase = dataBasePool.get(identifier);
                    dataBase.delete();
                    dataBasePool.remove(identifier);
                    lock.writeLock().unlock();
                    return;
                }
                logger.debug("DataBase "+identifier+" Not Found");
                throw new DataStorageException(204, "DataManager: DataBase "+identifier+" Not Found.");
            }
            logger.error("Not Ready Yet");
            throw new DataStorageException(231, "DataManager Not Ready Yet");
        }catch (DataStorageException e){
            lock.writeLock().unlock();
            throw e;
        }
    }

    /**
     * Used to check if the DataManager contains a specific DataBase
     * <p>
     * Every String type input will be converted to lowercase only to simplify handling.
     *
     * @param identifier identifier of the DataBase. See {@link DataBase} for further information.
     * @return boolean boolean
     */
    public static boolean containsDataBase(String identifier){
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
    public static void insertDataBase(DataBase dataBase) throws DataStorageException {
        try{
            lock.writeLock().lock();
            if(ready.get()){
                if(!dataBasePool.containsKey(dataBase.getDataBaseName())){
                    dataBasePool.put(dataBase.getDataBaseName(), dataBase);
                    lock.writeLock().unlock();
                    return;
                }
                logger.debug("DataBase "+dataBase.getDataBaseName()+" Already Existing");
                throw new DataStorageException(214, "DataManager: DataBase "+dataBase.getDataBaseName()+" Already Existing.");
            }
            logger.error("Not Ready Yet");
            throw new DataStorageException(231, "DataManager Not Ready Yet");
        }catch (DataStorageException e){
            lock.writeLock().unlock();
            throw e;
        }
    }

    /*                  MISC                    */

    /**
     * Used for initial setup of the DataManager
     * <p>
     * Creates all listed DataBase objects. See {@link DataBase}
     *
     * @throws SetupException on error
     */
    private void setup() throws SetupException{
        if(!ready.get() && !shutdown.get()){
            try{
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
                                if(!dataBasePool.containsKey(dataBase.getDataBaseName())){
                                    dataBasePool.put(dataBase.getDataBaseName(), dataBase);
                                }else{
                                    throw new DataStorageException(214, "DataManager: Setup: DataBase "+dataBase.getDataBaseName()+" Already Existing.");
                                }
                            }catch (DataStorageException e){
                                logger.error("Failed To Create DataBase During Setup", e);
                            }
                        }
                    }
                }
                // setup task & ses
                ses = Executors.newScheduledThreadPool(1);
                counterTask = ses.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
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
                    }
                }, 10, 10, TimeUnit.MINUTES);
            }catch (Exception e){
                throw new SetupException("DataManager: Setup Failed: "+e.getMessage());
            }
        }
    }

    /**
     * Used to store content on shutdown
     *
     * @throws ShutdownException on any error. This may result in data getting lost.
     */
    public static void shutdown() throws ShutdownException{
        shutdown.set(true);
        ready.set(false);
        try{
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
                    jsonArray.put(entry.getValue().getDataBaseName());
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
        }catch (Exception e){
            logger.error("Shutdown Failed. Data May Be Lost", e);
            throw new ShutdownException("DataManager: Shutdown Failed. Data May Be Lost: "+e.getMessage());
        }
        shutdown.set(false);
    }

    /*              POOL              */

    /**
     * Returns the internal storage object for all DataBase objects
     *
     * @return ConcurrentHashMap<String, DataBase> data pool
     */
    public static ConcurrentHashMap<String, DataBase> getDataPool() {
        return dataBasePool;
    }
}
