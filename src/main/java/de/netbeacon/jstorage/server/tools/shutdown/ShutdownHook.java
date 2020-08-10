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

package de.netbeacon.jstorage.server.tools.shutdown;

import de.netbeacon.jstorage.server.internal.cachemanager.CacheManager;
import de.netbeacon.jstorage.server.internal.datamanager.DataManager;
import de.netbeacon.jstorage.server.internal.notificationmanager.NotificationManager;
import de.netbeacon.jstorage.server.internal.usermanager.UserManager;
import de.netbeacon.jstorage.server.socket.api.APISocket;
import de.netbeacon.jstorage.server.socket.hello.HelloSocket;
import de.netbeacon.jstorage.server.socket.notification.NotificationSocket;
import de.netbeacon.jstorage.server.tools.ipban.IPBanManager;
import de.netbeacon.jstorage.server.tools.meta.SystemStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class contains the shutdown hook which is supposed to shutdown everything nicely, at least for normal shutdowns
 *
 * @author horstexplorer
 */
public class ShutdownHook {

    private static final AtomicBoolean inserted = new AtomicBoolean(false);
    private final Logger logger = LoggerFactory.getLogger(ShutdownHook.class);

    /**
     * Insert this as ShutdownHook
     *
     * Will only add itself once </br>
     */
    public ShutdownHook(){
        if(!inserted.get()){
            inserted.set(true);
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdownNow));
        }
    }

    private void shutdownNow(){
        logger.info("! ShutdownHook Executed !");
        try{
            logger.info("Shutting Down HelloSocket...");
            HelloSocket.getInstance().shutdown();
            logger.info("Shutting Down HelloSocket Finished");
        }catch (NullPointerException e){
            logger.info("HelloSocket Not Initialized");
        }catch (Exception e){
            logger.error("Shutting Down HelloSocket Failed", e);
        }
        try{
            logger.info("Shutting Down APISocket...");
            APISocket.getInstance().shutdown();
            logger.info("Shutting Down APISocket Finished");
        }catch (NullPointerException e){
            logger.info("Down APISocket Not Initialized");
        }catch (Exception e){
            logger.error("Shutting Down APISocket Failed", e);
        }
        try{
            logger.info("Shutting Down NotificationSocket...");
            NotificationSocket.getInstance().shutdown();
            logger.info("Shutting Down NotificationSocket Finished");
        }catch (NullPointerException e){
            logger.info("NotificationSocket Not Initialized");
        }catch (Exception e){
            logger.error("Shutting Down NotificationSocket Failed", e);
        }
        try{
            logger.info("Shutting Down IPBanManager...");
            IPBanManager.getInstance().shutdown();
            logger.info("Shutting Down IPBanManager Finished");
        }catch (NullPointerException e){
            logger.info("IPBanManager Not Initialized");
        }catch (Exception e){
            logger.error("Shutting Down IPBanManager Failed", e);
        }
        try{
            logger.info("Shutting Down UserManager...");
            UserManager.getInstance().shutdown();
            logger.info("Shutting Down UserManager Finished");
        }catch (NullPointerException e){
            logger.info("UserManager Not Initialized");
        }catch (Exception e){
            logger.error("Shutting Down UserManager Failed", e);
        }
        try{
            logger.info("Shutting Down DataManager...");
            DataManager.getInstance().shutdown();
            logger.info("Shutting Down DataManager Finished");
        }catch (NullPointerException e){
            logger.info("DataManager Not Initialized");
        }catch (Exception e){
            logger.error("Shutting Down DataManager Failed", e);
        }
        try{
            logger.info("Shutting Down CacheManager...");
            CacheManager.getInstance().shutdown();
            logger.info("Shutting Down CacheManager Finished");
        }catch (NullPointerException e){
            logger.info("CacheManager Not Initialized");
        }catch (Exception e){
            logger.error("Shutting Down CacheManager Failed", e);
        }
        try{
            logger.info("Shutting Down NotificationManager...");
            NotificationManager.getInstance().shutdown();
            logger.info("Shutting Down NotificationManager Finished");
        }catch (NullPointerException e){
            logger.info("CacheManager Not Initialized");
        }catch (Exception e){
            logger.error("Shutting Down NotificationManager Failed", e);
        }
        try{
            logger.info("Shutting Down SystemStats...");
            SystemStats.getInstance().stopAnalysis();
            logger.info("Shutting Down SystemStats Finished");
        }catch (NullPointerException e){
            logger.info("SystemStats Not Initialized");
        }catch (Exception e){
            logger.error("Shutting Down SystemStats Failed", e);
        }
    }
}
