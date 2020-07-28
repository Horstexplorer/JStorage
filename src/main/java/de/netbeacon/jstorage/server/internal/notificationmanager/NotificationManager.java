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

package de.netbeacon.jstorage.server.internal.notificationmanager;

import de.netbeacon.jstorage.server.internal.notificationmanager.objects.DataNotification;
import de.netbeacon.jstorage.server.socket.notification.object.NotificationListener;
import de.netbeacon.jstorage.server.tools.exceptions.SetupException;
import de.netbeacon.jstorage.server.tools.exceptions.ShutdownException;
import de.netbeacon.jstorage.server.tools.nullc.Null;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class takes care of receiving and handling incoming and outgoing notifications
 */
public class NotificationManager {

    private static NotificationManager instance;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private ExecutorService notificationDispatcher;
    private ScheduledExecutorService heartbeatService;
    private final BlockingQueue<DataNotification> notificationQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<NotificationListener, Null> registeredClients = new ConcurrentHashMap<>(); // abused as list

    /**
     * Used to create an instance of this class
     */
    private NotificationManager(){}

    /**
     * Used to get the instance of this class without forcing initialization
     * @return DataManager
     */
    public static NotificationManager getInstance(){
        return getInstance(false);
    }

    /**
     * Used to get the instance of this class
     * <p>
     * Can be used to initialize the class if this didnt happened yet
     * @param initializeIfNeeded boolean
     * @return DataManager
     */
    public static NotificationManager getInstance(boolean initializeIfNeeded){
        if((instance == null && initializeIfNeeded)){
            instance = new NotificationManager();
        }
        return instance;
    }

    /**
     * Used to add notifications
     * @param notification notification
     */
    public void notify(DataNotification notification){
        if(isRunning.get()){
            notificationQueue.add(notification);
        }
    }

    /**
     * Added function to register & deregister listeners
     * @param notificationListener the listener
     * @param mode true to register
     */
    public void register(NotificationListener notificationListener, boolean mode){
        if(mode){
            registeredClients.put(notificationListener, Null.getInstance());
        }else{
            registeredClients.remove(notificationListener);
        }
    }

    /**
     * Used to set up the required worker
     * @throws SetupException on exception
     */
    public void setup() throws SetupException{
        notificationDispatcher = Executors.newSingleThreadExecutor();
        heartbeatService = Executors.newSingleThreadScheduledExecutor();
        notificationDispatcher.submit(()->{
            isRunning.set(true);
            Logger logger = LoggerFactory.getLogger("NotificationDispatcher");
            try{
                while(true){
                    try{
                        DataNotification dn = notificationQueue.take();
                        for(Map.Entry<NotificationListener, Null> entry : registeredClients.entrySet()){
                            entry.getKey().offerNotification(dn);
                        }
                    }catch (InterruptedException e){
                        throw e;
                    }catch (Exception ignore){}
                }
            }catch (InterruptedException e){
                logger.warn("Exiting Notification Dispatcher Due To Interrupt");
            }finally {
               isRunning.set(false);
            }
        });
        heartbeatService.scheduleAtFixedRate(()->{
            Logger logger = LoggerFactory.getLogger("NotificationHeartbeatDispatcher");
            DataNotification heartbeat = new DataNotification(null, null, null, null, null, DataNotification.Content.heartbeat);
            try{
                for(Map.Entry<NotificationListener, Null> entry : registeredClients.entrySet()){
                    try {
                        entry.getKey().offerNotification(heartbeat);
                    } catch (InterruptedException e) {
                        throw e;
                    }catch (Exception ignore){}
                }
            }catch (InterruptedException e){
                logger.warn("Exiting Notification Heartbeat Dispatcher Due To Interrupt");
            }
        },2, 2, TimeUnit.SECONDS);
    }

    /**
     * Used to shut down execution
     * @throws ShutdownException
     */
    public void shutdown() throws ShutdownException{
        if(notificationDispatcher != null){
            notificationDispatcher.shutdownNow();
        }
        if(heartbeatService != null){
            heartbeatService.shutdownNow();
        }
        instance = null;
        notificationQueue.clear();
        registeredClients.clear();
    }
}
