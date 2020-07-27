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
import de.netbeacon.jstorage.server.socket.notification.NotificationSocketHandler;
import de.netbeacon.jstorage.server.tools.exceptions.SetupException;
import de.netbeacon.jstorage.server.tools.exceptions.ShutdownException;
import de.netbeacon.jstorage.server.tools.nullc.Null;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class NotificationManager {

    private static NotificationManager instance;

    private Thread notificationDispatcher;
    private final BlockingQueue<DataNotification> notificationQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<NotificationSocketHandler, Class<Null>> registeredClients = new ConcurrentHashMap<>(); // abused as list

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


    public void notify(DataNotification notification){

    }

    public void setup() throws SetupException{

    }


    public void shutdown() throws ShutdownException{

    }
}
