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

package de.netbeacon.jstorage;

import de.netbeacon.jstorage.server.internal.cachemanager.CacheManager;
import de.netbeacon.jstorage.server.internal.datamanager.DataManager;
import de.netbeacon.jstorage.server.internal.notificationmanager.NotificationManager;
import de.netbeacon.jstorage.server.internal.usermanager.UserManager;
import de.netbeacon.jstorage.server.socket.api.APISocket;
import de.netbeacon.jstorage.server.socket.hello.HelloSocket;
import de.netbeacon.jstorage.server.socket.notification.NotificationSocket;
import de.netbeacon.jstorage.server.tools.exceptions.SetupException;
import de.netbeacon.jstorage.server.tools.info.Info;
import de.netbeacon.jstorage.server.tools.ipban.IPBanManager;
import de.netbeacon.jstorage.server.tools.meta.SystemStats;
import de.netbeacon.jstorage.server.tools.shutdown.ShutdownHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * This class contains the default execution functions
 *
 * @author horstexplorer
 */
public class JStorage {

    private static final Logger logger = LoggerFactory.getLogger(JStorage.class);
    /**
     * Main.
     *
     * @param args the args
     */
    public static void main(String... args){
        // say hello
        System.out.println("\n" +
                " _____ _______ _______  _____   ______ _______  ______ _______\n" +
                "   |   |______    |    |     | |_____/ |_____| |  ____ |______\n" +
                " __|   ______|    |    |_____| |    \\_ |     | |_____| |______\n" +
                "                                                               " +
                "");
        System.out.println("v "+ Info.VERSION);
        // read args
        HashMap<String, String> arguments = new HashMap<>();
        for(String s : args){
            // args should be in the format: -arg:value
            try{
                arguments.put(s.substring(1, s.indexOf(":")).toLowerCase(), s.substring(s.indexOf(":")+1));
            }catch (Exception e){
                logger.error("Invalid Argument Format: "+s);
            }
        }
        // check if encryption should be set up
        boolean runEncryptSetup = false;
        if(arguments.containsKey("encryptsetup")){
            runEncryptSetup = Boolean.parseBoolean(arguments.get("encryptsetup"));
        }
        // select mode
        String mode = "";
        if(arguments.containsKey("mode")){
            mode = arguments.get("mode");
        }

        switch(mode){
            default:
                System.out.println("Mode...Default");
                modeDefault(runEncryptSetup);
                break;
        }
    }

    private static void modeDefault(boolean runEncryptSetup){
        try{
            try{
                logger.info("Initializing ShutdownHook...");
                new ShutdownHook();
                logger.info("Initializing ShutdownHook finished");
            }catch (Exception e){
                logger.info("Initializing ShutdownHook failed");
                throw e;
            } // should not throw
            try{
                logger.info("Initializing DataManager...");
                DataManager.getInstance(true).setup(runEncryptSetup);
                logger.info("Initializing DataManager finished");
            }catch (SetupException e){
                logger.info("Initializing DataManager failed");
                throw e;
            }
            try{
                logger.info("Initializing CacheManager...");
                CacheManager.getInstance(true).setup();
                logger.info("Initializing CacheManager finished");
            }catch (SetupException e){
                logger.info("Initializing CacheManager failed");
                throw e;
            }
            try{
                logger.info("Initializing UserManager...");
                UserManager.getInstance(true).setup();
                logger.info("Initializing UserManager finished");
            }catch (SetupException e){
                logger.info("Initializing UserManager failed");
                throw e;
            }
            try{
                logger.info("Initializing IPBanManager...");
                IPBanManager.getInstance(true).setup();
                logger.info("Initializing IPBanManager finished");
            }catch (SetupException e){
                logger.info("Initializing IPBanManager failed");
                throw e;
            }
            try{
                logger.info("Initializing NotificationManager...");
                NotificationManager.getInstance(true).setup();
                logger.info("Initializing Notification finished");
            }catch (Exception e){
                logger.info("Initializing Notification failed");
                throw e;
            }
            try{
                logger.info("Initializing HelloSocket...");
                HelloSocket.getInstance(true).start();
                logger.info("Initializing HelloSocket finished");
            }catch (Exception e){
                logger.info("Initializing HelloSocket failed");
                throw e;
            }
            try{
                logger.info("Initializing APISocket...");
                APISocket.getInstance(true).start();
                logger.info("Initializing APISocket finished");
            }catch (Exception e){
                logger.info("Initializing APISocket failed");
                throw e;
            }
            try{
                logger.info("Initializing NotificationSocket...");
                NotificationSocket.getInstance(true).start();
                logger.info("Initializing NotificationSocket finished");
            }catch (Exception e){
                logger.info("Initializing NotificationSocket failed");
                throw e;
            }
            try{
                logger.info("Initializing SystemStats...");
                SystemStats.getInstance(true).startAnalysis();
                logger.info("Initializing SystemStats finished");
            }catch (Exception e){
                logger.info("Initializing SystemStats failed");
                throw e;
            }
        }catch (SetupException e){
            logger.error("Error Starting Components. Exiting.", e);
            System.exit(-1);
        }
    }
}
