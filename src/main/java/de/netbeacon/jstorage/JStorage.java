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

import de.netbeacon.jstorage.server.api.socket.APISocket;
import de.netbeacon.jstorage.server.internal.cachemanager.CacheManager;
import de.netbeacon.jstorage.server.internal.datamanager.DataManager;
import de.netbeacon.jstorage.server.internal.usermanager.UserManager;
import de.netbeacon.jstorage.server.tools.exceptions.SetupException;
import de.netbeacon.jstorage.server.tools.info.Info;
import de.netbeacon.jstorage.server.tools.shutdown.ShutdownHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Objects;
import java.util.Scanner;

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
        // check if the jar is called from the same level directory
        System.out.print("DirCheck...");
        if(arguments.containsKey("dircheck") && "disable".equalsIgnoreCase(arguments.get("dircheck"))){
            System.out.println("ok (disabled)");
        }else{
            if(System.getProperty("user.dir").equalsIgnoreCase(new File(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource(".")).getPath()).getAbsolutePath())){
                System.out.println("ok");
            }else{
                System.out.println("failed");
                System.err.println("Working Dir ("+System.getProperty("user.dir")+")Does Not Match Jar Dir ("+new File(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource(".")).getPath()).getAbsolutePath()+")");
                System.exit(-1);
            }
        }
        // check if encryption should be set up
        boolean runEncryptSetup = false;
        if(arguments.containsKey("encryptSetup")){
            runEncryptSetup = Boolean.parseBoolean(arguments.get("encryptSetup"));
        }
        // should print out version and build
        System.out.println("Version: "+Info.VERSION);
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
            try{ System.out.print("ShutdownHook..."); new ShutdownHook(); System.out.println("ok"); }catch (Exception e){System.out.println("error"); throw e;} // should not throw
            try{ System.out.print("DataManager..."); new DataManager(runEncryptSetup); System.out.println("ok"); }catch (SetupException e){System.out.println("error"); throw e;}
            try{ System.out.print("CacheManager..."); new CacheManager(); System.out.println("ok"); }catch (SetupException e){System.out.println("error"); throw e;}
            try{ System.out.print("UserManager..."); new UserManager(); System.out.println("ok"); }catch (SetupException e){System.out.println("error"); throw e;}
            APISocket.start();
        }catch (SetupException e){
            logger.error("Error Starting Components", e);
            System.exit(0);
        }
        input();
    }

    private static void input(){
        Scanner scanner = new Scanner(System.in);
        while(true){
            String line = scanner.nextLine();
            if(line.toLowerCase().equals("exit")){
                break;
            }
        }
        System.exit(0);
    }

}
