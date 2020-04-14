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

import de.netbeacon.jstorage.server.api.socket.APISocket;
import de.netbeacon.jstorage.server.internal.cachemanager.CacheManager;
import de.netbeacon.jstorage.server.internal.datamanager.DataManager;
import de.netbeacon.jstorage.server.internal.usermanager.UserManager;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class contains the shutdown hook which is supposed to shutdown everything nicely, at least for normal shutdowns
 *
 * @author horstexplorer
 */
public class ShutdownHook {

    private static final AtomicBoolean inserted = new AtomicBoolean(false);

    /**
     * Insert this as ShutdownHook
     * <p>
     * Will only add itself once
     */
    public ShutdownHook(){
        if(!inserted.get()){
            inserted.set(true);
            Runtime.getRuntime().addShutdownHook(new Thread()
            {
                public void run()
                {
                    shutdownNow();
                }
            });
        }
    }

    private void shutdownNow(){
        System.out.println("! ShutdownHook Executed !");
        try{System.out.print("APISocket..."); APISocket.shutdown(); System.out.println("ok");}catch (Exception e){System.out.println(e.getCause()+"   "+e.getMessage());}
        try{System.out.print("DataManager..."); DataManager.shutdown(); System.out.println("ok");}catch (Exception e){System.out.println(e.getCause()+"   "+e.getMessage());}
        try{System.out.print("CacheManager..."); CacheManager.shutdown(); System.out.println("ok");}catch (Exception e){System.out.println(e.getCause()+"   "+e.getMessage());}
        try{System.out.print("UserManager..."); UserManager.shutdown(); System.out.println("ok");}catch (Exception e){System.out.println(e.getCause()+"   "+e.getMessage());}
    }
}
