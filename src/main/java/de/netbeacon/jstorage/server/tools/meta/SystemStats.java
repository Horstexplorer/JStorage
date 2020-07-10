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

package de.netbeacon.jstorage.server.tools.meta;

import de.netbeacon.jstorage.server.api.socket.APISocket;

import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Contains the current stats of the system
 */
public class SystemStats {

    public enum Load{
        Unknown,
        Idle,
        Low,
        Medium,
        High
    }


    private static SystemStats instance;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> task;
    private Load apiLoad = Load.Unknown;
    private Load systemLoad = Load.Unknown;

    /**
     * Used to create an instance of this class
     */
    private SystemStats(){}

    /**
     * Used to get the instance of this class without forcing initialization
     * @return SystemStats
     */
    public static SystemStats getInstance(){
        return getInstance(false);
    }

    /**
     * Used to get the instance of this class
     * <p>
     * Can be used to initialize the class if this didnt happened yet
     * @param initializeIfNeeded boolean
     * @return SystemStats
     */
    public static SystemStats getInstance(boolean initializeIfNeeded){
        if(instance == null && initializeIfNeeded){
            instance = new SystemStats();
        }
        return instance;
    }


    /**
     * Used to get an enum indicating the api load
     * @return Load
     */
    public Load getAPILoad(){
        return apiLoad;
    }

    /**
     * Used to get an enum indicating the system load
     * @return Load
     */
    public Load getSystemLoad(){
        return systemLoad;
    }

    /**
     * Used to get the avg cpu load
     * @return long
     */
    public double getAVGLoad(){
        return ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
    }

    /**
     * Used to get the amount of total memory
     * @return long
     */
    public double getTotalMemory(){
        return Runtime.getRuntime().totalMemory();
    }

    /**
     * Used to get the amount of free memory
     * @return long
     */
    public long getFreeMemory(){
        return Runtime.getRuntime().freeMemory();
    }


    /**
     * Creates and starts the analysis task
     */
    public void startAnalysis(){
        task = scheduledExecutorService.scheduleAtFixedRate(()->{
            // system load
            int sl = 0;
            sl += ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage()*10;
            sl += (1-(Runtime.getRuntime().freeMemory()/Runtime.getRuntime().totalMemory()))*10;
            if(sl < 2){
                systemLoad = Load.Idle;
            }else if(sl < 5){
                systemLoad = Load.Low;
            }else if(sl < 10)
                systemLoad = Load.Medium;
            else{
                systemLoad = Load.High;
            }
            // api load
            int al = APISocket.getInstance().getLoadValue();
            if(al < 2){
                apiLoad = Load.Idle;
            }else if(al < 5){
                apiLoad = Load.Low;
            }else if(al < 10)
                apiLoad = Load.Medium;
            else{
                apiLoad = Load.High;
            }
        }, 1, 30, TimeUnit.SECONDS);
    }

    /**
     * Stops the analysis task & resets it
     */
    public void stopAnalysis(){
        if(task != null){
            task.cancel(true);
            task = null;
        }
    }

}
