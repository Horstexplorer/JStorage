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

import de.netbeacon.jstorage.server.socket.api.APISocket;

import java.lang.management.ManagementFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Contains the current stats of the system
 *
 * @author horstexplorer
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
     *
     * @return SystemStats
     */
    public static SystemStats getInstance(){
        return getInstance(false);
    }

    /**
     * Used to get the instance of this class
     *
     * Can be used to initialize the class if this didnt happened yet </br>
     *
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
     *
     * @return Load
     */
    public Load getAPILoad(){
        return apiLoad;
    }

    /**
     * Used to get an enum indicating the system load
     *
     * @return Load
     */
    public Load getSystemLoad(){
        return systemLoad;
    }

    /**
     * Used to get the avg cpu load
     *
     * Returns -1 on error </br>
     *
     * @return long
     */
    public double getAVGLoad(){
        return ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
    }

    /**
     * Used to get the amount of total memory
     *
     * @return long
     */
    public double getTotalMemory(){
        return Runtime.getRuntime().totalMemory();
    }

    /**
     * Used to get the amount of free memory
     *
     * @return long
     */
    public long getFreeMemory(){
        return Runtime.getRuntime().freeMemory();
    }

    /**
     * Used to get the number of threads within the core pool
     *
     * Returns -1 if the executor is null </br>
     *
     * @return int
     */
    public int getAPICorePoolSize(){
        return (APISocket.getInstance().getExecutor() != null) ? APISocket.getInstance().getExecutor().getCorePoolSize() : -1;
    }

    /**
     * Used to get the maximum number of threads
     *
     * Returns -1 if the executor is null </br>
     *
     * @return int
     */
    public int getAPIMaxPoolSize(){
        return (APISocket.getInstance().getExecutor() != null) ? APISocket.getInstance().getExecutor().getMaxPoolSize() : -1;
    }

    /**
     * Used to get the current number of threads
     *
     * Returns -1 if the executor is null </br>
     *
     * @return int
     */
    public int getAPICurrentPoolSize(){
        return (APISocket.getInstance().getExecutor() != null) ? APISocket.getInstance().getExecutor().getCurrentPoolSize() : -1;
    }

    /**
     * Used to get the number of currently active threads
     *
     * Returns -1 if the executor is null </br>
     *
     * @return int
     */
    public int getAPIActiveThreads(){
        return (APISocket.getInstance().getExecutor() != null) ? APISocket.getInstance().getExecutor().getActiveThreads() : -1;
    }

    /**
     * Used to get the remaining capacity of the queue
     *
     * Returns -1 if the executor is null </br>
     *
     * @return int
     */
    public int getAPIQueueRemainingCapacity(){
        return (APISocket.getInstance().getExecutor() != null) ? APISocket.getInstance().getExecutor().getRemainingQueueCapacity() : -1;
    }

    /**
     * Used to get the max capacity if the queue
     *
     * Returns -1 if the executor is null </br>
     *
     * @return int
     */
    public int getAPIQueueMaxCapacity(){
        return (APISocket.getInstance().getExecutor() != null) ? APISocket.getInstance().getExecutor().getMaxQueueCapacity() : -1;
    }

    /**
     * Creates and starts the analysis task
     */
    public void startAnalysis(){
        task = scheduledExecutorService.scheduleAtFixedRate(()->{
            // system load
            int sl = 0;
            sl += Math.max(ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage()*10, 0);
            sl += Math.max((1-(Runtime.getRuntime().freeMemory()/(double)Runtime.getRuntime().totalMemory()))*10, 0);
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
            int al = 0;
            al += Math.max(1-(getAPIQueueRemainingCapacity()/(double)getAPIQueueMaxCapacity())*10, 0);
            al += Math.max(1-(getAPIActiveThreads()/(double)getAPIMaxPoolSize())*10, 0);
            al += Math.max((getAPICurrentPoolSize()-getAPICorePoolSize())/(double)(getAPIMaxPoolSize()-getAPICorePoolSize())*5, 0);
            if(al < 3){
                apiLoad = Load.Idle;
            }else if(al < 7){
                apiLoad = Load.Low;
            }else if(al < 13)
                apiLoad = Load.Medium;
            else{
                apiLoad = Load.High;
            }
        }, 1, 1, TimeUnit.SECONDS);
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
