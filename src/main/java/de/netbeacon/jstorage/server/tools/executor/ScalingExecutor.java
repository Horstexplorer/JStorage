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

package de.netbeacon.jstorage.server.tools.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Used to handle executions in a fast scaling way
 */
public class ScalingExecutor {

    private final ThreadPoolExecutor baseExecutor;
    private final ThreadPoolExecutor scalingExecutor;
    private final ArrayBlockingQueue<Runnable> taskQueue;

    /**
     * Creates a new instance of this class
     *
     * @param baseThreads number of threads which should be always available
     * @param additionalThreads number of threads which might be additional available
     * @param maxWaitingTasks number of tasks that can wait if there is no free thread available
     * @param keepAliveTime time in timeUnit until an additional thread is stopped after being idle for this long
     * @param timeUnit unit of keepAliveTime
     */
    public ScalingExecutor(int baseThreads, int additionalThreads, int maxWaitingTasks, int keepAliveTime, TimeUnit timeUnit){
        taskQueue = new ArrayBlockingQueue<>(maxWaitingTasks);
        this.baseExecutor = new ThreadPoolExecutor(baseThreads,baseThreads, keepAliveTime, timeUnit, new ArrayBlockingQueue<>(1));
        this.baseExecutor.prestartAllCoreThreads();
        this.scalingExecutor = new ThreadPoolExecutor(additionalThreads, additionalThreads, keepAliveTime, timeUnit, taskQueue);
        this.scalingExecutor.allowCoreThreadTimeOut(true);
    }


    /**
     * Execute a new runnable
     * @param runnable Runnable
     * @throws RejectedExecutionException if the queue is full
     */
    public void execute(Runnable runnable) throws RejectedExecutionException{
        try{
            baseExecutor.execute(runnable);
        }catch (RejectedExecutionException e){
            scalingExecutor.execute(runnable); // this might throw
        }
    }

    /**
     * Returns the number of threads always available
     * @return int
     */
    public int getCorePoolSize(){
        return baseExecutor.getCorePoolSize();
    }

    /**
     * Returns the number of threads available at most
     * @return int
     */
    public int getMaxPoolSize(){
        return baseExecutor.getCorePoolSize()+scalingExecutor.getCorePoolSize();
    }

    /**
     * Returns the current number of threads used in the threadpool
     * @return int
     */
    public int getCurrentPoolSize(){
        return baseExecutor.getPoolSize()+scalingExecutor.getPoolSize();
    }

    /**
     * Used to get the remaining capacity of the queue
     * @return int
     */
    public int getRemainingQueueCapacity(){
        return taskQueue.remainingCapacity();
    }

    /**
     * Used to get the maximum capacity of the queue
     * @return int
     */
    public int getMaxQueueCapacity(){
        return taskQueue.remainingCapacity()+taskQueue.size();
    }

    /**
     * Used to get the number of currently active threads
     * @return int
     */
    public int getActiveThreads(){
        return baseExecutor.getActiveCount()+scalingExecutor.getActiveCount();
    }

    /**
     * Used to shutdown
     */
    public void shutdown(){
        scalingExecutor.shutdown();
        baseExecutor.shutdown();
    }

    /**
     * Used to shutdown now
     * @return List<Runnable>
     * @throws SecurityException on Exception
     */
    public List<Runnable> shutdownNow() throws SecurityException{
        List<Runnable> list = new ArrayList<>();
        list.addAll(scalingExecutor.shutdownNow());
        list.addAll(baseExecutor.shutdownNow());
        return list;
    }

}
