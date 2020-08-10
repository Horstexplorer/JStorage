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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Can be used to execute a specific task after a given amount of time
 */
public class TimeoutExecutor {

    private static final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> timeoutTask;
    private final Runnable runnable;

    /**
     * Creates a new instance of this class
     *
     * @param runnable the runnable which will be executed when the time is up
     */
    public TimeoutExecutor(Runnable runnable){
        this.runnable = runnable;
    }

    /**
     * Used to start a timeout for the connection
     * <br>
     * This will close the connection after a given time if not stopped
     * If the task is already running it will be canceled and restarted
     *
     * @param timeout timeout in ms
     */
    public void start(long timeout){
        if(timeoutTask != null){
            timeoutTask.cancel(true);
        }
        timeoutTask = ses.schedule(runnable, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Used to stop the timeout task
     */
    public void stop(){
        if(timeoutTask != null){
            timeoutTask.cancel(true);
        }
    }
}
