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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents the usage statistics of a specific object
 *
 * @author horstexplorer
 */
public class UsageStatistics {

    public enum Usage{
        any,
        get_success, get_failure,
        insert_success, insert_failure,
        update_success, update_failure,
        delete_success, delete_failure,
        acquire_success, acquire_failure
    }

    private static final ScheduledExecutorService ses = Executors.newScheduledThreadPool(4);
    private final ConcurrentHashMap<Usage, AtomicLong> map = new ConcurrentHashMap<>();

    /**
     * Creates a new instance of this class
     */
    public UsageStatistics(){
        for(Usage u : Usage.values()){
            map.put(u, new AtomicLong(0));
        }
    }


    /**
     * Returns the number of 'uses' for a specific type within the last 10 minutes
     *
     * @param e DSMSEnum
     * @return long
     */
    public long getCountFor(Usage e){
        return map.get(e).get();
    }

    /**
     * Adds a 'use' for a specific type
     *
     * @param e DSMSEnum
     */
    public void add(Usage e){
        map.get(e).incrementAndGet();
        ses.schedule(()->{map.get(e).decrementAndGet();}, 10, TimeUnit.MINUTES);
    }

}
