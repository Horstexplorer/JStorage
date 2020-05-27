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

/**
 * Represents the usage statistics of a specific object
 */
public class UsageStatistics {

    private final ConcurrentHashMap<Long, Usage> access = new ConcurrentHashMap<>();
    public enum Usage{
        any,
        get_success, get_failure,
        insert_success, insert_failure,
        update_success, update_failure,
        delete_success, delete_failure,
        acquire_success, acquire_failure
    };

    /**
     * Returns the number of 'uses' for a specific type within the last 10 minutes
     *
     * @param e DSMSEnum
     * @return long
     */
    public long getCountFor(Usage e){
        long oldest = System.nanoTime()-600000000000L;
        access.entrySet().stream().filter(x-> (x.getKey() <= oldest)).forEach(x->access.remove(x.getKey()));
        return access.entrySet().stream().filter(x -> (x.getKey() >= oldest && (x.getValue() == e || e == Usage.any))).count();
    }

    /**
     * Adds a 'use' for a specific type
     *
     * @param e DSMSEnum
     */
    public void add(Usage e){
        long current = System.nanoTime();
        access.put(current, e);
        // delete older datasets
        long finalCurrent = current-600000000000L;
        access.entrySet().stream().filter(x-> (x.getKey() <= finalCurrent)).forEach(x->access.remove(x.getKey()));
    }

}
