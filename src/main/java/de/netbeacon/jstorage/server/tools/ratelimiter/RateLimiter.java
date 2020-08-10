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

package de.netbeacon.jstorage.server.tools.ratelimiter;

import java.util.concurrent.TimeUnit;

/**
 * This class is used to calculate if an action has been performed x times within a given time interval
 *
 * @author horstexplorer
 */
public class RateLimiter {

    private final long nsWindowSize;
    private long filler;
    private long maxUsages;
    private long nsPerUsage;

    /**
     * This creates a new RateLimiter object
     *
     * @param refillUnit time unit in which the next value has to be interpreted
     * @param refillUnitNumbers number of timeunits it should take to fully refill the bucket
     */
    public RateLimiter(TimeUnit refillUnit, long refillUnitNumbers){
        nsWindowSize = refillUnit.toNanos(Math.abs(refillUnitNumbers));
    }

    /*                  GET                 */

    /**
     * Returns the setting on how many usages are allowed within each refill cycle
     *
     * @return long
     */
    public long getMaxUsages(){
        return maxUsages;
    }

    /**
     * Calculates an estimate on how many usages are probably left within the refill cycle
     *
     * @return long
     */
    public long getRemainingUsages(){
        long current = System.nanoTime();
        if(filler < current){
            filler = current;
        }
        long div = Math.max(current + nsWindowSize - filler, 0);
        return (div / nsPerUsage);
    }

    /**
     * Returns an estimated timestamp at which the bucket should be completely refilled
     *
     * @return long
     */
    public long getRefillTime(){
        return System.currentTimeMillis()+((nsWindowSize-(getRemainingUsages()*nsPerUsage))/1000000);
    }

    /*                  SET                 */

    /**
     * Used to set the number of usages within each refill cycle
     *
     * @param maxUsages long
     */
    public void setMaxUsages(long maxUsages){
        this.maxUsages = maxUsages;
        nsPerUsage = nsWindowSize / maxUsages;
    }

    /*                  CHECK                   */

    /**
     * Increases the usage by one, returns true if this usage fits into the limit
     *
     * This will count up until double of the limit is reached </br>
     *
     * @return boolean
     */
    public boolean takeNice(){
        long current = System.nanoTime();
        // lower limit
        if(filler < current){
            filler = current;
        }
        // add take to filler
        filler += nsPerUsage;
        // upper limit
        if(filler > current+(nsWindowSize*2)){
            filler = current+(nsWindowSize*2);
        }
        // check if filler fits inside the window
        return (current+nsWindowSize) >= filler;
    }

    /**
     * Increases the usage by one
     *
     * This will count up until double of the limit is reached </br>
     *
     * @throws RateLimitException if the usage wont fit into the limit
     */
    public void take() throws RateLimitException {
        long current = System.nanoTime();
        // lower limit
        if(filler < current){
            filler = current;
        }
        // add take to filler
        filler += nsPerUsage;
        // upper limit
        if(filler > current+(nsWindowSize*2)){
            filler = current+(nsWindowSize*2);
        }
        // check if filler fits inside the window
        if((current+nsWindowSize) < filler){
            throw new RateLimitException("Ratelimit Exceeded");
        }
    }

    /*              Exception                   */

    /**
     * Helper class for exceptions
     */
    public static class RateLimitException extends Exception {
        public RateLimitException(String msg){
            super(msg);
        }
    }
}