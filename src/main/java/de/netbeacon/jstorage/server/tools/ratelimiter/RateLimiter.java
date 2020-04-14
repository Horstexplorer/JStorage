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

public class RateLimiter {

    private final long msWindowSize;
    private long filler;
    private long maxUsages;
    private long msPerUsage;

    /**
     * This creates a new RateLimiter object
     *
     * @param refillUnit time unit in which the next value has to be interpreted
     * @param refillUnitNumbers number of timeunits it should take to fully refill the bucket
     */
    public RateLimiter(TimeUnit refillUnit, long refillUnitNumbers){
        msWindowSize = refillUnit.toMillis(Math.abs(refillUnitNumbers));
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
        long current = System.currentTimeMillis();
        if(filler < current){
            filler = current;
        }
        long div = Math.max(current+msWindowSize - filler, 0);
        return  (int) (div / msPerUsage);
    }

    /*                  SET                 */

    /**
     * Used to set the number of usages within each refill cycle
     *
     * @param maxUsages long
     */
    public void setMaxUsages(long maxUsages){
        this.maxUsages = maxUsages;
        msPerUsage = msWindowSize / maxUsages;
    }

    /*                  CHECK                   */

    /**
     * Increases the usage by one, returns true if this usage fits into the limit
     * <p>
     * This will count up until double of the limit is reached
     *
     * @return boolean
     */
    public boolean takeNice(){
        long current = System.currentTimeMillis();
        // lower limit
        if(filler < current){
            filler = current;
        }
        // add take to filler
        filler += msPerUsage;
        // upper limit
        if(filler > current+(msWindowSize*2)){
            filler = current+(msWindowSize*2);
        }
        // check if filler fits inside the window
        return (current+msWindowSize) >= filler;
    }

    /**
     * Increases the usage by one
     * <p>
     * This will count up until double of the limit is reached
     *
     * @throws RateLimitException if the usage wont fit into the limit
     */
    public void take() throws RateLimitException {
        long current = System.currentTimeMillis();
        // lower limit
        if(filler < current){
            filler = current;
        }
        // add take to filler
        filler += msPerUsage;
        // upper limit
        if(filler > current+(msWindowSize*2)){
            filler = current+(msWindowSize*2);
        }
        // check if filler fits inside the window
        if((current+msWindowSize) < filler){
            throw new RateLimitException("Ratelimit Exceeded");
        }
    }

    /*              Exception                   */

    public static class RateLimitException extends Exception {
        public RateLimitException(String msg){
            super(msg);
        }
    }
}
