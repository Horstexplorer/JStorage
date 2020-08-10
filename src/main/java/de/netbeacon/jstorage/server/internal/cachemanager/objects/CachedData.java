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

package de.netbeacon.jstorage.server.internal.cachemanager.objects;

import org.json.JSONObject;

/**
 * Contains the data which should be cached
 *
 * @author horstexplorer
 */
public class CachedData {

    private final String cacheIdentifier;
    private final String identifier;
    private final JSONObject data;
    private long validUntil = -1L;

    /**
     * Creates a new CachedData object
     *
     * @param cacheIdentifier identifier of the parent cache; will be converted to lower case
     * @param identifier      identifier of the object, this one should be unique; will be converted to lower case
     * @param data            the data which should be stored as json
     */
    public CachedData(String cacheIdentifier, String identifier, JSONObject data){
        this.cacheIdentifier = cacheIdentifier.toLowerCase();
        this.identifier = identifier.toLowerCase();
        this.data = data;
    }

    /*                  OBJECT                  */

    /**
     * Returns the identifier of the parent cache
     *
     * @return String string
     */
    public String getCacheIdentifier(){ return cacheIdentifier; }

    /**
     * Returns the identifier of this object
     *
     * @return String string
     */
    public String getIdentifier(){ return identifier; }

    /**
     * Returns the data of this object
     *
     * @return JSONObject data
     */
    public JSONObject getData() { return data; }

    /**
     * Used to set the duration until expiration of this object
     *
     * @param durationInSeconds duration in seconds
     */
    public void setValidForDuration(long durationInSeconds){
        if(durationInSeconds < 0){
            validUntil = -1L; // should be valid without expiration
        }else{
            validUntil = System.currentTimeMillis()+1000*durationInSeconds;
        }
    }

    /*                  VALIDATION                  */

    /**
     * Returns the timestamp to which this object is valid and should not be replaced / modified
     *
     * @return long long
     */
    public long isValidUntil(){
        return validUntil;
    }

    /**
     * Used to check if this object is still valid
     *
     * Returns true if it is or if there is no expiration time set (expressed as negative values) </br>
     *
     * @return boolean boolean
     */
    public boolean isValid(){
        if(validUntil >= System.currentTimeMillis()){
            return true;
        }else return validUntil < 0;
    }

    /*                  MISC                    */

    /**
     * Used to export this object for storage purposes
     *
     * @return JSONObject json object
     */
    public JSONObject export(){
        return new JSONObject()
                .put("identifier", identifier)
                .put("cacheIdentifier", cacheIdentifier)
                .put("validUntil", validUntil)
                .put("data", data);
    }
}
