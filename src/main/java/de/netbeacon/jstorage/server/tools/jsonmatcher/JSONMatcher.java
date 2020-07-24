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

package de.netbeacon.jstorage.server.tools.jsonmatcher;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Set;

/**
 * Used to check whether a specific json structure is adhered to
 *
 * @author horstexplorer
 */
public class JSONMatcher {

    /**
     * Checks if the structure of test matches the requires structure of specification
     *
     * @param specification the predefined structure
     * @param test the object containing the data
     * @return boolean
     */
    public static boolean structureMatch(JSONObject specification, JSONObject test) {
        // get keys
        Set<String> specKeys = specification.keySet();
        Set<String> testKeys = test.keySet();
        // both contain different keys?
        if (!specKeys.containsAll(testKeys) || !testKeys.containsAll(specKeys)) {
            return false;
        }
        // both contain different values and types?
        for (String key : specKeys) {
            if (specification.get(key).getClass() != test.get(key).getClass()) {
                return false;
            }
            if (specification.get(key).getClass() == JSONObject.class) {
                if(!structureMatch(specification.getJSONObject(key), test.getJSONObject(key))){
                    return false;
                }
            } else if (specification.get(key).getClass() == JSONArray.class) {
                if(!structureMatch(specification.getJSONArray(key), test.getJSONArray(key))){
                    return false;
                }
            }
        }
        // ok
        return true;
    }

    /**
     * Checks if the structure of test matches the requires structure of specification
     *
     * @param specification the predefined structure
     * @param test the object containing the data
     * @return boolean
     */
    public static boolean structureMatch(JSONArray specification, JSONArray test) {
        if(specification.isEmpty()){
            return true;
        }
        for(Object o : test){
            if (specification.get(0).getClass() != o.getClass()) {
                return false;
            }
            if(specification.get(0).getClass() == JSONObject.class){
                if(!structureMatch(specification.getJSONObject(0), (JSONObject) o)){
                    return false;
                }
            }else if(specification.get(0).getClass() == JSONArray.class){
                if(!structureMatch(specification.getJSONArray(0), (JSONArray) o)){
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Forces the given data to match the specification format
     *
     * @param specification the predefined structure
     * @param data the object containing the data
     * @return JSONObject
     */
    public static JSONObject structureUpgrade(JSONObject specification, JSONObject data){
        Set<String> structSet = specification.keySet();
        JSONObject copy = new JSONObject(data.toString());
        // remove unnecessary structures
        for(String s : data.keySet()){
            if(!structSet.contains(s)){
                copy.remove(s);
            }
        }
        Set<String> copySet = copy.keySet();
        // check types for each key
        for(String key : structSet){
            // does it exist
            if(!copySet.contains(key)){
                copy.put(key, specification.get(key));
                continue;
            }
            // is it of the same class
            if(copy.get(key).getClass() != specification.get(key).getClass()){
                copy.put(key, specification.get(key));
                continue;
            }
            // if it is a json object or json array
            if(copy.get(key).getClass() == JSONObject.class){
                copy.put(key, structureUpgrade(specification.getJSONObject(key), copy.getJSONObject(key)));
            }else if(copy.get(key).getClass() == JSONArray.class){
                copy.put(key, structureUpgrade(specification.getJSONArray(key), copy.getJSONArray(key)));
            }
        }
        return copy;
    }

    /**
     * Forces the given data to match the specification format
     *
     * @param specification the predefined structure
     * @param data the object containing the data
     * @return JSONArray
     */
    public static JSONArray structureUpgrade(JSONArray specification, JSONArray data){
        if(specification.isEmpty()){
            return data;
        }
        JSONArray copy = new JSONArray(data.toString());
        for(int i = 0; i < copy.length(); i++){
            // check types
            if(specification.get(0).getClass() != copy.get(i).getClass()){
                copy.put(i, RemoveThis.class);
                continue;
            }
            // if it is a json object or json array
            if(copy.get(i).getClass() == JSONObject.class){
                copy.put(i, structureUpgrade(specification.getJSONObject(0), copy.getJSONObject(i)));
            }else if(copy.get(i).getClass() == JSONArray.class){
                copy.put(i, structureUpgrade(specification.getJSONArray(0), copy.getJSONArray(i)));
            }
        }
        // remove null
        for(int i = 0; i < copy.length(); i++){
            if(copy.get(i) == RemoveThis.class){
                copy.remove(i);
                i = -1; // restart the lööp
            }
        }
        return copy;
    }
}
