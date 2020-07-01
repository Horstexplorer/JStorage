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
                for(Object o : test.getJSONArray(key)){
                    if (specification.getJSONArray(key).get(0).getClass() != o.getClass()) {
                        return false;
                    }
                    if(specification.getJSONArray(key).get(0).getClass() == JSONObject.class){
                        if(!structureMatch(specification.getJSONArray(key).getJSONObject(0), (JSONObject) o)){
                            return false;
                        }
                    }
                }
            }
        }
        // ok
        return true;
    }
}
