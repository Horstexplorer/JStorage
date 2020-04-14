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

package de.netbeacon.jstorage.server.internal.usermanager.object;

import java.util.Arrays;

/**
 * This set contains all database dependent permissions a user may have
 *
 * @author horstexplorer
 */
public enum DependentPermission {

    /**
     * Db admin creator dependent permission.
     */
    DBAdmin_Creator,            // manage created db, can add user to allowed or admin_user
    /**
     * Db admin user dependent permission.
     */
    DBAdmin_User,               // manage allowed db, can add user to allowed
    /**
     * Db access modify dependent permission.
     */
    DBAccess_Modify,            // can access db, no restrictions
    /**
     * Db access read dependent permission.
     */
    DBAccess_Read,              // can only read from a db

    /**
     * Cache admin creator dependent permission.
     */
    CacheAdmin_Creator,
    /**
     * Cache admin user dependent permission.
     */
    CacheAdmin_User,
    /**
     * Cache access modify dependent permission.
     */
    CacheAccess_Modify,         // can access a cache, no restrictions
    /**
     * Cache access read dependent permission.
     */
    CacheAccess_Read;           // can only read from a cache

    /**
     * Used to get the enum matching the string
     * <p>
     * Should be used instead of the valueOf method
     * returns null if no matching enum has been found
     *
     * @param name identifier of the enum
     * @return DependentPermission or Null
     */
    public static DependentPermission getByValue(String name){
        return Arrays.stream(DependentPermission.values()).filter(gp->gp.toString().equalsIgnoreCase(name)).findFirst().orElse(null);
    }

}