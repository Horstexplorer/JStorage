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
 * This set contains all global permissions a user may have
 *
 * @author horstexplorer
 */
public enum GlobalPermission {

    /**
     * Admin global permission.
     */
    Admin,                      // everything

    /**
     * User admin global permission.
     */
    UserAdmin,                  // manage users
    /**
     * User admin self global permission.
     */
    UserAdmin_Self,             // manage high priority settings ur own
    /**
     * User default self global permission.
     */
    UserDefault_Self,           // manage low priority settings on ur own

    /**
     * Db admin global permission.
     */
    DBAdmin,                    // manage all dbs
    /**
     * Cache admin global permission.
     */
    CacheAdmin,                 // manage all caches

    /**
     * Allows to use the notification socket
     */
    UseNotifications,

    /**
     * Used to let the user access the advanced statistics on the hello socket
     */
    ViewAdvancedStatistics;

    /**
     * Used to get the enum matching the string
     * <p>
     * Should be used instead of the valueOf method
     * returns null if no matching enum has been found
     *
     * @param name identifier of the enum
     * @return DependentPermission or Null
     */
    public static GlobalPermission getByValue(String name){
        return Arrays.stream(GlobalPermission.values()).filter(f->f.toString().equalsIgnoreCase(name)).findFirst().orElse(null);
    }

}
