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

package de.netbeacon.jstorage.server.socket.api.processing.action;

import de.netbeacon.jstorage.server.internal.cachemanager.CacheManager;
import de.netbeacon.jstorage.server.internal.cachemanager.objects.Cache;
import de.netbeacon.jstorage.server.internal.cachemanager.objects.CachedData;
import de.netbeacon.jstorage.server.internal.usermanager.object.DependentPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.socket.api.processing.APIProcessorResult;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Cache Action - Get Cached Data
 * <p>
 * --- Does --- <br>
 * Tries to get the selected data from the specified cache <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * cache, identifier, isvalid, isValidUntil, data if cached data is valid, else <br>
 * cache, identifier, isvalid as JSON <br>
 * --- Requirements --- <br>
 * path: cache/data <br>
 * action: get <br>
 * http_method: get <br>
 * login-mode: token <br>
 * payload: no <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.CacheAdmin, DependentPermission.CacheAdmin_Creator, DependentPermission.CacheAdmin_User, DependentPermission.CacheAccess_Modify, DependentPermission.CacheAccess_Read <br>
 * required_arguments: cache(String, cacheIdentifier), identifier(String, dataIdentifier) <br>
 * optional_arguments: <br>
 *
 * @author horstexplorer
 */
public class CacheAction_GetCachedData implements ProcessingAction {

    private APIProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new CacheAction_GetCachedData();
    }

    @Override
    public String getAction() {
        return "get";
    }

    @Override
    public void setup(User user, APIProcessorResult result, HashMap<String, String> args) {
        this.user = user;
        this.result = result;
        this.args = args;
    }

    @Override
    public boolean supportedHTTPMethod(String method) {
        return "get".equalsIgnoreCase(method);
    }

    @Override
    public List<String> requiredArguments() {
        return Arrays.asList("cache", "identifier");
    }

    @Override
    public boolean userHasPermission() {
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.CacheAdmin) ||
                user.hasDependentPermission(args.get("cache"), DependentPermission.CacheAdmin_Creator) ||
                user.hasDependentPermission(args.get("cache"), DependentPermission.CacheAdmin_User) ||
                user.hasDependentPermission(args.get("cache"), DependentPermission.CacheAccess_Modify) ||
                user.hasDependentPermission(args.get("cache"), DependentPermission.CacheAccess_Read);
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {
        Cache c = CacheManager.getInstance().getCache(args.get("cache"));
        CachedData d = c.getCachedData("identifier");
        JSONObject customResponseData = new JSONObject();
        if(d.isValid()){
            customResponseData
                    .put("cache", c.getIdentifier())
                    .put("identifier", d.getIdentifier())
                    .put("isvalid", d.isValid())
                    .put("isValidUntil", d.isValidUntil())
                    .put("data", d.getData());
        }else{
            customResponseData
                    .put("cache", c.getIdentifier())
                    .put("identifier", d.getIdentifier())
                    .put("isvalid", d.isValid());
        }
        // set result
        result.addResult(this.getDefaultResponse(customResponseData));
    }
}
