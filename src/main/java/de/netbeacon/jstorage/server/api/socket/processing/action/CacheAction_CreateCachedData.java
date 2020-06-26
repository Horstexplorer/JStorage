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

package de.netbeacon.jstorage.server.api.socket.processing.action;

import de.netbeacon.jstorage.server.api.socket.processing.HTTPProcessorResult;
import de.netbeacon.jstorage.server.internal.cachemanager.CacheManager;
import de.netbeacon.jstorage.server.internal.cachemanager.objects.Cache;
import de.netbeacon.jstorage.server.internal.cachemanager.objects.CachedData;
import de.netbeacon.jstorage.server.internal.usermanager.object.DependentPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Cache Action - Create Cached Data
 * <p>
 * --- Does --- <br>
 * Tries to create a specific dataset within the selected cache <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * cache, cachedData - identifier, validUntil <br>
 * --- Requirements --- <br>
 * path: cache/data <br>
 * action: create <br>
 * http_method: put <br>
 * login-mode: token <br>
 * payload: yes - dataset (undefined structure) <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.CacheAdmin, DependentPermission.CacheAdmin_Creator, CacheAdmin_User, CacheAccess_Modify <br>
 * required_arguments: cache(String, cacheIdentifier), identifier(String, dataIdentifier) <br>
 * optional_arguments: duration(int, seconds until the data should be invalid) <br>
 *
 * @author horstexplorer
 */
public class CacheAction_CreateCachedData implements ProcessingAction {

    private HTTPProcessorResult result;
    private HashMap<String, String> args;
    private User user;
    private JSONObject data;

    @Override
    public ProcessingAction createNewInstance() {
        return new CacheAction_CreateCachedData();
    }

    @Override
    public String getAction() {
        return "create";
    }

    @Override
    public void setup(User user, HTTPProcessorResult result, HashMap<String, String> args) {
        this.user = user;
        this.result = result;
        this.args = args;
    }

    @Override
    public boolean supportedHTTPMethod(String method) {
        return "put".equalsIgnoreCase(method);
    }

    @Override
    public List<String> requiredArguments() {
        return Arrays.asList("cache", "identifier");
    }

    @Override
    public boolean requiresData(){
        return true;
    }

    @Override
    public void setPayload(JSONObject payload){
        this.data = payload;
    }

    @Override
    public boolean userHasPermission() {
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.CacheAdmin) ||
                user.hasDependentPermission(args.get("cache"), DependentPermission.CacheAdmin_Creator) ||
                user.hasDependentPermission(args.get("cache"), DependentPermission.CacheAdmin_User) ||
                user.hasDependentPermission(args.get("cache"), DependentPermission.CacheAccess_Modify);
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {
        Cache c = CacheManager.getInstance().getCache(args.get("cache"));
        CachedData cachedData = new CachedData(c.getIdentifier(), args.get("identifier"), data);
        c.insertCachedData(cachedData);
        if(args.containsKey("duration")){
            try{
                int i = Integer.parseInt(args.get("duration"));
                cachedData.setValidForDuration(i);
            }catch (Exception ignore){
                result.setInternalStatus("Failed To Parse Duration - Cache Valid For 10 Seconds");
                cachedData.setValidForDuration(10);
            }
        }
        result.addResult(new JSONObject().put("cache", c.getIdentifier()).put("cachedData", new JSONObject().put("identifier", cachedData.getIdentifier()).put("validUntil", cachedData.isValidUntil())));
    }
}
