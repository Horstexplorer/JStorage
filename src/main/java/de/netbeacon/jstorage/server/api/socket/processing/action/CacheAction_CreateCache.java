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
import de.netbeacon.jstorage.server.internal.usermanager.object.DependentPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Cache Action - Create Cache
 * <p>
 * --- Does --- <br>
 * Tries to create a specific cache <br>
 * User will receive CacheAdmin_Creator permission for the new cache <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * cache as JSON <br>
 * --- Requirements --- <br>
 * path: cache/mng <br>
 * action: create <br>
 * http_method: put <br>
 * login-mode: token <br>
 * payload: no <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.CacheAdmin <br>
 * required_arguments: identifier(String, cacheIdentifier) <br>
 * optional_arguments: <br>
 *
 * @author horstexplorer
 */
public class CacheAction_CreateCache implements ProcessingAction {

   private HTTPProcessorResult result;
   private HashMap<String, String> args;
   private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new CacheAction_CreateCache();
    }

    @Override
    public boolean supportedHTTPMethod(String method) {
        return "put".equalsIgnoreCase(method);
    }

    @Override
    public boolean userHasPermission() {
        return user.hasGlobalPermission(GlobalPermission.Admin) || user.hasGlobalPermission(GlobalPermission.CacheAdmin);
    }

    @Override
    public String getAction() {
        return "create";
    }

    @Override
    public List<String> requiredArguments() {
        return Collections.singletonList("identifier");
    }

    @Override
    public void setup(User user, HTTPProcessorResult result, HashMap<String, String> args){
        this.user = user;
        this.result = result;
        this.args = args;
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException{
        Cache c = CacheManager.getInstance().createCache(args.get("identifier"));
        user.addDependentPermission(c.getCacheIdentifier(), DependentPermission.CacheAdmin_Creator);
        result.addResult(new JSONObject().put("cache", c.getCacheIdentifier()));
    }
}
