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
import de.netbeacon.jstorage.server.internal.usermanager.UserManager;
import de.netbeacon.jstorage.server.internal.usermanager.object.DependentPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.socket.api.processing.APIProcessorResult;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Cache Action - Delete Cache
 * <p>
 * --- Does --- <br>
 * Tries to delete the specified cache <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * cache <br>
 * --- Requirements --- <br>
 * path: cache/mng <br>
 * action: delete <br>
 * http_method: delete <br>
 * login-mode: token <br>
 * payload: no <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.CacheAdmin, DependentPermission.CacheAdmin_Creator <br>
 * required_arguments: identifier(String, cacheIdentifier) <br>
 * optional_arguments: <br>
 *
 * @author horstexplorer <br>
 */
public class CacheAction_DeleteCache implements ProcessingAction {

    private APIProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new CacheAction_DeleteCache();
    }

    @Override
    public boolean supportedHTTPMethod(String method) {
        return "delete".equalsIgnoreCase(method);
    }

    @Override
    public boolean userHasPermission() {
        return user.hasGlobalPermission(GlobalPermission.Admin) || user.hasGlobalPermission(GlobalPermission.CacheAdmin) || user.hasDependentPermission(args.get("identifier"), DependentPermission.CacheAdmin_Creator);
    }

    @Override
    public String getAction() {
        return "delete";
    }

    @Override
    public List<String> requiredArguments() {
        return Collections.singletonList("identifier");
    }

    @Override
    public void setup(User user, APIProcessorResult result, HashMap<String, String> args){
        this.user = user;
        this.result = result;
        this.args = args;
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {
        CacheManager.getInstance().deleteCache(args.get("identifier"));
        UserManager.getInstance().getDataPool().forEach((k,v)->v.removeDependentPermissions(args.get("identifier")));
        JSONObject customResponseData = new JSONObject()
                .put("identifier", args.get("identifier").toLowerCase());
        // set result
        result.addResult(this.getDefaultResponse(customResponseData));
    }
}
