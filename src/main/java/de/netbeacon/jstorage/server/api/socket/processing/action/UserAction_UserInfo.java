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
import de.netbeacon.jstorage.server.internal.usermanager.UserManager;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * User Action - User Info
 * <p>
 * --- Does --- <br>
 * Tries to list information for all or a specific user <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * userID, userName, bucketSize, globalPermission, dependentPermission or <br>
 * users as JSONObject <br>
 * --- Requirements --- <br>
 * action: userinfo <br>
 * http_method: get <br>
 * login-mode: token <br>
 * payload: no <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.UserAdmin, GlobalPermission.UserAdmin_Self, UserDefault_Self <br>
 * required_arguments: <br>
 * optional_arguments: identifier(String, userID) <br>
 *
 * @author horstexplorer
 */
public class UserAction_UserInfo implements ProcessingAction {

    private HTTPProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new UserAction_UserInfo();
    }

    @Override
    public String getAction() {
        return "userinfo";
    }

    @Override
    public void setup(User user, HTTPProcessorResult result, HashMap<String, String> args) {
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
        return new ArrayList<>();
    }

    @Override
    public boolean userHasPermission() {
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.UserAdmin) ||
                (args.containsKey("identifier") && user.hasGlobalPermission(GlobalPermission.UserAdmin_Self) && user.getUserID().equalsIgnoreCase(args.get("identifier"))) ||
                (args.containsKey("identifier") && user.hasGlobalPermission(GlobalPermission.UserDefault_Self) && user.getUserID().equalsIgnoreCase(args.get("identifier")));
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException {
        JSONObject jsonObject = new JSONObject();
        if(args.containsKey("identifier")){
            User u = UserManager.getUserByID(args.get("identifier"));
            jsonObject.put("userID", u.getUserID()).put("userName", u.getUserName()).put("bucketSize", u.getMaxBucket());
            JSONArray jsonArray1 = new JSONArray();
            u.getGlobalPermissions().forEach(jsonArray1::put);
            JSONArray jsonArray2 = new JSONArray();
            u.getDependentPermission().forEach((k,v)->{
                JSONObject jsonObject1 = new JSONObject().put("dependentOn", k);
                JSONArray jsonArray3 = new JSONArray();
                v.forEach(jsonArray3::put);
                jsonObject1.put("permissions", jsonArray3);
                jsonArray2.put(jsonObject1);
            });
            jsonObject.put("globalPermission", jsonArray1).put("dependentPermission", jsonArray2);
        }else{
            JSONArray jsonArray = new JSONArray();
            UserManager.getDataPool().values().forEach(v->{
                jsonArray.put(new JSONObject().put("userName", v.getUserName()).put("userID", v.getUserID()));
            });
            jsonObject.put("users", jsonArray);
        }
        result.addResult(jsonObject);
    }
}
