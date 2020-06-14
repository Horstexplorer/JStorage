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
import de.netbeacon.jstorage.server.internal.usermanager.object.DependentPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * User Action - User Settings
 * <p>
 * --- Does --- <br>
 * Tries to change settings for the current or selected user <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * userID, userName, bucketSize, globalPermission, dependentPermission as JSONObject <br>
 * --- Requirements ---
 * action: usersettings <br>
 * http_method: put <br>
 * login-mode: token <br>
 * payload: yes - optional: bucketSize(int), dependentPermission(JSON), globalPermission(JSON) <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.UserAdmin <br>
 * required_arguments: <br>
 * optional_arguments: identifier(String, userID) <br>
 *
 * @author horstexplorer
 */
public class UserAction_UserSettings implements ProcessingAction {

    private HTTPProcessorResult result;
    private HashMap<String, String> args;
    private User user;
    private JSONObject data;

    @Override
    public ProcessingAction createNewInstance() {
        return new UserAction_UserSettings();
    }

    @Override
    public String getAction() {
        return "usersettings";
    }

    @Override
    public void setup(User user, HTTPProcessorResult result, HashMap<String, String> args) {
        this.user = user;
        this.result = result;
        this.args = args;
    }

    @Override
    public void setPayload(JSONObject payload){
        this.data = payload;
    }

    @Override
    public boolean supportedHTTPMethod(String method) {
        return "put".equalsIgnoreCase(method);
    }

    @Override
    public List<String> requiredArguments() {
        return new ArrayList<>();
    }

    @Override
    public boolean requiresData(){return true;}

    @Override
    public boolean userHasPermission() {
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.UserAdmin);
    }

    @Override
    public boolean loginModeIsSupported(int loginMode){return 0 == loginMode;} // only login with password

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {
        JSONObject jsonObject = new JSONObject();
        if(args.containsKey("identifier")){
            // get user
            User u = UserManager.getInstance().getUserByID(args.get("identifier"));
            // update data
            updateData(u);
            // build result
            result.addResult(buildResult(u));
        }else{
            // update data
            updateData(user);
            // build result
            result.addResult(buildResult(user));
        }
    }

    private void updateData(User u) throws GenericObjectException, DataStorageException {
        if(data.has("bucketSize")){
            try{
                u.setMaxBucketSize(data.getInt("bucketSize"));
            }catch (Exception ignore){
                throw new GenericObjectException(200, "Error Parsing Setting \"bucketSize\"");
            }
        }
        if(data.has("dependentPermission")){
            try{
                JSONObject p = data.getJSONObject("dependentPermission");
                if(p.has("add")){
                    JSONArray array = p.getJSONArray("add");
                    for(int i = 0; i < array.length(); i++){
                        JSONObject dp = array.getJSONObject(i);
                        String dpnd = dp.getString("dependentOn");
                        JSONArray array2 = dp.getJSONArray("permissions");
                        for(int o = 0; o < array2.length() ; o++){
                            DependentPermission dependentPermission = DependentPermission.getByValue(array2.getString(o));
                            if(dependentPermission != null){
                                u.addDependentPermission(dpnd, dependentPermission);
                            }
                        }
                    }
                }else if(p.has("remove")){
                    JSONArray array = p.getJSONArray("remove");
                    for(int i = 0; i < array.length(); i++){
                        JSONObject dp = array.getJSONObject(i);
                        String dpnd = dp.getString("dependentOn");
                        JSONArray array2 = dp.getJSONArray("permissions");
                        for(int o = 0; o < array2.length() ; o++){
                            DependentPermission dependentPermission = DependentPermission.getByValue(array2.getString(o));
                            if(dependentPermission != null){
                                u.removeDependentPermission(dpnd, dependentPermission);
                            }
                        }
                    }
                }
            }catch (Exception ignore){
                throw new GenericObjectException(200, "Error Parsing Setting \"dependentPermission\"");
            }
        }
        if(data.has("globalPermission") ){
            try{
                JSONObject p = data.getJSONObject("globalPermission");
                if(p.has("add")){
                    JSONArray array = p.getJSONArray("add");
                    for(int i = 0; i < array.length(); i++){
                        GlobalPermission globalPermission = GlobalPermission.getByValue(array.getString(i));
                        if(globalPermission != null){
                            u.addGlobalPermission(globalPermission);
                        }
                    }
                }else if(p.has("remove")){
                    JSONArray array = p.getJSONArray("remove");
                    for(int i = 0; i < array.length(); i++){
                        GlobalPermission globalPermission = GlobalPermission.getByValue(array.getString(i));
                        if(globalPermission != null){
                            u.removeGlobalPermission(globalPermission);
                        }
                    }
                }
            }catch (Exception ignore){
                throw new GenericObjectException(200, "Error Parsing Setting \"globalPermission\"");
            }
        }
    }

    private JSONObject buildResult(User u){
        JSONObject jsonObject = new JSONObject().put("userID", user.getUserID()).put("userName", user.getUserName()).put("bucketSize", user.getMaxBucket());
        JSONArray jsonArray1 = new JSONArray();
        user.getGlobalPermissions().forEach(jsonArray1::put);
        JSONArray jsonArray2 = new JSONArray();
        user.getDependentPermission().forEach((k,v)->{
            JSONObject jsonObject1 = new JSONObject().put("dependentOn", k);
            JSONArray jsonArray3 = new JSONArray();
            v.forEach(jsonArray3::put);
            jsonObject1.put("permissions", jsonArray3);
            jsonArray2.put(jsonObject1);
        });
        jsonObject.put("globalPermission", jsonArray1).put("dependentPermission", jsonArray2);
        return jsonObject;
    }
}
