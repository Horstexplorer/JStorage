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

import de.netbeacon.jstorage.server.api.socket.processing.APIProcessorResult;
import de.netbeacon.jstorage.server.internal.usermanager.UserManager;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.apache.commons.lang3.RandomStringUtils;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * User Action - Create User
 * <p>
 * --- Does --- <br>
 * Tries to create a new user <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * Returns userID, userName, password, bucketSize as json on success <br>
 * --- Requirements --- <br>
 * path: user/mng <br>
 * action: create <br>
 * http_method: put <br>
 * login-mode: token <br>
 * payload: no <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.UserAdmin <br>
 * required_arguments: identifier(String, Username) <br>
 * optional_arguments: password(String), bucketSize(int) <br>
 *
 * @author horstexplorer
 */
public class UserAction_CreateUser implements ProcessingAction {

    private APIProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new UserAction_CreateUser();
    }

    @Override
    public String getAction() {
        return "create";
    }

    @Override
    public void setup(User user, APIProcessorResult result, HashMap<String, String> args) {
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
        return Collections.singletonList("identifier");
    }

    @Override
    public boolean userHasPermission() {
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.UserAdmin);
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {
        User u = UserManager.getInstance().createUser(args.get("identifier"));
        String password;
        if(args.containsKey("password")){
            password = args.get("password");
        }else{
            password = RandomStringUtils.random(8);
        }
        u.setPassword(password);
        if(args.containsKey("bucketSize")){
            try{
                int b = Integer.parseInt("bucketSize");
                u.setMaxBucketSize(b);
            }catch(Exception e){
                throw new GenericObjectException(200, "Error Parsing Setting \"bucketSize\"");
            }
        }
        u.addGlobalPermission(GlobalPermission.UserDefault_Self);
        JSONObject customResponseData = new JSONObject()
                .put("identifier", u.getUserID())
                .put("userName", u.getUserName())
                .put("bucketSize", u.getMaxBucket())
                .put("password", password);
        // set result
        result.addResult(this.getDefaultResponse(customResponseData));
    }
}
