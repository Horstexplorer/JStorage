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

import de.netbeacon.jstorage.server.internal.usermanager.UserManager;
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
 * User Action - User Change Password
 * <p>
 * --- Does --- <br>
 * Tries to change the password of the current or selected user <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * userid <br>
 * --- Requirements --- <br>
 * path: user/mng <br>
 * action: changepw <br>
 * http_method: put <br>
 * login-mode: password <br>
 * payload: no <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.UserAdmin, GlobalPermission.UserAdmin_Self, UserDefault_Self <br>
 * required_arguments: password(String, newPassword) <br>
 * optional_arguments: identifier(String, userID) <br>
 *
 * @author horstexplorer
 */
public class UserAction_UserChangePassword implements ProcessingAction {

    private APIProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new UserAction_UserChangePassword();
    }

    @Override
    public String getAction() {
        return "changepw";
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
        return Collections.singletonList("password");
    }

    @Override
    public boolean userHasPermission() {
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.UserAdmin) ||
                (user.hasGlobalPermission(GlobalPermission.UserAdmin_Self) && (!args.containsKey("identifier") || user.getUserID().equalsIgnoreCase(args.get("identifier")))) ||
                (user.hasGlobalPermission(GlobalPermission.UserDefault_Self) && (!args.containsKey("identifier") || user.getUserID().equalsIgnoreCase(args.get("identifier"))));
    }

    @Override
    public boolean loginModeIsSupported(int loginMode){return 0 == loginMode;} // only login with password

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {
        JSONObject customResponseData = new JSONObject();
        if(args.containsKey("identifier")){
            User u = UserManager.getInstance().getUserByID(args.get("identifier"));
            u.setPassword(args.get("password"));
            customResponseData
                    .put("identifier", u.getUserID());
        }else{
            user.setPassword(args.get("password"));
            customResponseData
                    .put("identifier", user.getUserID());
        }
        // set result
        result.addResult(this.getDefaultResponse(customResponseData));
    }
}
