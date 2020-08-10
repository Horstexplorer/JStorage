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

import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.socket.api.processing.APIProcessorResult;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import de.netbeacon.jstorage.server.tools.info.Info;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Info Action - Basic Info
 * 
 * --- Does --- </br>
 * Provides basic information about this JStorage installation </br>
 * --- Returns --- </br>
 * basic information </br>
 * --- Requirements --- </br>
 * path: info/basic </br>
 * action: info </br>
 * http_method: get </br>
 * login-mode: all </br>
 * payload: no </br>
 * permissions: none </br>
 * required_arguments: </br>
 * optional_arguments: </br>
 *
 * @author horstexplorer
 */
public class InfoAction_BasicInfo implements ProcessingAction{

    private APIProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new InfoAction_BasicInfo();
    }

    @Override
    public String getAction() {
        return "info";
    }

    @Override
    public void setup(User user, APIProcessorResult result, HashMap<String, String> args) {
        this.user = user;
        this.result = result;
        this.args = args;
    }

    @Override
    public boolean loginModeIsSupported(int loginMode) {
        return true;
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
        return true;
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {
        JSONObject customResponseData = new JSONObject()
                .put("this", "jstorage")
                .put("version", Info.VERSION);
        // set result
        result.addResult(this.getDefaultResponse(customResponseData));
    }
}
