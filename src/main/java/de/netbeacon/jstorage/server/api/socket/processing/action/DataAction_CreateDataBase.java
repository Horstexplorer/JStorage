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
import de.netbeacon.jstorage.server.internal.datamanager.DataManager;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataBase;
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
 * Data Action - Create Data Base
 * <p>
 * --- Does --- <br>
 * Tries to create a specific database <br>
 * User will receive DBAdmin_Creator permission for the new database <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * database as JSON <br>
 * --- Requirements --- <br>
 * path: data/db <br>
 * action: create <br>
 * http_method: put <br>
 * login-mode: token <br>
 * payload: no <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.DBAdmin <br>
 * required_arguments: identifier(String, databaseIdentifier) <br>
 * optional_arguments: <br>
 *
 * @author horstexplorer
 */
public class DataAction_CreateDataBase implements ProcessingAction{

    private HTTPProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new DataAction_CreateDataBase();
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
        return Collections.singletonList("identifier");
    }

    @Override
    public boolean userHasPermission() {
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.DBAdmin);
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {
        DataBase d = DataManager.getInstance().createDataBase(args.get("identifier"));
        user.addDependentPermission(d.getIdentifier(), DependentPermission.DBAdmin_Creator);
        JSONObject customResponseData = new JSONObject()
                .put("identifier", d.getIdentifier());
        // set result
        result.addResult(this.getDefaultResponse(customResponseData));
    }
}
