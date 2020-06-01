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
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Data Action - Data Base Info
 * <p>
 * --- Does --- <br>
 * Tries to list information for all or a specific database <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * database, tables or <br>
 * databases as JSONObject <br>
 * --- Requirements --- <br>
 * action: deletedatabase <br>
 * http_method: get <br>
 * login-mode: token <br>
 * payload: no <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.DBAdmin, DependentPermission.DBAdmin_Creator, DependentPermission.DBAdmin_User <br>
 * required_arguments: <br>
 * optional_arguments: identifier(String, databaseIdentifier) <br>
 *
 * @author horstexplorer
 */
public class DataAction_DataBaseInfo implements ProcessingAction{

    private HTTPProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new DataAction_DataBaseInfo();
    }

    @Override
    public String getAction() {
        return "databaseinfo";
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
                user.hasGlobalPermission(GlobalPermission.DBAdmin) ||
                (args.containsKey("identifier") && user.hasDependentPermission(args.get("identifier"), DependentPermission.DBAdmin_Creator)) ||
                (args.containsKey("identifier") && user.hasDependentPermission(args.get("identifier"), DependentPermission.DBAdmin_User));
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException {
        JSONObject jsonObject = new JSONObject();
        if(args.containsKey("identifier")){
            DataBase d = DataManager.getDataBase(args.get("identifier"));
            JSONArray jsonArray = new JSONArray();
            d.getDataPool().values().forEach(v ->jsonArray.put(v.getIdentifier()));
            jsonObject.put("database", d.getIdentifier()).put("tables", jsonArray);
        }else{
            JSONArray jsonArray = new JSONArray();
            DataManager.getDataPool().values().forEach(v->jsonArray.put(v.getIdentifier()));
            jsonObject.put("databases", jsonArray);
        }
        result.addResult(jsonObject);
    }
}
