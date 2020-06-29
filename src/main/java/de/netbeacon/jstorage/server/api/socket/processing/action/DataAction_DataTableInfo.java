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
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataTable;
import de.netbeacon.jstorage.server.internal.usermanager.object.DependentPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Data Action - Data Table Info
 * <p>
 * --- Does --- <br>
 * Tries to list information for all or a specific data table within a database <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * database, table, adaptiveLoad, datasets, shards or <br>
 * database, tables as JSONObject <br>
 * --- Requirements --- <br>
 * path: data/db/table <br>
 * action: info <br>
 * http_method: get <br>
 * login-mode: token <br>
 * payload: no <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.DBAdmin, DependentPermission.DBAdmin_Creator, DependentPermission.DBAdmin_User, DependentPermission.DBAccess_Modify, DependentPermission.DBAccess_Read <br>
 * required_arguments: database(String, databaseIdentifier) <br>
 * optional_arguments: identifier(String, tableIdentifier) <br>
 *
 * @author horstexplorer
 */
public class DataAction_DataTableInfo implements ProcessingAction{

    private HTTPProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new DataAction_DataBaseInfo();
    }

    @Override
    public String getAction() {
        return "info";
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
        return Collections.singletonList("database");
    }

    @Override
    public boolean userHasPermission() {
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.DBAdmin) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.DBAdmin_Creator)) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.DBAdmin_User)) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.DBAccess_Modify)) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.DBAccess_Read));
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {
        JSONObject customResponseData = new JSONObject();
        DataBase d = DataManager.getInstance().getDataBase(args.get("database"));
        if(args.containsKey("identifier")){
            DataTable t = d.getTable(args.get("identifier"));
            JSONArray jsonArray = new JSONArray();
            t.getIndexPool().forEach((k,v)->jsonArray.put(k));
            customResponseData
                    .put("database", d.getIdentifier())
                    .put("identifier", t.getIdentifier())
                    .put("adaptiveLoading", t.isAdaptive())
                    .put("datasets", jsonArray)
                    .put("shards", t.getDataPool().size());
        }else{
            JSONArray jsonArray = new JSONArray();
            d.getDataPool().values().forEach(v->jsonArray.put(v.getIdentifier()));
            customResponseData
                    .put("database", d.getIdentifier())
                    .put("tables", jsonArray);
        }
        // set result
        result.addResult(this.getDefaultResponse(customResponseData));
    }
}
