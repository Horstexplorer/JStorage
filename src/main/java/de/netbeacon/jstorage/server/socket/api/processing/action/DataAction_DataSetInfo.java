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

import de.netbeacon.jstorage.server.internal.datamanager.DataManager;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataBase;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataSet;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataTable;
import de.netbeacon.jstorage.server.internal.usermanager.object.DependentPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.socket.api.processing.APIProcessorResult;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Data Action - Data Set Info
 * 
 * --- Does --- </br>
 * Tries to list information for a specific database within a within a datatable </br>
 * Exceptions catched by superordinate processing handler </br>
 * --- Returns --- </br>
 * database, table, identifier, size, keys </br>
 * --- Requirements --- </br>
 * path: data/db/table/dataset </br>
 * action: info </br>
 * http_method: get </br>
 * login-mode: token </br>
 * payload: no </br>
 * permissions: GlobalPermission.Admin, GlobalPermission.DBAdmin, DependentPermission.DBAdmin_Creator, DependentPermission.DBAdmin_User, DependentPermission.DBAccess_Modify, DependentPermission.DBAccess_Read </br>
 * required_arguments: database(String, databaseIdentifier), table(String, tableIdentifier), identifier(String, datasetIdentifier) </br>
 * optional_arguments: </br>
 *
 * @author horstexplorer
 */
public class DataAction_DataSetInfo implements ProcessingAction{

    private APIProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new DataAction_DataSetInfo();
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
    public boolean supportedHTTPMethod(String method) {
        return "get".equalsIgnoreCase(method);
    }

    @Override
    public List<String> requiredArguments() {
        return Arrays.asList("database", "table", "identifier");
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
        DataBase d = DataManager.getInstance().getDataBase(args.get("database"));
        DataTable t = d.getTable(args.get("table"));
        DataSet ds = t.getDataSet(args.get("identifier"));
        JSONObject data = ds.getFullData();
        JSONArray jsonArray = new JSONArray();
        data.keySet().stream().filter(v->!(v.equalsIgnoreCase("database") || v.equalsIgnoreCase("table") || v.equalsIgnoreCase("identifier"))).forEach(jsonArray::put);
        JSONObject customResponseData = new JSONObject()
                .put("database", ds.getDataBase().getIdentifier())
                .put("table", ds.getTable().getIdentifier())
                .put("identifier", ds.getIdentifier())
                .put("size", data.toString().getBytes().length)
                .put("keys", jsonArray);
        // set result
        result.addResult(this.getDefaultResponse(customResponseData));
    }
}
