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
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataSet;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataTable;
import de.netbeacon.jstorage.server.internal.usermanager.object.DependentPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Data Action - Create Data Set
 * <p>
 * --- Does --- <br>
 * Tries to create a specific dataset within the selected table from a database <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * dataset <br>
 * --- Requirements --- <br>
 * action: createdataset <br>
 * http_method: put <br>
 * login-mode: token <br>
 * payload: optional <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.DBAdmin, DependentPermission.DBAdmin_Creator, DependentPermission.DBAdmin_User, DependentPermission.DBAccess_Modify <br>
 * required_arguments: database(String, databaseIdentifier), table(String, tableIdentifier), identifier(String, datasetIdentifier) <br>
 * optional_arguments: <br>
 *
 * @author horstexplorer
 */
public class DataAction_CreateDataSet implements ProcessingAction{

    private HTTPProcessorResult result;
    private HashMap<String, String> args;
    private User user;
    private JSONObject data;

    @Override
    public ProcessingAction createNewInstance() {
        return new DataAction_CreateDataSet();
    }

    @Override
    public String getAction() {
        return "createdataset";
    }

    @Override
    public void setup(User user, HTTPProcessorResult result, HashMap<String, String> args) {
        this.user = user;
        this.result = result;
        this.args = args;
    }

    @Override
    public void setPayload(JSONObject payload) {
        this.data = payload;
    }

    @Override
    public boolean supportedHTTPMethod(String method) {
        return "put".equalsIgnoreCase(method);
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
                (user.hasDependentPermission(args.get("database"), DependentPermission.CacheAdmin_Creator)) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.CacheAdmin_User)) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.DBAccess_Modify));
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException {
        DataBase d = DataManager.getDataBase(args.get("database"));
        DataTable t = d.getTable(args.get("identifier"));
        DataSet ds;
        if(data == null){
            // create empty
            ds = new DataSet(t.getDatabaseName(), t.getTableName(), args.get("identifier"));
        }else{
            ds = new DataSet(t.getDatabaseName(), t.getTableName(), args.get("identifier"), data);
        }
        t.insertDataSet(ds); // as this might throw, leaving an increased dataset count but no actual dataset, the DataManager corrects the count from time to time
        // return ds
        result.addResult(ds.getFullData());
    }
}
