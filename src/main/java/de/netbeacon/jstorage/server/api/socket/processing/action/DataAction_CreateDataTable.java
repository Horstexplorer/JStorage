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
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Data Action - Create Data Table
 * <p>
 * --- Does --- <br>
 * Tries to create a specific datatable within the selected database <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * database, table as JSONObject <br>
 * --- Requirements --- <br>
 * action: createdatatable <br>
 * http_method: put <br>
 * login-mode: token <br>
 * payload: no <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.DBAdmin, DependentPermission.DBAdmin_Creator, DependentPermission.DBAdmin_User <br>
 * required_arguments: database(String, databaseIdentifier), identifier(String, tableIdentifier) <br>
 * optional_arguments: adaptiveLoad(Boolean, Setting) <br>
 *
 * @author horstexplorer
 */
public class DataAction_CreateDataTable implements ProcessingAction{

    private HTTPProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new DataAction_CreateDataTable();
    }

    @Override
    public String getAction() {
        return "createdatatable";
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
        return Arrays.asList("identifier", "database");
    }

    @Override
    public boolean userHasPermission() {
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.DBAdmin) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.CacheAdmin_Creator)) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.CacheAdmin_User));
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException {
        DataBase d = DataManager.getDataBase(args.get("database"));
        DataTable t = new DataTable(d, args.get("identifier"));
        d.insertTable(t);

        if(args.containsKey("adaptiveLoad")){
            try{
                boolean a = Boolean.parseBoolean(args.get("adaptiveLoad"));
                t.setAdaptiveLoading(a);
            }catch (Exception e){
                throw new GenericObjectException(200, "Error Parsing Setting \"adaptiveLoad\"");
            }
        }

        result.addResult(new JSONObject().put("database", d.getIdentifier()).put("table", t.getIdentifier()));
    }
}
