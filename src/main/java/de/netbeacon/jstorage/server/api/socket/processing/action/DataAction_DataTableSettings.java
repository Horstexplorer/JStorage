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
 * Cache Action - Data Table Settings
 * <p>
 * --- Does --- <br>
 * Tries to change settings for a specific datatable within a database <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * database, table, settings <br>
 * --- Requirements --- <br>
 * action: datatablesettings <br>
 * http_method: put <br>
 * login-mode: token <br>
 * payload: yes - optional: adaptiveLoading(boolean), defaultStructure(JSONObject), autoOptimize(Boolean), autoResolveDataInconsistency(Integer in range -1 to 3) <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.DBAdmin, DependentPermission.DBAdmin_Creator <br>
 * required_arguments: database(String, databaseIdentifier), identifier(String, tableIdentifier) <br>
 * optional_arguments: optimize(Boolean), resolveDataInconsistency(Integer in range -1 to 3) <br>
 *
 * @author horstexplorer
 */
public class DataAction_DataTableSettings implements ProcessingAction{

    private HTTPProcessorResult result;
    private HashMap<String, String> args;
    private User user;
    private JSONObject data;

    @Override
    public ProcessingAction createNewInstance() {
        return new DataAction_DataTableSettings();
    }

    @Override
    public String getAction() {
        return "datatablesettings";
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
        return Arrays.asList("database", "identifier");
    }

    @Override
    public boolean requiresData() {
        return true;
    }

    @Override
    public boolean userHasPermission() {
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.DBAdmin) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.DBAdmin_Creator));
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException {
        DataBase d = DataManager.getDataBase(args.get("database"));
        DataTable t = d.getTable(args.get("identifier"));

        if(data.has("adaptiveLoading")) {
            try {
                boolean newVal = Boolean.parseBoolean(data.getString("adaptiveLoading"));
                t.setAdaptiveLoading(newVal);
            } catch (Exception ignore) {
                throw new GenericObjectException(200, "Error Parsing Setting \"adaptiveLoading\"");
            }
        }

        if(data.has("defaultStructure") && data.get("defaultStructure").getClass() == JSONObject.class){
            t.setDefaultStructure(data.getJSONObject("defaultStructure"));
        }

        if(data.has("autoOptimize")){
            t.setAutoOptimization(data.getBoolean("autoOptimize"));
        }

        if(data.has("autoResolveDataInconsistency")){
            t.setAutoResolveDataInconsistency(data.getInt("autoResolveDataInconsistency"));
        }

        if(args.containsKey("optimize") && Boolean.parseBoolean(args.get("optimize"))){
            // optimize table now
            t.optimize();
        }

        if(args.containsKey("resolveDataInconsistency")){
            // resolve data inconsistency
            t.resolveDataInconsistency(Integer.parseInt(args.get("resolveDataInconsistency")));
        }

        // return info
        JSONObject settings = new JSONObject();
        settings.put("adaptiveLoading", t.isAdaptive()).put("defaultStructure", t.getDefaultStructure()).put("autoResolveDataInconsistency", t.autoResolveDataInconsistencyMode()).put("autoOptimize", t.autoOptimizationEnabled());
        result.addResult(new JSONObject().put("database", d.getIdentifier()).put("table", t.getIdentifier()).put("settings", settings));
    }
}
