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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Data Action - Multi Select
 * <p>
 * --- Does --- <br>
 * Tries to update the data from a specific datatype within the selected dataset <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * datatype data <br>
 * --- Requirements --- <br>
 * path: data/tool <br>
 * action: multiselect <br>
 * http_method: put <br>
 * login-mode: token <br>
 * payload: yes <br>
 * permissions: none -> GlobalPermission.Admin, GlobalPermission.DBAdmin, DependentPermission.DBAdmin_Creator, DependentPermission.DBAdmin_User, DependentPermission.DBAccess_Modify <br>
 * required_arguments: <br>
 * optional_arguments: <br>
 *
 * @author horstexplorer
 */
public class DataAction_MultiSelect implements ProcessingAction{

    private APIProcessorResult result;
    private HashMap<String, String> args;
    private User user;
    private JSONObject data;

    @Override
    public ProcessingAction createNewInstance() {
        return new DataAction_MultiSelect();
    }

    @Override
    public String getAction() {
        return "multiselect";
    }

    @Override
    public void setup(User user, APIProcessorResult result, HashMap<String, String> args) {
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
        return new ArrayList<>();
    }

    @Override
    public boolean requiresData() {
        return true;
    }

    @Override
    public boolean userHasPermission() {
        return true; // do the actual check later
    }

    private boolean userHasPermission(String database){
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.DBAdmin) ||
                (user.hasDependentPermission(database, DependentPermission.DBAdmin_Creator)) ||
                (user.hasDependentPermission(database, DependentPermission.DBAdmin_User)) ||
                (user.hasDependentPermission(database, DependentPermission.DBAccess_Modify));
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {
        /*

            {selection:[
                    { database:"", table:"", dataset:"" },
                    { database:"", table:"", dataset:"", datatype:"" },
                    { database:"", table:"", dataset:"", datatype:"" , acquire=bool},
             ]}

         */

        // prepare result
        JSONArray resultArray = new JSONArray();
        JSONObject customResponse = new JSONObject().put("selection", resultArray);
        // parse data
        JSONArray selections = data.getJSONArray("selection");
        for(int i = 0; i < selections.length(); i++) {
            try{
                JSONObject jsonObject = selections.getJSONObject(i);
                if(!userHasPermission(jsonObject.getString("database"))){
                    continue;
                }
                DataBase d = DataManager.getInstance().getDataBase(jsonObject.getString("database"));
                DataTable t = d.getTable(jsonObject.getString("table"));
                DataSet ds = t.getDataSet(jsonObject.getString("dataset"));
                if(jsonObject.has("datatype")){
                    if(jsonObject.has("acquire")){
                        resultArray.put(ds.get(jsonObject.getString("datatype"), jsonObject.getBoolean("acquire")));
                    }else{
                        resultArray.put(ds.get(jsonObject.getString("datatype"), false));
                    }
                }else{
                    resultArray.put(ds.getFullData());
                }
            }catch (Exception ignore){}
        }
        // set result
        result.addResult(getDefaultResponse(customResponse));
    }
}
