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

import de.netbeacon.jstorage.server.api.socket.processing.APIProcessorResult;
import de.netbeacon.jstorage.server.internal.datamanager.objects.DataSet;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Data Action - Data Set Settings
 * <p>
 * --- Does --- <br>
 * Tries to change settings for dataset management <br>
 * Exceptions catched by superordinate processing handler <br>
 * --- Returns --- <br>
 * global dataset settings <br>
 * --- Requirements --- <br>
 * path: data/db/table/dataset <br>
 * action: settings <br>
 * http_method: put <br>
 * login-mode: token <br>
 * payload: yes - optional: maxSTPEThreads(int), dataSetsPerThread(int) <br>
 * permissions: GlobalPermission.Admin, GlobalPermission.UserAdmin <br>
 * required_arguments: <br>
 * optional_arguments: <br>
 *
 * @author horstexplorer
 */
public class DataAction_DataSetSettings implements ProcessingAction{

    private APIProcessorResult result;
    private HashMap<String, String> args;
    private User user;
    private JSONObject data;

    @Override
    public ProcessingAction createNewInstance() {
        return new DataAction_DataSetSettings();
    }

    @Override
    public String getAction() {
        return "settings";
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
        return
                user.hasGlobalPermission(GlobalPermission.Admin) ||
                        user.hasGlobalPermission(GlobalPermission.UserAdmin);
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException, CryptException, NullPointerException {

        if(data.has("maxSTPEThreads")){
            try{
                int i = data.getInt("maxSTPEThreads");
                DataSet.setMaxSTPEThreads(i);
            }catch (Exception ignore){
                throw new GenericObjectException(200, "Error Parsing Setting \"maxSTPEThreads\"");
            }
        }

        if(data.has("dataSetsPerThread")){
            try{
                int i = data.getInt("dataSetsPerThread");
                DataSet.setDataSetsPerThread(i);
            }catch (Exception ignore){
                throw new GenericObjectException(200, "Error Parsing Setting \"dataSetsPerThread\"");
            }
        }

        JSONObject customResponseData = new JSONObject()
                .put("maxSTPEThreads", DataSet.getMaxSTPEThreads())
                .put("dataSetsPerThread", DataSet.getDataSetsPerThread())
                .put("dataSets", DataSet.getDataSetCount());
        // set result
        result.addResult(this.getDefaultResponse(customResponseData));
    }
}