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
import de.netbeacon.jstorage.server.tools.meta.UsageStatistics;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Info Action - Statistics
 * <p>
 * --- Does --- <br>
 * Provides statistics about selected types <br>
 * --- Returns --- <br>
 * basic information <br>
 * --- Requirements --- <br>
 * action: statistics <br>
 * http_method: get <br>
 * login-mode: token <br>
 * payload: no <br>
 * permissions: none <br>
 * required_arguments: <br>
 * optional_arguments: database, table, dataset <br>
 *
 * @author horstexplorer
 */
public class InfoAction_Statistics implements ProcessingAction{

    private HTTPProcessorResult result;
    private HashMap<String, String> args;
    private User user;

    @Override
    public ProcessingAction createNewInstance() {
        return new InfoAction_Statistics();
    }

    @Override
    public String getAction() {
        return "statistics";
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
        return  user.hasGlobalPermission(GlobalPermission.Admin) ||
                user.hasGlobalPermission(GlobalPermission.DBAdmin) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.DBAdmin_Creator)) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.DBAdmin_User)) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.DBAccess_Modify)) ||
                (user.hasDependentPermission(args.get("database"), DependentPermission.DBAccess_Read));
    }

    @Override
    public void process() throws DataStorageException, GenericObjectException {
        UsageStatistics usageStatistics = null;
        JSONObject statistics = new JSONObject();
        DataBase dataBase = DataManager.getDataBase(args.get("database"));
        statistics.put("database", dataBase.getIdentifier());
        if (args.containsKey("table")) {
            DataTable dataTable = dataBase.getTable(args.get("table"));
            statistics.put("table", dataTable.getIdentifier());
            if (args.containsKey("dataset")) {
                DataSet dataSet = dataTable.getDataSet(args.get("dataset"));
                statistics.put("dataset", dataTable.getIdentifier());
                usageStatistics = dataTable.getStatisticsFor(dataSet.getIdentifier()); // reuse the exception if not found
            } else {
                usageStatistics = dataTable.getStatistics();
            }
        } else {
            usageStatistics = dataBase.getStatistics();
        }
        JSONObject values = new JSONObject()
                .put("all", usageStatistics.getCountFor(UsageStatistics.Usage.any))
                .put("success", new JSONObject().put("get", usageStatistics.getCountFor(UsageStatistics.Usage.get_success)).put("insert", usageStatistics.getCountFor(UsageStatistics.Usage.insert_success)).put("update", usageStatistics.getCountFor(UsageStatistics.Usage.update_success)).put("delete", usageStatistics.getCountFor(UsageStatistics.Usage.delete_success)))
                .put("failure", new JSONObject().put("get", usageStatistics.getCountFor(UsageStatistics.Usage.get_failure)).put("insert", usageStatistics.getCountFor(UsageStatistics.Usage.insert_failure)).put("update", usageStatistics.getCountFor(UsageStatistics.Usage.update_failure)).put("delete", usageStatistics.getCountFor(UsageStatistics.Usage.delete_failure)));
        statistics.put("statistics", values);
        result.setHTTPStatusCode(200);
        result.addResult(values);
    }
}
