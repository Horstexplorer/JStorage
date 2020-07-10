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

package de.netbeacon.jstorage.server.hello.socket.processing.action;

import de.netbeacon.jstorage.server.api.socket.APISocket;
import de.netbeacon.jstorage.server.hello.socket.processing.HelloProcessorResult;
import de.netbeacon.jstorage.server.internal.usermanager.object.GlobalPermission;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.meta.SystemStats;
import org.json.JSONObject;

import java.util.HashMap;

/**
 * Used to get extended system stats
 */
public class AdvancedStatisticsAction implements HelloProcessingAction{

    private User user;
    private HelloProcessorResult result;
    private HashMap<String, String> args;


    @Override
    public HelloProcessingAction createNewInstance() {
        return new AdvancedStatisticsAction();
    }

    @Override
    public String getAction() {
        return "advancedstatistics";
    }

    @Override
    public boolean requiresAuth() {
        return true;
    }

    @Override
    public void setup(User user, HelloProcessorResult result, HashMap<String, String> args) {
        this.user = user;
        this.result = result;
        this.args = args;
    }

    @Override
    public void process() {
        JSONObject jsonObject = new JSONObject();

        if(user.hasGlobalPermission(GlobalPermission.Admin) || user.hasGlobalPermission(GlobalPermission.ViewAdvancedStatistics)){
            jsonObject
                    .put("api_load", new JSONObject()
                            .put("simple", SystemStats.getInstance().getAPILoad())
                            .put("queue_capacity_remaining", APISocket.getInstance().getWorkQueueRemainingCapacity())
                            .put("queue_capacity_max",APISocket.getInstance().getWorkQueueMaxCapacity())
                            .put("threadpool_current",APISocket.getInstance().getCurrentPoolSize())
                            .put("threadpool_core",APISocket.getInstance().getCorePoolSize())
                            .put("threadpool_max", APISocket.getInstance().getMaxPoolSize()))
                    .put("host_load", new JSONObject()
                            .put("simple", SystemStats.getInstance().getSystemLoad())
                            .put("cpu_avg", SystemStats.getInstance().getAVGLoad()))
                            .put("mem_free", SystemStats.getInstance().getFreeMemory())
                            .put("mem_max", SystemStats.getInstance().getTotalMemory());
        }

        result.setBodyJSON(jsonObject);
    }
}
