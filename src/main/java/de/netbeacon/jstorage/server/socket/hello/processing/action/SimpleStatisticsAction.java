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

package de.netbeacon.jstorage.server.socket.hello.processing.action;

import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.socket.hello.processing.HelloProcessorResult;
import de.netbeacon.jstorage.server.tools.meta.SystemStats;
import org.json.JSONObject;

import java.util.HashMap;

/**
 * Used to get simplified system stats
 */
public class SimpleStatisticsAction implements HelloProcessingAction{

    private User user;
    private HelloProcessorResult result;
    private HashMap<String, String> args;

    @Override
    public HelloProcessingAction createNewInstance() {
        return new SimpleStatisticsAction();
    }

    @Override
    public String getAction() {
        return "simplestatistics";
    }

    @Override
    public void setup(User user, HelloProcessorResult result, HashMap<String, String> args) {
        this.user = user;
        this.result = result;
        this.args = args;
    }

    @Override
    public void process() {
        JSONObject jsonObject = new JSONObject()
                .put("api_load", SystemStats.getInstance().getAPILoad())
                .put("host_load", SystemStats.getInstance().getSystemLoad());

        result.setBodyJSON(jsonObject);
    }
}
