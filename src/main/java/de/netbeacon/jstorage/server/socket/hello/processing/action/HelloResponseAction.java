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

import java.util.HashMap;

/**
 * This action returns a simple string (hello) for each request. This can be used to check if the service is on / token is correct etc
 */
public class HelloResponseAction implements HelloProcessingAction{

    private HelloProcessorResult result;

    @Override
    public HelloProcessingAction createNewInstance() {
        return new HelloResponseAction();
    }

    @Override
    public String getAction() {
        return "hello";
    }

    @Override
    public void setup(User user, HelloProcessorResult result, HashMap<String, String> args) {
        this.result = result;
    }

    @Override
    public void process() {
        result.setBodyPage("hello");
    }
}
