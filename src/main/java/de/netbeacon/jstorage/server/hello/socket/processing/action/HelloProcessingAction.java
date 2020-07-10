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

import de.netbeacon.jstorage.server.hello.socket.processing.HelloProcessorResult;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;

import java.util.HashMap;

/**
 * Used to unify handling of actions
 *
 * @author horstexplorer
 */
public interface HelloProcessingAction {

    /*                  PRESETUP                    */

    /**
     * Create new instance processing action.
     *
     * @return the processing action
     */
    HelloProcessingAction createNewInstance(); // <3

    /**
     * Gets action.
     *
     * @return the action
     */
    String getAction();

    /**
     * Used to determine if an auth is required
     * @return boolean
     */
    default boolean requiresAuth(){ return false; }

    /**
     * Setup
     */
    void setup(User user, HelloProcessorResult result, HashMap<String, String> args);

    /**
     * Process
     */
    void process();

}
