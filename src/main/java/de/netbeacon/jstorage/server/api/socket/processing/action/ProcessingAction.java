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
import de.netbeacon.jstorage.server.internal.usermanager.object.User;
import de.netbeacon.jstorage.server.tools.exceptions.CryptException;
import de.netbeacon.jstorage.server.tools.exceptions.DataStorageException;
import de.netbeacon.jstorage.server.tools.exceptions.GenericObjectException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;

/**
 * Used to unify handling of actions
 *
 * @author horstexplorer
 */
public interface ProcessingAction {

    /*                  PRESETUP                    */

    /**
     * Create new instance processing action.
     *
     * @return the processing action
     */
    ProcessingAction createNewInstance(); // <3

    /**
     * Gets action.
     *
     * @return the action
     */
    String getAction();

    /*                  SETUP                   */

    /**
     * Sets .
     *
     * @param user   the user
     * @param result the result
     * @param args   the args
     */
    void setup(User user, HTTPProcessorResult result, HashMap<String, String> args);

    /**
     * Set payload.
     *
     * @param payload the payload
     */
    default void setPayload(JSONObject payload){};

    /*                  ANALYSIS                    */

    /**
     * Supported http method boolean.
     *
     * @param method the method
     * @return the boolean
     */
    boolean supportedHTTPMethod(String method);

    /**
     * Required arguments list.
     *
     * @return the list
     */
    List<String> requiredArguments();

    /**
     * Requires data boolean.
     *
     * @return the boolean
     */
    default boolean requiresData(){return false;}

    /**
     * Login mode is supported boolean.
     *
     * @param loginMode the login mode
     * @return the boolean
     */
    default boolean loginModeIsSupported(int loginMode){return 1 == loginMode;}

    /**
     * User has permission boolean.
     *
     * @return the boolean
     */
    boolean userHasPermission();

    /*                  ACTIVATE                   */

    /**
     * Process.
     *
     * @throws DataStorageException   the data storage exception
     * @throws GenericObjectException the generic object exception
     * @throws CryptException exception related to encryption
     */
    void process() throws DataStorageException, GenericObjectException, CryptException;

}
