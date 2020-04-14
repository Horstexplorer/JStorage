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

package de.netbeacon.jstorage.server.tools.exceptions;

/**
 * This exception should be thrown when an exception occurs during shutdown of an object. This might be used for top level objects only.
 *
 * @author horstexplorer
 */
public class ShutdownException extends Exception {

    /**
     * Instantiates a new Shutdown exception.
     *
     * @param errorMessage the error message
     */
    public ShutdownException(String errorMessage){
        super("Shutdown Failed, Data May Be Lost Or Corrupted: "+errorMessage);
    }
}
