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
 * Used for general error handling
 *
 * Error types: </br>
 * 000 - Undefined Error </br>
 * 1xx - Not Ready </br>
 * 0 - </br>
 * 0 - Undefined </br>
 * 1 - Loading </br>
 * 2 - Unloading </br>
 * 2xx - Not Found </br>
 * 0 - </br>
 * 0 - Undefined </br>
 * 3xx - Already Existing </br>
 * 0 - </br>
 * 0 - Undefined </br>
 * 4xx - Format Exception </br>
 * 0 - </br>
 * 0 - Undefined </br>
 *
 * @author horstexplorer
 */
public class GenericObjectException extends Exception {

    private final int type;

    /**
     * Instantiates a new Generic object exception.
     *
     * @param type    the type
     * @param message the message
     */
    public GenericObjectException(int type, String message){
        super(message);
        this.type = type;
    }

    /**
     * Get type int.
     *
     * @return the int
     */
    public int getType(){
        return type;
    }
}
