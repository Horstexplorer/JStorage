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
 * <p>
 * Error types:
 * 000 - Undefined Error
 * 1xx - Not Ready
 * 0 -
 * 0 - Undefined
 * 1 - Loading
 * 2 - Unloading
 * 2xx - Not Found
 * 0 -
 * 0 - Undefined
 * 3xx - Already Existing
 * 0 -
 * 0 - Undefined
 * 4xx - Format Exception
 * 0 -
 * 0 - Undefined
 *
 * @author horstexplorer
 */
public class GenericObjectException extends Exception {

    private int type;

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
