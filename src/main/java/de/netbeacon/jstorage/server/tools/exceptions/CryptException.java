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
 * This exception should be thrown whenever something with encryption goes wrong
 *
 * @author horstexplorer
 */
public class CryptException extends Exception{

    /*
        Types:
        0   -   CryptTool Not Ready / Not Set Up/ Action Denied
        1   -   Invalid Password
        1x      Encryption Error
         1  -
        2x      Decryption Error
         1  -   Data Is Not Encrypted (JS2-Format)
     */
    private final int type;

    /**
     * Instantiates a new crypt exception.
     *
     * @param type the type
     * @param msg error msg
     */
    public CryptException(int type, String msg){
        super(msg);
        this.type = type;
    }

    /**
     * Get type int.
     *
     * @return the int
     */
    public int getType(){ return type; }

}
