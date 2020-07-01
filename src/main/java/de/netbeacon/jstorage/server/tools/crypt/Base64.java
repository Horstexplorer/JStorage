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

package de.netbeacon.jstorage.server.tools.crypt;

/**
 * Simple QOL wrap for base64 en/decoding
 *
 * @author horstexplorer
 */
public class Base64 {

    /**
     * Used to encode bytes as base64
     * @param bytes input
     * @return base64 encoded output
     */
    public static byte[] encode(byte[] bytes){
        return java.util.Base64.getEncoder().encode(bytes);
    }

    /**
     * Used to decode base64 encoded bytes
     * @param bytes base64 encoded input
     * @return decoded output
     */
    public static byte[] decode(byte[] bytes){
        return java.util.Base64.getDecoder().decode(bytes);
    }
}
