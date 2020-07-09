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

package de.netbeacon.jstorage.server.hello.socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocket;
import java.io.BufferedReader;
import java.io.BufferedWriter;


/**
 * The type hello socket handler.
 * @author horstexplorer
 */
public class HelloSocketHandler implements Runnable {

    private final SSLSocket socket;
    private BufferedReader bufferedReader;
    private BufferedWriter bufferedWriter;
    private final String ip;

    private final Logger logger = LoggerFactory.getLogger(HelloSocketHandler.class);

    /**
     * Instantiates a new Hello socket handler.
     *
     * @param socket the socket
     */
    protected HelloSocketHandler(SSLSocket socket){
        this.socket = socket;
        logger.info("Handling Connection From "+socket.getRemoteSocketAddress());
        this.ip = socket.getRemoteSocketAddress().toString().substring(1, socket.getRemoteSocketAddress().toString().indexOf(":"));
    }


    @Override
    public void run() {

    }
}
