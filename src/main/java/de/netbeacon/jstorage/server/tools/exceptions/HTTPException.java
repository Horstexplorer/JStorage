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
 * This exception should be thrown when a direct feedback to a web client is required
 *
 * @author horstexplorer
 */
public class HTTPException extends Exception{

    private final int statusCode;
    private String additionalInformation;

    /**
     * Instantiates a new Http exception.
     *
     * @param statusCode the status code
     */
    public HTTPException(int statusCode){
        super("generic_placeholder");
        this.statusCode = statusCode;
    }

    /**
     * Instantiates a new Http exception.
     *
     * @param statusCode            the status code
     * @param additionalInformation the additional information
     */
    public HTTPException(int statusCode, String additionalInformation){
        super("generic_placeholder");
        this.statusCode = statusCode;
        this.additionalInformation = additionalInformation;
    }

    /**
     * Get status code int.
     *
     * @return the int
     */
    public int getStatusCode(){ return statusCode; }

    /**
     * Get additional information string.
     *
     * @return the string
     */
    public String getAdditionalInformation(){ return additionalInformation; }

    @Override
    public String getMessage(){
        switch(statusCode){
            case 100:
                return "Continue";
            case 101:
                return "Switching Protocols";
            case 102:
                return "Processing";

            case 200:
                return "OK";
            case 201:
                return "Created";
            case 202:
                return "Accepted";
            case 203:
                return "Non-Authoritative Information";
            case 204:
                return "No Content";
            case 205:
                return "Reset Content";
            case 206:
                return "Partial Content";
            case 207:
                return "Multi-Status";
            case 208:
                return "Already Reported";
            case 226:
                return "IM Used";

            case 300:
                return "Multiple Choices";
            case 301:
                return "Moved Permanently";
            case 302:
                return "Found (Moved Temporarily)";
            case 303:
                return "See Other";
            case 304:
                return "Not Modified";
            case 305:
                return "Use Proxy";
            case 307:
                return "Temporary Redirect";
            case 308:
                return "Permanent Redirect";

            case 400:
                return "Bad Request";
            case 401:
                return "Unauthorized";
            case 402:
                return "Payment Required";
            case 403:
                return "Forbidden";
            case 404:
                return "Not Found";
            case 405:
                return "Method Not Allowed";
            case 406:
                return "Not Acceptable";
            case 407:
                return "Proxy Authentication Required";
            case 408:
                return "Request Timeout";
            case 409:
                return "Conflict";
            case 410:
                return "Gone";
            case 411:
                return "Length Required";
            case 412:
                return "Precondition Failed";
            case 413:
                return "Request Entity Too Large";
            case 414:
                return "URI Too Long";
            case 415:
                return "Unsupported Media Type";
            case 416:
                return "Requested range not satisfiable";
            case 417:
                return "Expectation Failed";
            case 420:
                return "Policy Not Fulfilled";
            case 421:
                return "Misdirected Request";
            case 422:
                return "Unprocessable Entity";
            case 423:
                return "Locked";
            case 424:
                return "Failed Dependency";
            case 426:
                return "Upgrade Required";
            case 428:
                return "Precondition Required";
            case 429:
                return "Too Many Requests";
            case 431:
                return "Request Header Fields Too Large";
            case 451:
                return "\tUnavailable For Legal Reasons";
            case 418:
                return "I’m a teapot";
            case 425:
                return "Unordered Collection";
            case 444:
                return "No Response";
            case 449:
                return "The request should be retried after doing the appropriate action";
            case 499:
                return "Client Closed Request";

            case 500:
                return "Internal Server Error";
            case 501:
                return "Not Implemented";
            case 502:
                return "Bad Gateway";
            case 503:
                return "Service Unavailable";
            case 504:
                return "Gateway Timeout";
            case 505:
                return "HTTP Version not supported";
            case 506:
                return "Variant Also Negotiates";
            case 507:
                return "Insufficient Storage";
            case 508:
                return "Loop Detected";
            case 509:
                return "Bandwidth Limit Exceeded";
            case 510:
                return "Not Extended";
            case 511:
                return "Network Authentication Required";

            default:
                return "Internal Server Error";
        }
    }

}
