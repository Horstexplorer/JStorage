# JStorage
###### JSON based data storage & cache system with web api access
> Version: 1.1.0_7 D


## Table of Content
- [Setup](#setup)
- [Usage](#usage)
- [Actions](./ACTIONS.MD)
- [Known Issues](#known-issues)
- [Future releases](#future-releases)
- [Changelog](./CHANGELOG.MD)

## Setup
1. Clone Git
2. Get Dependencies (Gradle)
3. Build Jar (ShadowJar)
4. Execute
5. Configure
6. Use

During first run an administrator user will be created, and the login credentials for it will be shown.
First run might result in some exceptions as required files may need to be configured. Config files should be placed within the ./jstorage/config directory

## Usage
The application can be used via http request with predefined actions  
For a list of all supported actions see [Actions](./ACTIONS.MD)

Limits:
- Header: 8kb
- Body: 8mb

The request may contain following information/headers:

URL structure
- `https://domain.tld/<action>?arg=var&arg2=var...`

Auth:  
- via token header `Token: logintoken`
- via basic auth `Authorization: Basic <Base64_user:pass>`

Actions requiring JSON payload/data
- `Content-Length: `
- `Content-Type: application/json`

## Known-Issues
Currently, there are no known issues. This list might not be updated yet, so please check the issue tracker.

## Future-Releases
Future releases might contain following features:
- Mode: Node - let multiple storages act like one through a proxy
- DB: Update lock for data types might contain subtypes
- DB: Advanced Searching Features
- User: more secure way of building login tokens
- User: better permissions
- WebSocket: more nicely shutdown