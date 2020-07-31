# JStorage
###### JSON based data storage & cache system with web api access
> Version: 1.7.3_28


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

### Start Parameters
Param | Description
--- | ---
-encryptsetup:<true/false> | tries to set up encryption
-mode:<default> | changes the mode of this instance

### Setup Encryption
Use the start parameter explained above to enable encryption.
You will be prompted to enter a password which will be used to en-/decrypt the data. From now on this prompt will show up every time the software starts.
After setting up the password you are able to activate encryption for each database individually.

#### ! Notes !
- Remember the password, there is no way to reset it
- Choose a good password, as it cannot be changed later (that easily)
- The data will not be encrypted right away - only shards which had been loaded and stored again will be
- To disable encryption make sure to disable encryption for each database and make sure the data has been decrypted, then just delete the js2config file

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