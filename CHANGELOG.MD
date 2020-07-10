# Changelog
This list contains most of the changes which have been made such as bug fixes, new features and breaking changes.

##### Versioning:
> major.minor.patch_counter type

```
major - a lot of breaking changes  
minor - might break things a little  
patch - should not break things; might add new features  
counter - shows the number of total changes
```

Types which may be used:
- D - Development
- T - Testing
- R - Release

## v1.3.1_17 R
- added system stats to hello socket
- added hello socket actions to readme
## v1.3.0_16 R
- minor improvements
- added hello socket
## v1.2.3_15 R
- fixed npe occurring when the ip has been banned
- changed and extended ratelimit headers
- unified responses
## v1.2.2_14 R
- minor improvements, response now should always contain a response body
## v1.2.1_13 R
- minor improvements
## v1.2.0_12 R
- improved actions (defined by path, shorter names possible)
- small bug fixes
## v1.1.4_11 D
- connections get now accepted by a ThreadPoolExecutor
## v1.1.3_10 R
- some internal optimizations
## v1.1.2_9 D
- removed tests to rewrite them
- updated usage statistics to work properly
- some minor improvements
- fixed a missing lock
## v1.1.1_8 R
- fixed null byte padding of encryption
- fixed api permissions
- some other fixes
## v1.1.0_7 D
- supports database encryption
## v1.0.5_6 D
- added usage statistics
- some improvements
## v1.0.4_5 R
- added feature to optimize sharding
- minor improvements
## v1.0.3_4 R
- improved linkage between objects
- added support to limit objects to s given structure within the same table
- creating password for admin users by default
- minor improvements
## v1.0.2_3 R
- fixed the issue where no end of headers would be sent
- changed login-mode for InfoAction_BasicInfo to all
## v1.0.1_2 R
- actions which were supposed to use UPDATE as http method now use PUT
- fixed the issue where reading of the headers also read the content of the body
## v1.0.0_1 R
Initial Release
## v0.0.0 D
Note: This version is used as placeholder during initial development
