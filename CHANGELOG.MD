# Changelog
This list contains most of the changes which have been made such as bug fixes, new features and breaking changes.

##### Versioning:
> major.minor.patch_counter type

```
major - major changes (lots of new features)  
minor - minor changes (new features)  
patch - bug fixes  
counter - number of total changes
```

## v1.7.3_28
- added a timestamp to update & insert operations (datasets)
- fixed a npe of the notification system
- some code cleanup
## v1.7.2_27
- fixed messages on shutdown
## v1.7.1_26
- minor change in handling connection request for notification socket
- added logging to file
## v1.7.0_25
- added notification handling
## v1.6.0_24
- added 2 new actions
- minor changes to error handling
## v1.5.2_23
- switched from ms to ns scale to calculate rate limits
## v1.5.1_22
- fixed some ssl cert related issue (I hope so at least)
## v1.5.0_21
- some bug fixes
- added feature to enable or disable secure dataset modifications
- added feature to upgrade all dataset within the same table to match the default structure
## v1.4.0_20
This version does not contain new features.
- simplified versioning (@1.4.0)
- improved build env
## v1.3.2_19
- no real changes
## v1.3.2_18
- improved performance
- improved statistics
## v1.3.1_17
- added system stats to hello socket
- added hello socket actions to readme
## v1.3.0_16
- minor improvements
- added hello socket
## v1.2.3_15
- fixed npe occurring when the ip has been banned
- changed and extended ratelimit headers
- unified responses
## v1.2.2_14
- minor improvements, response now should always contain a response body
## v1.2.1_13
- minor improvements
## v1.2.0_12
- improved actions (defined by path, shorter names possible)
- small bug fixes
## v1.1.4_11
- connections get now accepted by a ThreadPoolExecutor
## v1.1.3_10
- some internal optimizations
## v1.1.2_9
- removed tests to rewrite them
- updated usage statistics to work properly
- some minor improvements
- fixed a missing lock
## v1.1.1_8
- fixed null byte padding of encryption
- fixed api permissions
- some other fixes
## v1.1.0_7
- supports database encryption
## v1.0.5_6
- added usage statistics
- some improvements
## v1.0.4_5
- added feature to optimize sharding
- minor improvements
## v1.0.3_4
- improved linkage between objects
- added support to limit objects to s given structure within the same table
- creating password for admin users by default
- minor improvements
## v1.0.2_3
- fixed the issue where no end of headers would be sent
- changed login-mode for InfoAction_BasicInfo to all
## v1.0.1_2
- actions which were supposed to use UPDATE as http method now use PUT
- fixed the issue where reading of the headers also read the content of the body
## v1.0.0_1
Initial Release
## v0.0.0
Note: This version is used as placeholder during initial development
