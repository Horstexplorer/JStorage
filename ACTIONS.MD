# Actions

## Table of Content
- Cache Actions
- Data Actions
- User Actions
- Other Actions

## Cache Actions

| Action | Description | Requirements |
--- | --- | --- 
Cache Info | Tries to list information for all or a specific cache | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/CacheAction_CacheInfo.html) |
Cache Settings | Tries to change settings for a specific cache Exceptions catched by superordinate processing handler | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/CacheAction_CacheSettings.html) |
Create Cache | Tries to create a specific cache | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/CacheAction_CreateCache.html) |
Clear Cache | Tries to clear a specific cache | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/CacheAction_ClearCache.html)|
Delete Cache | Tries to delete the specified cache | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/CacheAction_DeleteCache.html) |
Get Cached Data | Tries to get the selected data from the specified cache | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/CacheAction_GetCachedData.html) |
Delete Cached Data | Tries to delete the selected data from the specified cache | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/CacheAction_DeleteCachedData.html) |

## Data Actions

| Action | Description | Requirements |
--- | --- | ---
Create DataBase | Tries to create a specific database | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_CreateDataBase.html) |
Create DataTable | Tries to create a specific datatable within the selected database | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_CreateDataTable.html) |
Create DataSet  | Tries to create a specific dataset within the selected table from a database | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_CreateDataSet.html) |
Create DataType | Tries to create a specific datatype identifier within the selected dataset | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_CreateDataType.html) |
DataBase Info | Tries to list information for all or a specific database | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_DataBaseInfo.html) |
DataTable Info | Tries to list information for all or a specific data table within a database | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_DataTableInfo.html) |
DataSet Info | Tries to list information for a specific database within a within a datatable | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_DataSetInfo.html) |
DataSet Settings | Tries to change settings for dataset management | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_DataSetSettings.html) |
DataTable Settings | Tries to change settings for a specific datatable within a database | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_DataTableSettings.html) |
Delete DataBase | Tries to delete a specific database | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_DeleteDataBase.html) |
Delete DataTable | Tries to delete a specific datatable within the selected database | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_DeleteDataTable.html) |
Delete DataSet | Tries to delete a specific dataset datatable within the selected table from a database | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_DeleteDataSet.html) |
Delete DataType | Tries to delete a specific datatype within the selected dataset | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_DeleteDataType.html) |
Get DataSet | Tries to get and return a specific dataset datatable within the selected table from a database | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_GetDataSet.html) |
Get DataType | Tries to get the data from a specific datatype within the selected dataset | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/DataAction_GetDataType.html) |

## User Actions

| Action | Description | Requirements |
--- | --- | ---
User Info | Tries to list information for all or a specific user | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/UserAction_UserInfo.html) |
User Settings | Tries to change settings for the current or selected user | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/UserAction_UserSettings.html) |
Create User | Tries to create a new user | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/UserAction_CreateUser.html) |
Delete User |  Tries to delete a user | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/UserAction_DeleteUser.html) |
Change Password | Tries to change the password of the current or selected user | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/UserAction_UserChangePassword.html) |
Get Login Token |  Tries to create a new login token for the current or selected user | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/UserAction_UserGetNewLoginToken.html) |

## Other Actions

| Action | Description | Requirements
--- | --- | ---
Basic Info | Provides basic information about this JStorage installation | [View Docs](https://horstexplorer.github.io/JStorage/de/netbeacon/jstorage/server/api/socket/processing/action/InfoAction_BasicInfo.html) |