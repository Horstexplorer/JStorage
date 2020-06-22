# Actions

## Table of Content
[Detailed Description](https://github.com/Horstexplorer/JStorage/tree/master/src/main/java/de/netbeacon/jstorage/server/api/socket/processing/action)
- Cache Actions
- Data Actions
- User Actions
- Other Actions

## Cache Actions

| Action | Full Path | Description |
--- | --- | ---
Cache Info | cache/info | Tries to list information for all or a specific cache |
Cache Settings | cache/settings | Tries to change settings for a specific cache Exceptions catched by superordinate processing handler |
Create Cache | cache/mng/create | Tries to create a specific cache |
Clear Cache | cache/mng/clear | Tries to clear a specific cache |
Delete Cache | cache/mng/delete | Tries to delete the specified cache |
Get Cached Data | cache/data/get | Tries to get the selected data from the specified cache |
Delete Cached Data | cache/data/delete | Tries to delete the selected data from the specified cache |
Create Cached Data | cache/data/create | Tries to create a specific dataset within the selected cache |

## Data Actions

| Action | Full Path | Description |
--- | --- | ---
Create DataBase | data/db/create | Tries to create a specific database |
Create DataTable | data/db/table/create | Tries to create a specific datatable within the selected database |
Create DataSet  | data/db/table/dataset/create | Tries to create a specific dataset within the selected table from a database |
Create DataType | data/db/table/dataset/datatype/create | Tries to create a specific datatype identifier within the selected dataset |
DataBase Info | data/db/info | Tries to list information for all or a specific database |
DataTable Info | data/db/table/info | Tries to list information for all or a specific data table within a database |
DataSet Info | data/db/table/dataset/info | Tries to list information for a specific database within a within a datatable |
DataSet Settings | data/db/table/dataset/settings | Tries to change settings for dataset management |
DataTable Settings | data/db/table/settings | Tries to change settings for a specific datatable within a database |
DataBase Settings | data/db/settings | Tries to change settings for a specific database |
Delete DataBase | data/db/delete | Tries to delete a specific database |
Delete DataTable | data/db/table/delete | Tries to delete a specific datatable within the selected database |
Delete DataSet | data/db/table/dataset/delete | Tries to delete a specific dataset datatable within the selected table from a database |
Delete DataType | data/db/table/dataset/datatype/delete | Tries to delete a specific datatype within the selected dataset |
Get DataSet | data/db/table/dataset/get | Tries to get and return a specific dataset datatable within the selected table from a database |
Get DataType | data/db/table/dataset/datatype/get | Tries to get the data from a specific datatype within the selected dataset |

## User Actions

| Action | Full Path | Description |
--- | --- | ---
User Info | user/info | Tries to list information for all or a specific user |
User Settings | user/settings | Tries to change settings for the current or selected user | 
Create User | user/mng/create | Tries to create a new user |
Delete User | user/mng/delete |  Tries to delete a user |
Change Password | user/mng/changepw | Tries to change the password of the current or selected user |
Get Login Token | user/mng/getnewlogintoken |  Tries to create a new login token for the current or selected user |

## Other Actions

| Action | Full Path | Description |
--- | --- | ---
Basic Info | info/basic/info | Provides basic information about this JStorage installation |
Statistics | info/stats/statistics | Provides usage statistics (from the last 10 minutes) of the selected object