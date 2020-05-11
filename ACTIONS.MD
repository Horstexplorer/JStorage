# Actions

## Table of Content
[Detailed Description](https://github.com/Horstexplorer/JStorage/tree/master/src/main/java/de/netbeacon/jstorage/server/api/socket/processing/action)
- Cache Actions
- Data Actions
- User Actions
- Other Actions

## Cache Actions

| Action | Description |
--- | ---
Cache Info | Tries to list information for all or a specific cache |
Cache Settings | Tries to change settings for a specific cache Exceptions catched by superordinate processing handler |
Create Cache | Tries to create a specific cache |
Clear Cache | Tries to clear a specific cache |
Delete Cache | Tries to delete the specified cache |
Get Cached Data | Tries to get the selected data from the specified cache |
Delete Cached Data | Tries to delete the selected data from the specified cache |

## Data Actions

| Action | Description |
--- | ---
Create DataBase | Tries to create a specific database |
Create DataTable | Tries to create a specific datatable within the selected database |
Create DataSet  | Tries to create a specific dataset within the selected table from a database |
Create DataType | Tries to create a specific datatype identifier within the selected dataset |
DataBase Info | Tries to list information for all or a specific database |
DataTable Info | Tries to list information for all or a specific data table within a database |
DataSet Info | Tries to list information for a specific database within a within a datatable |
DataSet Settings | Tries to change settings for dataset management |
DataTable Settings | Tries to change settings for a specific datatable within a database |
Delete DataBase | Tries to delete a specific database |
Delete DataTable | Tries to delete a specific datatable within the selected database |
Delete DataSet | Tries to delete a specific dataset datatable within the selected table from a database |
Delete DataType | Tries to delete a specific datatype within the selected dataset |
Get DataSet | Tries to get and return a specific dataset datatable within the selected table from a database |
Get DataType | Tries to get the data from a specific datatype within the selected dataset |

## User Actions

| Action | Description |
--- | ---
User Info | Tries to list information for all or a specific user |
User Settings | Tries to change settings for the current or selected user | 
Create User | Tries to create a new user |
Delete User |  Tries to delete a user |
Change Password | Tries to change the password of the current or selected user |
Get Login Token |  Tries to create a new login token for the current or selected user |

## Other Actions

| Action | Description |
--- | ---
Basic Info | Provides basic information about this JStorage installation |