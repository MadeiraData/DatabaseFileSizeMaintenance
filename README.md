# DatabaseFileSizeMaintenance

**APPLIES TO:** ![Yes](media/yes-icon.png)SQL Server ![Yes](media/yes-icon.png)Azure SQL Database (Managed Instance only) ![No](media/no-icon.png)Azure Synapse Analytics (SQL DW) ![No](media/no-icon.png)Parallel Data Warehouse 

-------

This project is for development of a stored procedure, which is to be used as an add-on to [Ola Hallengren's maintenance solution](https://ola.hallengren.com), and retains the same standards and conventions in its implementation of parameters.

The `DatabaseFileSizeMaintenance` procedure focuses on managing database file size growth and shrink, based on specified parameters.

## Download

Download [DatabaseFileSizeMaintenance.sql](DatabaseFileSizeMaintenance.sql).

## Syntax

For syntax conventions info, please see [Transact-SQL Syntax Conventions](https://docs.microsoft.com/en-us/sql/t-sql/language-elements/transact-sql-syntax-conventions-transact-sql).

```
EXEC dbo.DatabaseFileSizeMaintenance
   @Databases				= N'[ databases | ALL_DATABASES | SYSTEM_DATABASES | USER_DATABASES | AVAILABILITY_GROUP_DATABASES [ ,...n ] ]'
[ ,@UsedSpacePercentHighThreshold	= used_space_percent_high_threshold ]
[ ,@UsedSpacePercentLowThreshold	= used_space_percent_low_threshold ]
[ ,@MinFileSizeToShrinkMB		= minimum_file_size_to_shrink_mb ]
[ ,@MinDatabaseAgeInDays		= minimum_database_age_in_days ]
[ ,@TargetShrinkSizePercent		= target_shrink_size_percent ]
[ ,@MinTargetShrinkSizeMB		= minimum_target_shrink_size_mb ]
[ ,@ShrinkAllowReorganize		= { 'N' | 'Y' } ]
[ ,@DatabaseOrder			= { NULL | 'DATABASE_SIZE_ASC' | 'DATABASE_SIZE_DESC' | 'LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC' | 'LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC' | 'DATABASE_NAME_ASC' | 'DATABASE_NAME_DESC' } ]
[ ,@DatabasesInParallel			= { 'N' | 'Y' } ]
[ ,@LogToTable				= { 'N' | 'Y' } ]
[ ,@Execute				= { 'N' | 'Y' } ]
```

## Arguments

### `@Databases`

Select databases.

The keywords `SYSTEM_DATABASES`, `USER_DATABASES`, `ALL_DATABASES`, and `AVAILABILITY_GROUP_DATABASES` are supported.

The hyphen character (`-`) is used to exclude databases, and the percent character (`%`) is used for wildcard selection.

All of these operations can be combined by using the comma (`,`).

Value|Description
---|---
SYSTEM_DATABASES|All system databases (master, msdb, and model)
USER_DATABASES|All user databases
ALL_DATABASES|All databases
AVAILABILITY_GROUP_DATABASES|All databases in availability groups
USER_DATABASES, -AVAILABILITY_GROUP_DATABASES|All user databases that are not in availability groups
Db1|The database Db1
Db1, Db2|The databases Db1 and Db2
USER_DATABASES, -Db1|All user databases, except Db1
%Db%|All databases that have “Db” in the name
%Db%, -Db1|All databases that have “Db” in the name, except Db1
ALL_DATABASES, -%Db%|All databases that do not have “Db” in the name

### `@UsedSpacePercentHighThreshold`

If used space in a file is higher than this, then will grow the file.

Value is in percent (0..100)

Default is `90` percent.

### `@UsedSpacePercentLowThreshold`

If used space in a file is smaller than this, then will shrink the file.

Value is in percent (0..100)

Default is `10` percent.

### `@MinFileSizeToShrinkMB`

If file size is smaller than this, then will NOT shrink.

Value is in MB.

Default is `50000` MB (i.e. 50 GB).

### `@MinDatabaseAgeInDays`

Databases must be at least this old in order to be checked.

Value is in days.

Default is `30` days.

### `@TargetShrinkSizePercent`

When shrinking, try to shrink down to this percentage of the current file size.

Value is in percent (0..100).

Default is `33` percent.

### `@MinTargetShrinkSizeMB`

When shrinking files, prevent shrinking to a size smaller than this.

Value is in MB.

Default is `64` MB.

### `@ShrinkAllowReorganize`

Set whether to allow reorganizing pages during shrink.

Value|Description
---|---
Y|Allow reorganizing pages during shrink. This is the **default**.
N|Do not allow reorganizing pages during shrink.

**NOTE:** Reorganizing pages during shrink is potentially a very resource-intensive process.
Please try to schedule execution of this process during low workload hours.

### `@DatabaseOrder`

Specify the database order.

Value|Description
---|---
NULL|The order that the databases have been specified in. Then ascending by the database name. This is the **default**.
DATABASE_NAME_ASC|Ascending by the database name
DATABASE_NAME_DESC|Descending by the database name
DATABASE_SIZE_ASC|Ascending by the database size
DATABASE_SIZE_DESC|Descending by the database size
LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC|Ascending by `log_since_last_log_backup_mb` in `sys.dm_db_log_stats`
LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC|Descending by `log_since_last_log_backup_mb` in `sys.dm_db_log_stats`

### `@DatabasesInParallel`

Process databases in parallel.

Value|Description
---|---
Y|Process databases in parallel.
N|Process databases one at a time. This is the **default**.

You can process databases in parallel by creating multiple jobs with the same parameters, and add the parameter `@DatabasesInParallel = 'Y'`.

### `@LogToTable`

Log commands to the table dbo.CommandLog.

Value|Description
---|---
Y|Log commands to the table.
N|Do not log commands to the table. This is the **default**.

### `@Execute`

Execute commands. By default, the commands are executed normally. If this parameter is set to N, then the commands are printed only.

Value|Description
---|---
Y|Execute commands. This is the **default**.
N|Only print commands.

## Remarks

The procedure requires [Ola Hallengren's CommandExecute procedure](https://ola.hallengren.com/scripts/CommandExecute.sql) in order to run.

If `@LogToTable = 'Y'` is specified, then you must also have the [CommandLog](https://ola.hallengren.com/scripts/CommandLog.sql) table in the same database.

If `@DatabasesInParallel = 'Y'` is specified, then you must also have the [Queue](https://ola.hallengren.com/scripts/Queue.sql) and [QueueDatabase](https://ola.hallengren.com/scripts/QueueDatabase.sql) tables.

## Examples

TBA

## License

This project is licensed under the same open-source project as [Ola Hallengren's maintenance solution](https://ola.hallengren.com/license.html).

The DatabaseFileSizeMaintenance procedure is released under the [MIT license](LICENSE), a popular and widely used open source license.

Copyright (c) 2020 Madeira Data Solutions

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
