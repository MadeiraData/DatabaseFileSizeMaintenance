SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- EXEC [DatabaseFileSizeMaintenance] 'USER_DATABASES', @MinFileSizeForShrinkMB = 50, @ShrinkIntervalMB = 5
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DatabaseFileSizeMaintenance]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[DatabaseFileSizeMaintenance] AS SET NOCOUNT ON;'
END
GO
/*
License
-----------
THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
ALTER PROCEDURE [dbo].[DatabaseFileSizeMaintenance]

-- parameters controlling WHAT to shrink or grow
 @Databases				NVARCHAR(MAX)	= NULL		-- used for specifying which databases should be managed
,@FileTypes				CHAR(4)		= 'ALL'		-- used for specifying the file types we want to manage (ALL | ROWS | LOG)

-- parameters controlling WHEN to shrink or grow
,@UsedSpacePercentHighThreshold	        INT		= 95		-- if the file used space percentage is higher than this, will grow file
,@UsedSpacePercentLowThreshold	        INT		= 10		-- if the file used space percentage is smaller than this, will try to shrink file
,@MinFileSizeForShrinkMB		INT		= 50000		-- if size smaller than this (in MB), will NOT shrink
,@MinDatabaseAgeInDays			INT		= 30		-- databases must be at least this old (in days) to be checked

-- parameters controlling TARGET shrink size
,@ShrinkToTargetSizePercent		INT		= NULL		-- when shrinking, try to shrink down to this percentage of current file size
,@ShrinkToMinPercentFree                INT		= 80		-- when shrinking, try to leave at least this much percent free in current file. Must be smaller than @UsedSpacePercentHighThreshold.
,@ShrinkToMaxSizeMB			INT		= 64		-- prevent shrinking to a size smaller than this (in MB)
,@ShrinkAllowReorganize			CHAR(1)		= 'Y'		-- set whether to allow reorganizing pages during shrink

-- parameters controlling shrink INTERVALS
,@ShrinkIntervalMB			INT		= NULL		-- specify the interval size (in MB) for each shrink. Leave NULL to shrink the file in a single interval
,@DelayBetweenShrinks			VARCHAR(15)	= '00:00:00.5'  -- delay to wait between shrink iterations (in 'hh:mm[[:ss].mss]' format). Leave NULL to disable delay between iterations
,@RegrowOnError5240			BIT		= 1		-- common error 5240 may be resolved by temporarily increasing the file size before shrinking it again.

-- parameters for implementing AG recovery queue check
,@AGReplicaLinkedServer	                SYSNAME		= NULL		-- linked Server name of an AG replica to check. Leave as NULL to ignore.
,@MaxReplicaRecoveryQueue               INT		= 20000		-- max recovery queue of AG replica (in KB). Use this to prevent overload on the AG.
,@RecoveryQueueSeverity                 INT		= 16		-- error severity to raise when @MaxReplicaRecoveryQueue is breached. 

-- common DatabaseMaintenance parameters
,@Updateability				NVARCHAR(max)	= 'ALL'
,@TimeLimit				INT		= NULL
,@LockTimeout				INT		= NULL
,@LockMessageSeverity			INT		= 16
,@StringDelimiter			NVARCHAR(max)	= ','
,@DatabaseOrder				NVARCHAR(max)	= NULL
,@DatabasesInParallel			NVARCHAR(3)	= 'N'
,@LogToTable				NVARCHAR(3)	= 'Y'
,@Execute				NVARCHAR(3)	= 'Y'
AS
BEGIN
SET NOCOUNT, ARITHABORT, XACT_ABORT, ANSI_NULLS ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

  DECLARE @StartMessage nvarchar(max)
  DECLARE @EndMessage nvarchar(max)
  DECLARE @DatabaseMessage nvarchar(max)
  DECLARE @ErrorMessage nvarchar(max)

  DECLARE @StartTime datetime
  DECLARE @SchemaName nvarchar(max)
  DECLARE @ObjectName nvarchar(max)
  DECLARE @VersionTimestamp nvarchar(max)
  DECLARE @Parameters nvarchar(max)

  DECLARE @Version numeric(18,10)
  DECLARE @HostPlatform nvarchar(max)
  DECLARE @DirectorySeparator nvarchar(max)
  DECLARE @AmazonRDS bit

  DECLARE @Updated bit

  DECLARE @Cluster nvarchar(max)

  DECLARE @DefaultDirectory nvarchar(4000)

  DECLARE @QueueID int
  DECLARE @QueueStartTime datetime

  DECLARE @CurrentRootDirectoryID int
  DECLARE @CurrentRootDirectoryPath nvarchar(4000)

  DECLARE @CurrentDBID int
  DECLARE @CurrentDatabaseName nvarchar(max)

  DECLARE @CurrentDatabaseID int
  DECLARE @CurrentUserAccess nvarchar(max)
  DECLARE @CurrentIsReadOnly bit
  DECLARE @CurrentDatabaseState nvarchar(max)
  DECLARE @CurrentInStandby bit
  DECLARE @CurrentCreateDate datetime

DECLARE @SelectedDatabases TABLE (DatabaseName nvarchar(max),
                                  DatabaseType nvarchar(max),
                                  AvailabilityGroup nvarchar(max),
                                  StartPosition int,
                                  Selected bit)
								  
DECLARE @SelectedAvailabilityGroups TABLE (AvailabilityGroupName nvarchar(max),
                                             StartPosition int,
                                             Selected bit)



DECLARE @tmpAvailabilityGroups TABLE (ID int IDENTITY PRIMARY KEY,
                                      AvailabilityGroupName nvarchar(max),
                                      StartPosition int,
                                      Selected bit)


DECLARE @tmpDatabasesAvailabilityGroups TABLE (DatabaseName nvarchar(max),
                                               AvailabilityGroupName nvarchar(max))


DECLARE @tmpDatabases TABLE (ID int IDENTITY,
                             DatabaseName nvarchar(max),
                             DatabaseNameFS nvarchar(max),
                             DatabaseType nvarchar(max),
                             AvailabilityGroup bit,
                             StartPosition int,
                             DatabaseSize bigint,
                             LogSizeSinceLastLogBackup float,
                             [Order] int,
                             Selected bit,
                             Completed bit,
                             PRIMARY KEY(Selected, Completed, [Order], ID))


  DECLARE @CurrentCommandOutput01 int

  DECLARE @Error int
  DECLARE @ReturnCode int

  DECLARE @EmptyLine nvarchar(max)

  SET @Error = 0
  SET @ReturnCode = 0

  SET @EmptyLine = CHAR(9)


IF ISNULL(@UsedSpacePercentHighThreshold,0) NOT BETWEEN 1 AND 100
	RAISERROR(N'Invalid value for @UsedSpacePercentHighThreshold: %d, must be between 1 and 100.', 16, 1, @UsedSpacePercentHighThreshold);
	
IF ISNULL(@UsedSpacePercentLowThreshold,0) NOT BETWEEN 1 AND @UsedSpacePercentHighThreshold
	RAISERROR(N'Invalid value for @UsedSpacePercentLowThreshold: %d, must be between 1 and %d.', 16, 1, @UsedSpacePercentLowThreshold,@UsedSpacePercentHighThreshold);
	
IF ISNULL(@ShrinkToTargetSizePercent,1) NOT BETWEEN 1 AND 100
	RAISERROR(N'Invalid value for @ShrinkToTargetSizePercent: %d, must be between 1 and 100.', 16, 1, @ShrinkToTargetSizePercent);

IF ISNULL(@ShrinkToMinPercentFree,1) NOT BETWEEN 1 AND 100
	RAISERROR(N'Invalid value for @ShrinkToMinPercentFree: %d, must be between 1 and 100.', 16, 1, @ShrinkToMinPercentFree);

IF @ShrinkToMinPercentFree IS NOT NULL AND @ShrinkToTargetSizePercent IS NOT NULL AND @ShrinkToMinPercentFree > @ShrinkToTargetSizePercent
	RAISERROR(N'Invalid value for @ShrinkToMinPercentFree: %d, must be lower than or equal to @ShrinkToTargetSizePercent: %d.', 16, 1, @ShrinkToMinPercentFree, @ShrinkToTargetSizePercent);

IF ISNULL(@MinDatabaseAgeInDays,0) < 0
	RAISERROR(N'Invalid value for @MinDatabaseAgeInDays: %d, must be 0 or higher.', 16, 1, @MinDatabaseAgeInDays);

IF ISNULL(@ShrinkToMaxSizeMB,1) < 1
	RAISERROR(N'Invalid value for @ShrinkToMaxSizeMB: %d, must be 1 or higher.', 16, 1, @ShrinkToMaxSizeMB);

DECLARE @tmpDatabaseFiles AS TABLE
(
    dbName SYSNAME,
    dbFileName SYSNAME,
    [UsedSpaceIn_%] FLOAT,
    IsFileGrow_perc BIT NOT NULL,
    Growth INT,
    FileSizeInMB INT
)


DECLARE @DB_name		VARCHAR(500),
        @DB_FileName		SYSNAME,
	@SpaceUsedPercent	DECIMAL(10,2),
	@isPercentage		BIT,
	@FileAutoGrowth		INT,
	@FileSizeInMB		INT, 
	@Qry			NVARCHAR(MAX)

		

  SET @Version = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - 1) + '.' + REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)))),'.','') AS numeric(18,10))

  ----------------------------------------------------------------------------------------------------
  --// Select databases                                                                           //--
  ----------------------------------------------------------------------------------------------------

  SET @Databases = REPLACE(@Databases, CHAR(10), '')
  SET @Databases = REPLACE(@Databases, CHAR(13), '')

  WHILE CHARINDEX(', ',@Databases) > 0 SET @Databases = REPLACE(@Databases,', ',',')
  WHILE CHARINDEX(' ,',@Databases) > 0 SET @Databases = REPLACE(@Databases,' ,',',')

  SET @Databases = LTRIM(RTRIM(@Databases));

  WITH Databases1 (StartPosition, EndPosition, DatabaseItem) AS
  (
  SELECT 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(',', @Databases, 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, 1, ISNULL(NULLIF(CHARINDEX(',', @Databases, 1), 0), LEN(@Databases) + 1) - 1) AS DatabaseItem
  WHERE @Databases IS NOT NULL
  UNION ALL
  SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
         ISNULL(NULLIF(CHARINDEX(',', @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) AS EndPosition,
         SUBSTRING(@Databases, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(',', @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) - EndPosition - 1) AS DatabaseItem
  FROM Databases1
  WHERE EndPosition < LEN(@Databases) + 1
  ),
  Databases2 (DatabaseItem, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem LIKE '-%' THEN RIGHT(DatabaseItem,LEN(DatabaseItem) - 1) ELSE DatabaseItem END AS DatabaseItem,
         StartPosition,
         CASE WHEN DatabaseItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
  FROM Databases1
  ),
  Databases3 (DatabaseItem, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN DatabaseItem IN('ALL_DATABASES','SYSTEM_DATABASES','USER_DATABASES','AVAILABILITY_GROUP_DATABASES') THEN '%' ELSE DatabaseItem END AS DatabaseItem,
         CASE WHEN DatabaseItem = 'SYSTEM_DATABASES' THEN 'S' WHEN DatabaseItem = 'USER_DATABASES' THEN 'U' ELSE NULL END AS DatabaseType,
         CASE WHEN DatabaseItem = 'AVAILABILITY_GROUP_DATABASES' THEN 1 ELSE NULL END AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases2
  ),
  Databases4 (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
  (
  SELECT CASE WHEN LEFT(DatabaseItem,1) = '[' AND RIGHT(DatabaseItem,1) = ']' THEN PARSENAME(DatabaseItem,1) ELSE DatabaseItem END AS DatabaseItem,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases3
  )
  INSERT INTO @SelectedDatabases (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected)
  SELECT DatabaseName,
         DatabaseType,
         AvailabilityGroup,
         StartPosition,
         Selected
  FROM Databases4
  OPTION (MAXRECURSION 0)
  
  IF @Version >= 11 AND SERVERPROPERTY('IsHadrEnabled') = 1
  BEGIN
    INSERT INTO @tmpAvailabilityGroups (AvailabilityGroupName, Selected)
    SELECT name AS AvailabilityGroupName,
           0 AS Selected
    FROM sys.availability_groups

    INSERT INTO @tmpDatabasesAvailabilityGroups (DatabaseName, AvailabilityGroupName)
    SELECT databases.name,
           availability_groups.name
    FROM sys.databases databases
    INNER JOIN sys.dm_hadr_availability_replica_states dm_hadr_availability_replica_states ON databases.replica_id = dm_hadr_availability_replica_states.replica_id
    INNER JOIN sys.availability_groups availability_groups ON dm_hadr_availability_replica_states.group_id = availability_groups.group_id
  END

  INSERT INTO @tmpDatabases (DatabaseName, DatabaseNameFS, DatabaseType, AvailabilityGroup, [Order], Selected, Completed)
  SELECT [name] AS DatabaseName,
         RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE([name],'\',''),'/',''),':',''),'*',''),'?',''),'"',''),'<',''),'>',''),'|','')) AS DatabaseNameFS,
         CASE WHEN name IN('master','msdb','model') OR is_distributor = 1 THEN 'S' ELSE 'U' END AS DatabaseType,
         NULL AS AvailabilityGroup,
         0 AS [Order],
         0 AS Selected,
         0 AS Completed
  FROM sys.databases
  WHERE [name] <> 'tempdb'
  AND source_database_id IS NULL
  ORDER BY [name] ASC

  UPDATE tmpDatabases
  SET AvailabilityGroup = CASE WHEN EXISTS (SELECT * FROM @tmpDatabasesAvailabilityGroups WHERE DatabaseName = tmpDatabases.DatabaseName) THEN 1 ELSE 0 END
  FROM @tmpDatabases tmpDatabases

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  WHERE SelectedDatabases.Selected = 1

  UPDATE tmpDatabases
  SET tmpDatabases.Selected = SelectedDatabases.Selected
  FROM @tmpDatabases tmpDatabases
  INNER JOIN @SelectedDatabases SelectedDatabases
  ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
  AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
  AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
  WHERE SelectedDatabases.Selected = 0

  UPDATE tmpDatabases
  SET tmpDatabases.StartPosition = SelectedDatabases2.StartPosition
  FROM @tmpDatabases tmpDatabases
  INNER JOIN (SELECT tmpDatabases.DatabaseName, MIN(SelectedDatabases.StartPosition) AS StartPosition
              FROM @tmpDatabases tmpDatabases
              INNER JOIN @SelectedDatabases SelectedDatabases
              ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
              AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
              AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
              WHERE SelectedDatabases.Selected = 1
              GROUP BY tmpDatabases.DatabaseName) SelectedDatabases2
  ON tmpDatabases.DatabaseName = SelectedDatabases2.DatabaseName

  IF @Databases IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedDatabases) OR EXISTS(SELECT * FROM @SelectedDatabases WHERE DatabaseName IS NULL OR DatabaseName = ''))
  BEGIN
    SET @ErrorMessage = 'The value for the parameter @Databases is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  
  ----------------------------------------------------------------------------------------------------
  --// Check that selected databases and availability groups exist                                //--
  ----------------------------------------------------------------------------------------------------

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(DatabaseName) + ', '
  FROM @SelectedDatabases
  WHERE DatabaseName NOT LIKE '%[%]%'
  AND DatabaseName NOT IN (SELECT DatabaseName FROM @tmpDatabases)
  IF @@ROWCOUNT > 0
  BEGIN
    SET @ErrorMessage = 'The following databases in the @Databases parameter do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.'
    RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END

  SET @ErrorMessage = ''
  SELECT @ErrorMessage = @ErrorMessage + QUOTENAME(AvailabilityGroupName) + ', '
  FROM @SelectedAvailabilityGroups
  WHERE AvailabilityGroupName NOT LIKE '%[%]%'
  AND AvailabilityGroupName NOT IN (SELECT AvailabilityGroupName FROM @tmpAvailabilityGroups)
  IF @@ROWCOUNT > 0
  BEGIN
    SET @ErrorMessage = 'The following availability groups do not exist: ' + LEFT(@ErrorMessage,LEN(@ErrorMessage)-1) + '.'
    RAISERROR('%s',10,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
  END
  
  ----------------------------------------------------------------------------------------------------
  --// Update database order                                                                      //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabaseOrder IN('DATABASE_SIZE_ASC','DATABASE_SIZE_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET DatabaseSize = (SELECT SUM(size) FROM sys.master_files WHERE [type] = 0 AND database_id = DB_ID(tmpDatabases.DatabaseName))
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IN('LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC','LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC')
  BEGIN
    UPDATE tmpDatabases
    SET LogSizeSinceLastLogBackup = (SELECT log_since_last_log_backup_mb FROM sys.dm_db_log_stats(DB_ID(tmpDatabases.DatabaseName)))
    FROM @tmpDatabases tmpDatabases
  END

  IF @DatabaseOrder IS NULL
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_NAME_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseName DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'DATABASE_SIZE_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY DatabaseSize DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'LOG_SIZE_SINCE_LAST_LOG_BACKUP_ASC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LogSizeSinceLastLogBackup ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END
  ELSE
  IF @DatabaseOrder = 'LOG_SIZE_SINCE_LAST_LOG_BACKUP_DESC'
  BEGIN
    WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY LogSizeSinceLastLogBackup DESC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
    )
    UPDATE tmpDatabases
    SET [Order] = RowNumber
  END

  ----------------------------------------------------------------------------------------------------
  --// Update the queue                                                                           //--
  ----------------------------------------------------------------------------------------------------

  IF @DatabasesInParallel = 'Y'
  BEGIN

    BEGIN TRY

      SELECT @QueueID = QueueID
      FROM dbo.[Queue]
      WHERE SchemaName = @SchemaName
      AND ObjectName = @ObjectName
      AND [Parameters] = @Parameters

      IF @QueueID IS NULL
      BEGIN
        BEGIN TRANSACTION

        SELECT @QueueID = QueueID
        FROM dbo.[Queue] WITH (UPDLOCK, TABLOCK)
        WHERE SchemaName = @SchemaName
        AND ObjectName = @ObjectName
        AND [Parameters] = @Parameters

        IF @QueueID IS NULL
        BEGIN
          INSERT INTO dbo.[Queue] (SchemaName, ObjectName, [Parameters])
          SELECT @SchemaName, @ObjectName, @Parameters

          SET @QueueID = SCOPE_IDENTITY()
        END

        COMMIT TRANSACTION
      END

      BEGIN TRANSACTION

      UPDATE [Queue]
      SET QueueStartTime = GETDATE(),
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID)
      FROM dbo.[Queue] [Queue]
      WHERE QueueID = @QueueID
      AND NOT EXISTS (SELECT *
                      FROM sys.dm_exec_requests
                      WHERE session_id = [Queue].SessionID
                      AND request_id = [Queue].RequestID
                      AND start_time = [Queue].RequestStartTime)
      AND NOT EXISTS (SELECT *
                      FROM dbo.QueueDatabase QueueDatabase
                      INNER JOIN sys.dm_exec_requests ON QueueDatabase.SessionID = session_id AND QueueDatabase.RequestID = request_id AND QueueDatabase.RequestStartTime = start_time
                      WHERE QueueDatabase.QueueID = @QueueID)

      IF @@ROWCOUNT = 1
      BEGIN
        INSERT INTO dbo.QueueDatabase (QueueID, DatabaseName)
        SELECT @QueueID AS QueueID,
               DatabaseName
        FROM @tmpDatabases tmpDatabases
        WHERE Selected = 1
        AND NOT EXISTS (SELECT * FROM dbo.QueueDatabase WHERE DatabaseName = tmpDatabases.DatabaseName AND QueueID = @QueueID)

        DELETE QueueDatabase
        FROM dbo.QueueDatabase QueueDatabase
        WHERE QueueID = @QueueID
        AND NOT EXISTS (SELECT * FROM @tmpDatabases tmpDatabases WHERE DatabaseName = QueueDatabase.DatabaseName AND Selected = 1)

        UPDATE QueueDatabase
        SET DatabaseOrder = tmpDatabases.[Order]
        FROM dbo.QueueDatabase QueueDatabase
        INNER JOIN @tmpDatabases tmpDatabases ON QueueDatabase.DatabaseName = tmpDatabases.DatabaseName
        WHERE QueueID = @QueueID
      END

      COMMIT TRANSACTION

      SELECT @QueueStartTime = QueueStartTime
      FROM dbo.[Queue]
      WHERE QueueID = @QueueID

    END TRY

    BEGIN CATCH
      IF XACT_STATE() <> 0
      BEGIN
        ROLLBACK TRANSACTION
      END
      SET @ErrorMessage = 'Msg ' + CAST(ERROR_NUMBER() AS nvarchar) + ', ' + ISNULL(ERROR_MESSAGE(),'')
      RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
      RAISERROR(@EmptyLine,10,1) WITH NOWAIT
      SET @ReturnCode = ERROR_NUMBER()
      GOTO Logging
    END CATCH

  END

  ----------------------------------------------------------------------------------------------------
  --// Execute backup commands                                                                    //--
  ----------------------------------------------------------------------------------------------------

  WHILE (1 = 1)
  BEGIN

    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE QueueDatabase
      SET DatabaseStartTime = NULL,
          SessionID = NULL,
          RequestID = NULL,
          RequestStartTime = NULL
      FROM dbo.QueueDatabase QueueDatabase
      WHERE QueueID = @QueueID
      AND DatabaseStartTime IS NOT NULL
      AND DatabaseEndTime IS NULL
      AND NOT EXISTS (SELECT * FROM sys.dm_exec_requests WHERE session_id = QueueDatabase.SessionID AND request_id = QueueDatabase.RequestID AND start_time = QueueDatabase.RequestStartTime)

      UPDATE QueueDatabase
      SET DatabaseStartTime = GETDATE(),
          DatabaseEndTime = NULL,
          SessionID = @@SPID,
          RequestID = (SELECT request_id FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          RequestStartTime = (SELECT start_time FROM sys.dm_exec_requests WHERE session_id = @@SPID),
          @CurrentDatabaseName = DatabaseName
      FROM (SELECT TOP 1 DatabaseStartTime,
                         DatabaseEndTime,
                         SessionID,
                         RequestID,
                         RequestStartTime,
                         DatabaseName
            FROM dbo.QueueDatabase
            WHERE QueueID = @QueueID
            AND (DatabaseStartTime < @QueueStartTime OR DatabaseStartTime IS NULL)
            AND NOT (DatabaseStartTime IS NOT NULL AND DatabaseEndTime IS NULL)
            ORDER BY DatabaseOrder ASC
            ) QueueDatabase
    END
    ELSE
    BEGIN
      SELECT TOP 1 @CurrentDBID = ID,
                   @CurrentDatabaseName = DatabaseName
      FROM @tmpDatabases
      WHERE Selected = 1
      AND Completed = 0
      ORDER BY [Order] ASC
    END

    IF @@ROWCOUNT = 0
    BEGIN
      BREAK
    END

---- ************************************************************************************************************************************************************************************
    SELECT @CurrentDatabaseID = database_id,
           @CurrentUserAccess = user_access_desc,
           @CurrentIsReadOnly = is_read_only,
           @CurrentDatabaseState = state_desc,
           @CurrentInStandby = is_in_standby,
		   @CurrentCreateDate = create_date
    FROM sys.databases
    WHERE [name] = @CurrentDatabaseName

	IF @CurrentCreateDate >= DATEADD(dd,-ISNULL(@MinDatabaseAgeInDays,0), GETDATE())
	BEGIN
		RAISERROR(N'Database "%s" was created too recently.',0,1,@CurrentDatabaseName);
		GOTO CompleteDB;
	END

SET @Qry = 'SELECT 
	[DB_Name]				= DB_NAME(),
	[DB_FileName]			= df.name,
	[FileUsedSpace_%]		= (CAST(FILEPROPERTY(df.name, ''SpaceUsed'') AS INT) * 100.0 / df.size),
	[isPercentage_Growth]	= df.is_percent_growth,
	[growth]				= df.growth,
	[fileSizeInMB]			= df.size/128
FROM sys.database_files df
WHERE df.type IN ( ' + CASE @FileTypes
                        WHEN 'ALL' THEN '0,1'
                        WHEN 'LOG' THEN '1'
                        WHEN 'ROWS' THEN '0'
                        END + ')'


DECLARE @sp_executesql nvarchar(max)

SET @sp_executesql = QUOTENAME(@CurrentDatabaseName) + '.sys.sp_executesql'

INSERT INTO @tmpDatabaseFiles
EXEC @sp_executesql @Qry

-- From Here we start to check the threshold value for every file 

DECLARE cursor_CheckDBfile CURSOR
LOCAL FAST_FORWARD FOR 
SELECT dbName,dbFileName,[UsedSpaceIn_%],[IsFileGrow_perc],Growth,FileSizeInMB
FROM @tmpDatabaseFiles 
OPEN cursor_CheckDBfile

FETCH NEXT FROM cursor_CheckDBfile INTO @DB_name,@DB_FileName,@SpaceUsedPercent,@isPercentage,@FileAutoGrowth,@FileSizeInMB


WHILE @@FETCH_STATUS = 0
BEGIN
	DECLARE @NewSizeForFile INT, @Proc NVARCHAR(4000), @HasGrown BIT, @PercentString NVARCHAR(100)

	SET @HasGrown = 0

	WHILE @Error = 0 AND @SpaceUsedPercent >= @UsedSpacePercentHighThreshold  
	BEGIN
		SET @PercentString = CONVERT(nvarchar, @SpaceUsedPercent);

		IF @isPercentage = 1
		BEGIN
			SET @NewSizeForFile = @FileSizeInMB * (CAST(@FileAutoGrowth as float)/100) + 1
		END
		ELSE --IF @isPercentage = 0
		BEGIN
			SET @NewSizeForFile = @FileSizeInMB + (@FileAutoGrowth / 128)
		END

		IF @NewSizeForFile > @FileSizeInMB
		BEGIN
			
			RAISERROR(N'DB: %s, File: %s used space %s percent - growing from %d to %d', 0, 1, @DB_name, @DB_FileName, @PercentString, @FileSizeInMB, @NewSizeForFile) WITH NOWAIT;

			SET @Qry = N'ALTER DATABASE ' + QUOTENAME(@DB_name) + N' MODIFY FILE(NAME = ' + QUOTENAME(@DB_FileName) + N', SIZE = ' + CAST(@NewSizeForFile as nvarchar(max)) + N'MB )'
		
			EXECUTE @CurrentCommandOutput01 = [dbo].[CommandExecute]
										   @Command      = @Qry, 
										   @CommandType  = 'AutoGrowth', 
										   @Mode		 = 1, 
										   @DatabaseName = @DB_name, 
										   @LogToTable   = @LogToTable, 
										   @Execute      = @Execute
			SET @Error = @@ERROR
			IF @Error <> 0 SET @CurrentCommandOutput01 = @Error
			IF @CurrentCommandOutput01 <> 0 SET @ReturnCode = @CurrentCommandOutput01

			-- Re-check space used
		
			EXEC @sp_executesql N'
		SELECT 
			@NewSpaceUsed = (CAST(FILEPROPERTY(df.name, ''SpaceUsed'') AS INT) * 100.0 / df.size),
			@NewSizeInMB  = df.size/128 
		FROM sys.database_files df
		WHERE df.name = @FileName;'
					, N'@FileName SYSNAME, @NewSpaceUsed FLOAT OUTPUT, @NewSizeInMB FLOAT OUTPUT', @DB_FileName, @SpaceUsedPercent OUTPUT, @FileSizeInMB OUTPUT
		
			SET @HasGrown = 1
		END
		ELSE
			BREAK;
	END

	IF @HasGrown = 0 AND @SpaceUsedPercent <= @UsedSpacePercentLowThreshold AND @FileSizeInMB > @MinFileSizeForShrinkMB
	BEGIN
		SET @PercentString = CONVERT(nvarchar, @SpaceUsedPercent);
		SET @NewSizeForFile = (
									SELECT MAX(Val) 
									FROM (VALUES
										(CEILING(@FileSizeInMB * (@ShrinkToTargetSizePercent/100.0)))
										,(ISNULL(@ShrinkToMaxSizeMB,1))
									) AS V(Val)
								)
		
		WHILE @Error = 0 AND @NewSizeForFile < @FileSizeInMB AND @SpaceUsedPercent <= @UsedSpacePercentLowThreshold AND @FileSizeInMB > @MinFileSizeForShrinkMB
		BEGIN
			DECLARE @FileSizeAfterShrink INT
			RAISERROR(N'DB: %s, File: %s used space %s percent - shrinking from %d to %d', 0, 1, @DB_name, @DB_FileName, @PercentString, @FileSizeInMB, @NewSizeForFile) WITH NOWAIT;
			SET @Qry = 'DBCC SHRINKFILE(' + QUOTENAME(@DB_FileName) + ',' + CAST(@NewSizeForFile as varchar(1000)) 
						+ CASE WHEN @ShrinkAllowReorganize = 'Y' THEN N'' ELSE N', TRUNCATEONLY' END + ') WITH NO_INFOMSGS;'
			
			EXECUTE @CurrentCommandOutput01 = [dbo].[CommandExecute]
										   @Command      = @Qry, 
										   @CommandType  = 'AutoShrink', 
										   @Mode		 = 1, 
										   @DatabaseName = @DB_name, 
										   @LogToTable   = @LogToTable, 
										   @Execute      = @Execute
			SET @Error = @@ERROR
			IF @Error <> 0 SET @CurrentCommandOutput01 = @Error
			IF @CurrentCommandOutput01 <> 0 SET @ReturnCode = @CurrentCommandOutput01
			
			EXEC @sp_executesql N'
		SELECT 
			@NewSpaceUsed = (CAST(FILEPROPERTY(df.name, ''SpaceUsed'') AS INT) * 100.0 / df.size),
			@FileSizeAfterShrink  = df.size/128 
		FROM sys.database_files df
		WHERE df.name = @FileName;'
					, N'@FileName SYSNAME, @NewSpaceUsed FLOAT OUTPUT, @FileSizeAfterShrink INT OUTPUT', @DB_FileName, @SpaceUsedPercent OUTPUT, @FileSizeAfterShrink OUTPUT
		
			IF @FileSizeAfterShrink >= @FileSizeInMB
				BREAK;
			ELSE
				SET @FileSizeInMB = @FileSizeAfterShrink;

			SET @PercentString = CONVERT(nvarchar, @SpaceUsedPercent);
			SET @NewSizeForFile = (
										SELECT MAX(Val) 
										FROM (VALUES
											(CEILING(@FileSizeInMB * (@ShrinkToTargetSizePercent/100.0)))
											,(ISNULL(@ShrinkToMaxSizeMB,1))
										) AS V(Val)
									)
		
		END
	END

	FETCH NEXT FROM cursor_CheckDBfile INTO @DB_name,@DB_FileName,@SpaceUsedPercent,@isPercentage,@FileAutoGrowth,@FileSizeInMB

END;
CLOSE cursor_CheckDBfile; 
DEALLOCATE cursor_CheckDBfile;

CompleteDB:
---- ************************************************************************************************************************************************************************************

    -- Update that the database is completed
    IF @DatabasesInParallel = 'Y'
    BEGIN
      UPDATE dbo.QueueDatabase
      SET DatabaseEndTime = GETDATE()
      WHERE QueueID = @QueueID
      AND DatabaseName = @CurrentDatabaseName
    END
    ELSE
    BEGIN
      UPDATE @tmpDatabases
      SET Completed = 1
      WHERE Selected = 1
      AND Completed = 0
      AND ID = @CurrentDBID
    END
	
    -- Clear variables
    SET @CurrentDBID = NULL
    SET @CurrentDatabaseName = NULL

    SET @CurrentDatabaseID = NULL
    SET @CurrentUserAccess = NULL
    SET @CurrentIsReadOnly = NULL
    SET @CurrentDatabaseState = NULL
    SET @CurrentInStandby = NULL
    SET @CurrentCreateDate = NULL
	
	DELETE @tmpDatabaseFiles;

  END

  ----------------------------------------------------------------------------------------------------
  --// Log completing information                                                                 //--
  ----------------------------------------------------------------------------------------------------

  Logging:
  SET @EndMessage = 'Date and time: ' + CONVERT(nvarchar,GETDATE(),120)
  RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

  RAISERROR(@EmptyLine,10,1) WITH NOWAIT

  IF @ReturnCode <> 0
  BEGIN
    RETURN @ReturnCode
  END

  ----------------------------------------------------------------------------------------------------
END
