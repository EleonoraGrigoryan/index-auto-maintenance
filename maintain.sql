--Name:       Index Maintainance Automatation
--Purpose:    Defragment all fragmented indexes 
--Date:       21/11/2018
--Author:     Eleonora Grigoryan
----------------------------------------------------------------------------------

if exists(select [object_id] from sys.tables where name = N'tblInvestigation')

begin
    drop table dbo.tblInvestigation
end

    create table dbo.tblInvestigation
    (
		   [defragmentationID] int identity(1,1) Not Null,
		   constraint tblInvestigation_pk primary key clustered (defragmentationID),

           [databaseID] int Not Null,
		   [databaseName] nvarchar(150) Not Null,

		   [tableName] nvarchar(150) Not Null,

           [indexID] int Not Null,
		   [indexName] nvarchar(150) Not Null,

           [partitionNumber] int Not Null,
           [fragmentation] float Not Null,
           [pageCount] int Not Null,

           [date] datetime Not Null,
           [lengthSeconds] int Not Null   
    )


if ObjectProperty(Object_ID('dbo.sprocDefrag'), N'IsProcedure') = 1
begin
    drop procedure dbo.sprocDefrag;
end;
go

create procedure dbo.sprocDefrag

@debuggerHelp bit = 0,
@minFragPercent float = 5.0,
@rebuildLinePercent float = 30.0,
@sql bit = 1, 
@print bit = 0,
@printFragPercent bit = 0,
@retard char(8) = '00:00:05',
@db varchar(150) = NULL,
@tblName varchar(150) = NULL,
--dbname.schemaname.tblname
@onlineRebuild bit = 1,
@maxDop int = NULL

As
 
set NoCount On;
set XACT_Abort On;
set Quoted_Identifier On;

begin
    If @debuggerHelp = 1 RaisError('The procedure is starting', 0, 42) With NoWait;

    /* Declare our variables */
   Declare  @objID int,
             @dbID int,
             @dbName nvarchar(150),
             @idxID int,
             @partitionCount bigint,
             @schemaName nvarchar(150),
             @objName nvarchar(150),
             @idxName nvarchar(150),
             @partitionNumber smallint,
             @partitions smallint,
             @frag float,
             @pageCount int,
             @sqlCommand nvarchar(4000),
             @rebuildCommand nvarchar(200),
             @dateTimeStart datetime,
             @dateTimeEnd datetime,
             @containsLOB bit,
             @editionCheck bit,
             @debugMsg varchar(150),
             @updateSQL nvarchar(4000),
             @partitionSQL nvarchar(4000),
             @partitionSQL_Param nvarchar(1000),
             @LOB_SQL nvarchar(4000),
             @LOB_SQL_Param nvarchar(1000);

    
    Create Table #idxList
    (
          [dbID] int,
          [dbName] nvarchar(128),
          [objID] int,
		  [objName] nvarchar(150) Null,
          [idxID] int,
		  [idxName] nvarchar(150)   Null,
          [partitionNumber] smallint,
          [frag] float,
          [pageCount] int,
          [defragStatus] bit,
          [schemaName] nvarchar(150)  Null         
    );

    Create Table #databaseList
    (
          [dbID]        int,
          dbName      varchar(128)
    );

    Create Table #processorInfo
    (
         [index]           int,
         Name              varchar(128),
         Internal_Value    int,
         Character_Value   int
    );


    If @debuggerHelp = 1 RaisError('Starts to gather information about the server', 0, 42) With NoWait;

	
    If @minFragPercent Not Between 0.00 And 100.0
        Set @minFragPercent = 5.0;

    If @rebuildLinePercent Not Between 0.00 And 100.0
        Set @rebuildLinePercent = 30.0;

    If @retard Not Like '00:[0-5][0-9]:[0-5][0-9]'
        Set @retard = '00:00:05';

    
    Insert Into #processorInfo
    Execute xp_msver 'ProcessorCount';                     

    If @maxDop Is Not Null And @maxDop > (Select Internal_Value From #processorInfo)
        Select @maxDop = Internal_Value
        From #processorInfo;

    if (select ServerProperty('EditionID')) In (1804890536, 610778273, -2117995310) 
        Set @editionCheck = 1 
    Else
        Set @editionCheck = 0; -- no online rebuild

    If @debuggerHelp = 1 RaisError('Getting the list of databases', 0, 42) With NoWait;


    Insert Into #databaseList
    Select [database_id],
           [name]
    From sys.databases
    Where name = IsNull(@db, [name])
        And database_id > 4 
        And [state] = 0;   --online not restoring 

    If @debuggerHelp = 1 RaisError('Looping through our list of databases and checking for fragmentation...', 0, 42) With NoWait;


    While (Select Count(*) From #databaseList) > 0
    Begin

        Select Top 1 @dbID = [dbID]
        From #databaseList;

        Select @debugMsg = 'Cheking indexes of ' + DB_Name(@dbID);

        If @debuggerHelp = 1
            RaisError(@debugMsg, 0, 42) With NoWait;


		Insert Into #idxList
		Select
			database_id As dbID,
			quotename(DB_Name(database_id)) As 'dbName',
			[object_id] As objID,
			Null As 'objName',
			index_id As idxID,
			Null As 'idxName',
			partition_number As partitionNumber,
			avg_fragmentation_in_percent As frag,
			page_count ,
			0 As 'defragStatus', /* 0 = unprocessed, 1 = processed */
			Null As 'schemaName'
			From sys.dm_db_index_physical_stats (@dbID, Object_Id(@tblName), Null , Null, N'Limited')
			Where avg_fragmentation_in_percent >= @minFragPercent
			And index_id > 0 -- ignore heaps
			--  And page_count > 8 -- ignore objects with less than 1 extent
			Option (MaxDop 1);
			
			Delete From #databaseList
			Where dbID = @dbID;

    End

	--Creating an index for better search 
    Create Clustered Index idxList_cidx
        On #idxList([dbID], [objID], [idxID], [partitionNumber]);



    Select @debugMsg = 'There are ' + Cast(Count(*) As varchar(10)) + ' indexes to defrag!'
    From #idxList;

    If @debuggerHelp = 1 RaisError(@debugMsg, 0, 42) With NoWait;

    
    While (Select Count(*) From #idxList Where defragStatus = 0) > 0
    Begin

        If @debuggerHelp = 1 RaisError('  choosing an index', 0, 42) With NoWait;

        /* Grab the most fragmented index first to defrag */
           Select Top 1 
             @objID = [objID],
             @idxID = [idxID],
             @dbID = [dbID],
             @dbName = [dbName],
             @frag = [frag],
             @partitionNumber = [partitionNumber],
             @pageCount = [pageCount]
        From #idxList
        Where defragStatus = 0
        Order By frag Desc;

        If @debuggerHelp = 1 RaisError(' Looking up sequential index information', 0, 42) With NoWait;

      Select @updateSQL = N'
		    Update idl
            Set schemaName = QuoteName(s.name),
                 objName = QuoteName(o.name),
                 idxName = QuoteName(i.name)
            From #idxList As idl
            Inner Join ' + @dbName + '.sys.objects As o
                On idl.objID = o.object_id
            Inner Join ' + @dbName + '.sys.indexes As i
                On o.object_id = i.object_id
            Inner Join ' + @dbName + '.sys.schemas As s
                On o.schema_id = s.schema_id
            Where o.object_id = ' + Cast(@objID As varchar(50)) + '
                And i.index_id = ' + Cast(@idxID As varchar(50)) + '
                And i.type > 0
                And idl.dbID = ' + Cast(@dbID As varchar(50)); 

        Execute sp_executeSQL @updateSQL;

        
        Select @objName  = [objName],
             @schemaName = [schemaName],
             @idxName = [idxName]
        From #idxList
        Where [objID] = @objID
            And [idxID] = @idxID
            And [dbID] = @dbID; 


/*These functions return OUTPUT parameters from sp_executeSQL into parameters*/
  If @debuggerHelp = 1 RaisError('  Determine if the index is partitioned', 0, 42) With NoWait;

        Select @partitionSQL = 'Select @partitionCount_OUT = Count(*)
                                    From ' + @dbName + '.sys.partitions
                                    Where object_id = ' + Cast(@objID As varchar(10)) + '
                                        And index_id = ' + Cast(@idxID As varchar(10)) + ';'
            , @partitionSQL_Param = '@partitionCount_OUT int OutPut';

        Execute sp_executeSQL @partitionSQL, @partitionSQL_Param, @partitionCount_OUT = @partitionCount OutPut;



        If @debuggerHelp = 1 RaisError('  Determine if the table contains LOBs', 0, 42) With NoWait;
    
        Select @LOB_SQL = ' Select Top 1 @containsLOB_OUT = column_id
                            From ' + @dbName + '.sys.columns With (NoLock) 
                            Where [object_id] = ' + Cast(@objID As varchar(10)) + '
                                And (system_type_id In (34, 35, 99)
                                        Or max_length = -1);'
                            /*  system_type_id --> 34 = image, 35 = text, 99 = ntext
                                max_length = -1 --> varbinary(max), varchar(max), nvarchar(max), xml */
                , @LOB_SQL_Param = '@containsLOB_OUT int OutPut';

        Execute sp_executeSQL @LOB_SQL, @LOB_SQL_Param, @containsLOB_OUT = @containsLOB OutPut;
        
        If @debuggerHelp = 1 RaisError('  Building our SQL statements...', 0, 42) With NoWait;

		-- Cases to rebuild or reorganize
        If @frag < @rebuildLinePercent Or @containsLOB = 1 Or @partitionCount > 1
        Begin
        
            Set @sqlCommand = N'Alter Index ' + @idxName + N' On ' + @dbName + N'.' 
                                + @schemaName + N'.' + @objName + N' ReOrganize';

           
            If @partitionCount > 1
                Set @sqlCommand = @sqlCommand + N' Partition = ' 
                                + Cast(@partitionNumber As nvarchar(10));

        End;


        If @frag >= @rebuildLinePercent And IsNull(@containsLOB, 0) != 1 And @partitionCount <= 1
        Begin
        
            If @onlineRebuild = 1 And @editionCheck = 1 
                Set @rebuildCommand = N' Rebuild With (Online = On';
            Else
                Set @rebuildCommand = N' Rebuild With (Online = Off';

            If @maxDop Is Not Null And @editionCheck = 1
                Set @rebuildCommand = @rebuildCommand + N', MaxDop = ' + Cast(@maxDop As varchar(2)) + N')';
            Else
                Set @rebuildCommand = @rebuildCommand + N')';
        
            Set @sqlCommand = N'Alter Index ' + @idxName + N' On ' + @dbName + N'.'
                            + @schemaName + N'.' + @objName + @rebuildCommand;

        End;

        /* 
		check if we are executing sql or not */
        If @sql = 1
        Begin

            If @debuggerHelp = 1 RaisError('  SQL statements are being executed', 0, 42) With NoWait;

            Set @dateTimeStart  = GetDate();
            Execute sp_executeSQL @sqlCommand;
            Set @dateTimeEnd  = GetDate();
           
--------------------------------------------------------
            /* Insert our actions into tblInvestigation table */
            Insert Into dbo.tblInvestigation
            (
                 [databaseID],
                 [databaseName],
                 --[objID],
                 [tableName],
                 [indexID],
                 [indexName],
                 [partitionNumber],
                 [fragmentation],
                 [pageCount],
                 [date],
                 [lengthSeconds]
            )
            Select
                 @dbID,
                 @dbName,
                 --@objID,
                 @objName,
                 @idxID,
                 @idxName,
                 @partitionNumber,
                 @frag,
                 @pageCount,
                 @dateTimeStart,
                 DateDiff(second, @dateTimeStart, @dateTimeEnd);

            WaitFor Delay @retard;
    
		   --If required we should print
            If @print = 1
                Print N'Executed: ' + @sqlCommand;
        End


        Else  -- @sql = 0
        
        Begin
            If @debuggerHelp = 1 RaisError('  SQL statements are being printed', 0, 42) With NoWait;
            
            If @print = 1 Print IsNull(@sqlCommand, '- does not work');
        End

        If @debuggerHelp = 1 RaisError('  Set defrag status to 1', 0, 42) With NoWait;

       
        Update #idxList
        Set defragStatus = 1
        Where [dbID]       = @dbID
          And [objID]         = @objID
          And [idxID]          = @idxID
          And [partitionNumber]  = @partitionNumber;

    End

   
    If @printFragPercent = 1
    Begin

        If @debuggerHelp = 1 RaisError('  Showing fragmentation percent', 0, 42) With NoWait;

        Select [dbID],
             [dbName],
             [objID],
             [objName],
             [idxID],
             [idxName],
             [frag],
             [pageCount]
        From #idxList;

    End;

    Drop Table #idxList;
    Drop Table #databaseList;
    Drop Table #processorInfo;

    If @debuggerHelp = 1 RaisError('The procedure is finished', 0, 42) With NoWait;

    Set NoCount Off;
	Return 0
End
Go
  
/*
              Execute the program from here!!!                
--------------------------------------------------------

    Exec dbo.sprocDefrag
          @sql = 0  --katari te che
	, @print = 1  --tpi te che
        , @minFragPercent = 5
        , @debuggerHelp = 1
        , @printFragPercent   = 1
        , @db = NULL
        , @tblName = NULL

*/	
