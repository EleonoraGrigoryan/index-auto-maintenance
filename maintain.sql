-- Auto Maintenance

/* 
 * Select all the indexes in a specified database
 *
 * select 
 *    idx.[name] as [Index]
 * from sys.indexes as idx
 * inner join sys.objects as obj on idx.object_id = obj.object_id
 * where idx.[name] is not null and obj.[type] = 'u'
 * order by idx.[name]
 */
 
/*
 * Print more information about chosen indexes
 *
 * select 
 *    db_name() as [Database],
 *    sc.[name] as [Schema],
 *    obj.[name] as [Table],  
 *    idx.[type] as [Index-Type],
 *  idx.[name] as [Index]
 * from sys.indexes as idx
 * inner join sys.objects as obj on idx.object_id = obj.object_id
 * inner join sys.schemas as sc  on obj.schema_id = sc.schema_id
 * where idx.[name] is not null and obj.[type] = 'u'
 * order by [Table]
 */
 
 -- Create a cursor inside a sproc to iterate all the selected indexes
create procedure up_index_auto_maintenance
as
begin

 declare @index_holder sysname
 
 declare @dynamic_rebuild nvarchar(200)
 set @dynamic_rebuild = 'alter [' + @index_holder + '] on [' + db_name() +']  rebuild with (drop_existing = on, fillfactor = 80)'

 declare @dynamic_reorganize nvarchar(200)
 set @dynamic_reorganize = 'alter [' + @index_holder + '] on [' + db_name() +']  reorganize'

 declare cr_index cursor
 for 
   select 
    idx.[name] as [Index]
   from sys.indexes as idx
   inner join sys.objects as obj on idx.object_id = obj.object_id
   where idx.[name] is not null and obj.[type] = 'u'
   order by idx.[name]

 open cr_index

 fetch next from cr_index into @index_holder

 WHILE @@fetch_status = 0
 begin
    -- do something (f. ex. print @index_holder)
    fetch next from cr_index into @index_holder
 end

 close cr_index
 deallocate cr_index

end

-- Execute the procedure
exec up_index_auto_maintenance
GO

