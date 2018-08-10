-- Auto Rebuild or Reorganize Based on Fragmentation Percent

create procedure up_index_auto_maintenance  
as
begin  

-- Declare Variables for Sproc
declare @index_holder sysname
declare @fragmentation_holder int
declare @tablename_holder sysname
declare @dynamic_rebuild nvarchar(200)
declare @dynamic_reorganize nvarchar(200)

 -- Declare Cursor
	declare cr_index cursor
	for
	select
		idx.[name] as [index_name],
		obj.[name] as [table_name],  
		ips.[avg_fragmentation_in_percent] as [fragmentation_percent]
	from sys.indexes as idx
	inner join sys.objects as obj on idx.object_id = obj.object_id
	inner join sys.schemas as sc  on obj.schema_id = sc.schema_id
	cross apply sys.dm_db_index_physical_stats( DB_ID(), idx.object_id, idx.index_id, NULL ,'LIMITED') AS ips
	where idx.[name] is not NULL
	order by [fragmentation_percent] desc;


-- Open Cursor
	open cr_index
	
		fetch next from cr_index into @index_holder, @tablename_holder, @fragmentation_holder

		while @@fetch_status = 0
		begin
			if @fragmentation_holder  > 30
			begin
				set @dynamic_rebuild = 'alter index [' + @index_holder + '] on [' +  @tablename_holder +']  rebuild with (drop_existing = on, fillfactor = 80)'
				exec(@dynamic_rebuild)
				print @index_holder + 'is rebuilt'
			end
			else
			begin
				set @dynamic_reorganize = 'alter index  [' + @index_holder + '] on [' +  @tablename_holder +']  reorganize'
				exec(@dynamic_reorganize)
				print @index_holder + ' is reorganized'
			end
			
			fetch next from cr_index into @index_holder, @tablename_holder, @fragmentation_holder
		end
	close cr_index
	deallocate cr_index
end

-- Execute Procedure
exec up_index_auto_maintenance

