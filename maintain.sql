-- Auto Rebuild or Reorganize Based on Fragmentation Percent

create table #temptable
(
	[database_name] sysname,
	[schema_name] sysname,
	[table_name] sysname,
	[index_type_description] nvarchar(20),
	[index_name] sysname,
	[partition_number] int,
	[fragmentation_percent] int  
)

insert into #temptable
select
    db_name() as [database_name],
    sc.[name] as [schema_name],
    obj.[name] as [table_name],  
    idx.[type_desc] as [index_type_description],
    idx.[name] as [index_name],
    ips.partition_number as [partition_number],
    ips.[avg_fragmentation_in_percent] as [fragmentation_percent]
from sys.indexes as idx
inner join sys.objects as obj on idx.object_id = obj.object_id
inner join sys.schemas as sc  on obj.schema_id = sc.schema_id
cross apply sys.dm_db_index_physical_stats( DB_ID(), idx.object_id, idx.index_id, NULL ,'LIMITED') AS ips
where idx.[name] is not NULL
order by [fragmentation_percent] desc;
GO

alter procedure up_index_auto_maintenance

as
begin  

declare @index_holder sysname

declare @dynamic_rebuild nvarchar(200)
set @dynamic_rebuild = 'alter [' + @index_holder + '] on [' + db_name() +']  rebuild with (drop_existing = on, fillfactor = 80)'

declare @dynamic_reorganize nvarchar(200)
set @dynamic_reorganize = 'alter [' + @index_holder + '] on [' + db_name() +']  reorganize'

	
	declare cr_index cursor
	
	for 
		select [index_name] from #temptable

	open cr_index
		
		fetch next from cr_index into @index_holder

		while @@fetch_status = 0
		begin
			if (select [fragmentation_percent] from #temptable where [index_name] = @index_holder) > 30
			begin
				exec(@dynamic_rebuild)
				print @index_holder + 'is rebuilt'
			end

			else
			begin
				exec(@dynamic_reorganize)
				print @index_holder+ ' is reorganized'
			end
			
			fetch next from cr_index into @index_holder
		end

	close cr_index
	deallocate cr_index

end

exec up_index_auto_maintenance


