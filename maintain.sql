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
 
create table #temptable1
(
 [database] sysname,
 [schema] nvarchar(40),
 [table] sysname,
 [index_type] nvarchar(40),
 [index_name] nvarchar(90),
 [object_id] int,
 [index_id] int
)


insert into #temptable1
select
	db_name() as [Database],
	sc.[name] as [Schema],
	obj.[name] as [Table],  
	idx.[type_desc] as [Index Type],
	idx.[name] as [Index Name],
	idx.[object_id] as [Obj_id],
	idx.[index_id] as [Ind_id]
from sys.indexes as idx
 inner join sys.objects as obj on idx.object_id = obj.object_id
 inner join sys.schemas as sc  on obj.schema_id = sc.schema_id
where idx.[name] is not null and obj.[type] = 'u' or obj.[type] = 'v' 


/*

select
	db_name() as [Database],
	sc.[name] as [Schema],
	obj.[name] as [Table],  
	idx.[type_desc] as [Index Type],
	idx.[name] as [Index Name],
	idx.[object_id] as [Obj_id],
	ips.[avg_fragmentation_in_percent] as [Fragmentation Percent]
from sys.indexes as idx
 inner join sys.objects as obj on idx.object_id = obj.object_id
 inner join sys.schemas as sc  on obj.schema_id = sc.schema_id
 left join sys.dm_db_index_physical_stats( NULL,NULL, NULL, NULL ,'LIMITED') AS ips on idx.[object_id] = ips.[object_id] and idx.index_id = ips.index_id 
where database_id = db_id() and idx.[name] is not null and obj.[type] = 'u' or obj.[type] = 'v'
ORDER BY [Table], idx.[type_desc]

*/


create table #temptable2
(
	[index_name] sysname,
	[fragmentation_percent] int
)


insert into #temptable2
select
tb.[index_name],
ips.[avg_fragmentation_in_percent] as [fragmentation_percent]
from #temptable1 as tb
left join sys.dm_db_index_physical_stats( NULL,NULL, NULL, NULL ,'LIMITED') as ips 
on tb.[object_id] = ips.[object_id]  and tb.[index_id] = ips.index_id
where database_id = db_id();
GO

--select * from #temptable2
--------------------------------------------

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
		select [index_name] from #temptable2 

	open cr_index
		
		fetch next from cr_index into @index_holder

		while @@fetch_status = 0
		begin
			if (select [fragmentation_percent] from #temptable2 where [index_name] = @index_holder) > 30
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
