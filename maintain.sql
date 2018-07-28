-- Auto Maintenance

-- Select all the indexes in a specified database
 
select 
   idx.[name] as [Index]
from sys.indexes as idx
inner join sys.objects as obj on idx.object_id = obj.object_id
where idx.[name] is not null and obj.[type] = 'u'
order by idx.[name]

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
 
 
