# index-auto-maintenance
Transact SQL script to maintain indexes automatically for a current database. It either rebuilds or reorganizes table indexes based on their fragmentation percentage provided by sys.dm_db_index_physical_stats inline table valued function. 
