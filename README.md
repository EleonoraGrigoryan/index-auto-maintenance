# Index Auto Maintenance
Transact SQL script to maintain indexes automatically for a current database. It either rebuilds or reorganizes table indexes based on their fragmentation percentage provided by sys.dm_db_index_physical_stats inline table valued function. 
The choose of fragmentation percent is based on Microsoft recommendations.

----
MIT License <br>
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND
