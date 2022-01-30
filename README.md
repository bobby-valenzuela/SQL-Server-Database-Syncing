# SQL-Server-Database-Syncing
Perl script used to update SQL Server databases from one database server to another. A sort of database RSYNC to keep things synced up.

Ideal if you have two databases you itended to keep in sync that are on different hosts.
Define your source and destination databases in the top of the script and call the script passing in a table name as an argument or 'all' to sync all tables.

Example:

perl sql_server_update_databases.pl awesomeTable


Notables:
- If the table in question already exists on the destination database (which is likely the cases given this is intended to sync) then the existing table in the destination database will be renamed with a suffix of '__old'. 
- If the destination database already has a table name also ending with '__old' that one will be dropped.
- Non-clustered indexes will be recreated on the desination machine. 
- Only some constraints are being accounted for when recreated the new table on the destination database. Namely, IDENTITY, DEFAULT, NOT NULL, and PRIMARY KEY. This can be expanded on over time to account for more column contraints.
