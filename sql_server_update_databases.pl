#!/usr/bin/perl
use Apache::DBI;
use Data::Dumper;
use URI::Encode qw(uri_encode uri_decode);

# Source Database Details
my $src_db_servername = "<HOST_SERVER>";
my $src_db_databasename = "<DB_NAME>";
my $src_db_user = "<DB_USER>";
my $src_db_password = "<DB_PASSWORD>";
my $source_db = DBI->connect("DBI:Sybase:server=$src_db_servername;database=$src_db_databasename", $src_db_user, $src_db_password);

# Destination Database Details
my $src_db_servername = "<HOST_SERVER>";
my $src_db_databasename = "<DB_NAME>";
my $src_db_user = "<DB_NAME>";
my $src_db_password = "<DB_PASSWORD>";
my $destination_db = DBI->connect("DBI:Sybase:server=$src_db_servername;database=$src_db_databasename", $src_db_user, $src_db_password);

# Guard clause
if (!$ARGV[0]){die("Please Enter a Table Name as as argument or 'all' to update all tables.\n");}

my $debug = 1; # Adding this useful little guy as I learned from the best - my dev mentor Kevz0r

my $TABLE_TO_SEARCH = $ARGV[0] ne 'all' ? " AND TABLE_NAME='$ARGV[0]' " : '' ;

# Get all prod tables
my $query = "SELECT TABLE_NAME FROM sscore.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' $TABLE_TO_SEARCH ORDER BY TABLE_NAME";
my $sth = $source_db->prepare($query);
$sth->execute;
$src_tables = $sth->fetchall_arrayref();

die("Couldn't find any matching tables. Please try again.\n") if $src_tables->[0][0] eq '';

for my $table (0..$#{$src_tables}){

    my $SRC_TABLE_NAME = $src_tables->[$table][0];

    # Get source table columns
    $query = "SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='$SRC_TABLE_NAME' ORDER BY ORDINAL_POSITION";
    my $sth = $source_db->prepare($query);
    $sth->execute;
    my $columns = $sth->fetchall_arrayref();

    my $create_table_sql = "CREATE TABLE $SRC_TABLE_NAME (\n";

    my $first_col_is_identity = my $column_list = '';
    my $column_data_types = {}; # Hash to store column names and associated data types

    # Build Columns for table structure
    for my $column(0..$#{$columns}){

        my $data_type_length = my $is_iden = my $is_primary_key = '';
        my $data_type_length = "($columns->[$column][4])" if ($columns->[$column][4] ne ''&& lc($columns->[$column][3]) ne 'text');
        
        if ($column == 0){
            # If we're on first column - check if identity is applied to this col so we can add to new table
            my $query = "SELECT COLUMNPROPERTY(OBJECT_ID('$SRC_TABLE_NAME'),'$columns->[$column][0]','isidentity')";
            my $sth = $source_db->prepare($query);
            $sth->execute;
            $first_col_is_identity = $sth->fetchrow();

            # Let's also check if this first column serves as a primary key (only checking 1st )
            my $query = "
                SELECT  K.CONSTRAINT_NAME
                FROM    INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C
                        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K ON C.TABLE_NAME = K.TABLE_NAME
                        AND C.CONSTRAINT_CATALOG = K.CONSTRAINT_CATALOG
                        AND C.CONSTRAINT_SCHEMA = K.CONSTRAINT_SCHEMA
                        AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME
                WHERE   C.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        AND K.COLUMN_NAME = '$columns->[$column][0]'
                        AND K.TABLE_NAME = '$SRC_TABLE_NAME'
            ";
            my $sth = $source_db->prepare($query);
            $sth->execute;
            ($is_primary_key) = $sth->fetchrow();

        }
        my $has_primary_key = ( $is_primary_key ne '' && $column == 0) ? "PRIMARY KEY" : '';
        my $has_default = $columns->[$column][1] ne '' ? qq{DEFAULT $columns->[$column][1]} : '';
        my $is_nullable = $columns->[$column][2] eq 'NO' ? 'NOT NULL' : '';
        $is_iden = 'IDENTITY(1,1)' if $first_col_is_identity == 1 && $column == 0;
        $columns->[$column][3] = uc($columns->[$column][3]);
        
        # Build column list
        if ( ($first_col_is_identity == 0 && $column == 0) || $column > 0){
            $column_list .= "$columns->[$column][0],"
        };
        $column_list =~ s/,$// if $column == $#{$columns};
        
        # Buils create table query 
        $create_table_sql .= "  $columns->[$column][0] $columns->[$column][3] $has_primary_key $data_type_length $is_iden $is_nullable $has_default,\n";
        $create_table_sql =~ s/,$// if $column == $#{$columns};

        # Save column name and data type into Hash
        $column_data_types{"$column"} = "$columns->[$column][3]";
    }

    $create_table_sql .= ')';
    
    # If table name already exists, changed to tablename_old. If that already exists, drop that table to make room for new 'old' one :)
    my $old_table_name = "$SRC_TABLE_NAME\_old";
    my $query = "IF EXISTS 
                (SELECT object_id FROM sys.tables WHERE name = '$SRC_TABLE_NAME' AND SCHEMA_NAME(schema_id) = 'dbo')
                
                IF EXISTS
                    (SELECT object_id FROM sys.tables WHERE name = '$old_table_name' AND SCHEMA_NAME(schema_id) = 'dbo')
                    DROP TABLE $old_table_name
                ELSE
                    EXEC sp_rename '$SRC_TABLE_NAME', '$old_table_name'
            ELSE 
                PRINT 'The table ($SRC_TABLE_NAME) does not exist - no need to delete. Just add new one'";
    my $sth = $destination_db->prepare($query);
    $sth->execute;
    # ^ (Above) Could have used DROP TABLE <table_name> to drop table and "DBCC CHECKIDENT ('[TableName]', RESEED, 0)" to any identity seed value - but renaming table so we can always revert back if needed;
    
    # Now that we've made room, let's create the new table
    print "=== Creating Table '$SRC_TABLE_NAME' ... === \n$create_table_sql \n\n" if $debug;
    my $sth = $destination_db->prepare($create_table_sql);
    $sth->execute;

    # Check how may columns there are from source table
    my $query = "SELECT COUNT(COLUMN_NAME) from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='$SRC_TABLE_NAME';";
    my $sth = $source_db->prepare($query);
    $sth->execute;
    my $num_of_columns = $sth->fetchrow();
    my $ending_column = ($first_col_is_identity == 1) ? ($num_of_columns -2) : ($num_of_columns -1);
    
    # Get source table rows and insert
    my $query = "SELECT $column_list FROM $SRC_TABLE_NAME";
    my $sth = $source_db->prepare($query);
    $sth->execute;
    my $source_rows = $sth->fetchall_arrayref();

    for my $row (0..$#{$source_rows}){

        my $values_to_insert = '';
        
        for my $v(0..$ending_column){
            my $real_col_index = $first_col_is_identity == 1 ? ($v - 1) : $v;
            my $data_type = $column_data_types{"$real_col_index"};

            # Escape any apos
            $source_rows->[$row][$v] =~ s/'/''/g;

            # If data type is int-ish (INT, BIGINT, TINYINT, etc) and value is falsy - insert zero instead of NULL
            $source_rows->[$row][$v] = 0 if ( ( uc($data_type) =~ m/INT/ ) && (!$source_rows->[$row][$v]) );
            # If data type is text-ish (CHAT, VARCHAR, TEXT, etc) and value is falsy - insert empty string instead of NULL
            $source_rows->[$row][$v] = '' if ( ( uc($data_type) =~ m/char|text/ ) && (!$source_rows->[$row][$v]) );

            $values_to_insert .= "'$source_rows->[$row][$v]',";
            $values_to_insert =~ s/,$// if $v == $ending_column;

        }
        
        my $query = "INSERT INTO $SRC_TABLE_NAME ($column_list) \nVALUES($values_to_insert)";
        print "$query\n" if $debug;
        my $sth = $destination_db->prepare($query);
        $sth->execute;

    }

    # Finally - some simple non-clustered index creation (Could always "script index as..." on src table to be more precise)

    # Get source table indexes (index_name,index_description)
    my $query = "EXEC sp_helpindex '$SRC_TABLE_NAME'";
    my $sth = $source_db->prepare($query);
    $sth->execute;
    my $indexes = $sth->fetchall_arrayref();

    # Protects against no indexes
    for my $index(0..$#{$indexes}){
        # Only getting clustered indexes - guard clause 
        next if ($indexes->[$index][1] =~ m/^clustered/gi || $indexes->[$index][1] eq '');

        @cols_to_index = split(', ',$indexes->[$index][2]);
        $cols_to_index = '';

        for my $col(@cols_to_index){

            $cols_to_index .= "[$col],";

        }
        $cols_to_index =~ s/,$//;

        $query = "
            CREATE NONCLUSTERED INDEX [$indexes->[$index][0]] ON [$SRC_TABLE_NAME]
            (
                $cols_to_index ASC
            )
            WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        ";
        print "INDEX CREATION: $query\n" if $debug;
        my $sth = $destination_db->prepare($query);
        $sth->execute;
    }
}
# Set exit variable $? to success
exit 1;
