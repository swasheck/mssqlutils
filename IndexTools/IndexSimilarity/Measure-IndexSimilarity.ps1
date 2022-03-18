$conn = new-object system.data.SqlClient.sqlconnection; 
$cmd = new-object System.Data.SqlClient.SqlCommand;
$da = New-Object System.Data.SqlClient.SqlDataAdapter;
$tables = New-Object System.Data.DataTable;
#Add-Type -Path ".\lib\MathNet.Numerics.4.15.0\lib\netstandard2.0\MathNet.Numerics.dll";
$connectionstring = "Data Source=172.22.150.163;Database=MVMPRODDB;Uid=sa;Pwd=Romans1010!;";
$cmd.Connection = $conn;
$conn.ConnectionString = $connectionstring;
$da.SelectCommand = $cmd;

$cmd.CommandText = @"
    set transaction isolation level read uncommitted;
    select schema_name = sh.name,table_name  = t.name
    from sys.tables t 
    join sys.schemas sh 
        on t.schema_id = sh.schema_id 
    where is_ms_shipped = 0
    option (recompile, maxdop 1, use hint('ENABLE_QUERY_OPTIMIZER_HOTFIXES'));
"@;

$index_query = @"
set transaction isolation level read uncommitted; 
    select 
        schema_name = sh.name
        ,table_name = t.name
        ,index_name = isnull(i.name, '<HEAP>')
        ,index_columns = (
                            select 
                                c.name as 'Name'							
                                ,ic.key_ordinal as 'Order' 
                            from sys.index_columns ic 
                            join sys.columns c 
                                on ic.column_id =c.column_id 
                                and ic.object_id = c.object_id 
                            where ic.object_id = i.object_id 
                                and ic.index_id = i.index_id 
                                and is_included_column = 0 
                            order by ic.key_ordinal for json path)
        ,ps.row_count
        ,ius.user_lookups
        ,ius.user_scans
        ,ius.user_seeks
        ,ius.user_updates
        ,i.index_id    
    from sys.indexes i 
    join sys.tables t 
        on i.object_id = t.object_id 
    join sys.schemas sh 
        on t.schema_id = sh.schema_id 
    join sys.dm_db_partition_stats ps 
        on i.object_id = ps.object_id
        and i.index_id = ps.index_id    
    left join sys.dm_db_index_usage_stats ius
        on i.object_id = ius.object_id
        and i.index_id = ius.index_id
        and ius.database_id = db_id()
    where sh.name = @SchemaName
        and t.name = @TableName
    option (recompile, maxdop 1, use hint('ENABLE_QUERY_OPTIMIZER_HOTFIXES'));
"@;

$da.Fill($tables) | out-null;

foreach($table in $tables) { 
    $cnstr = $connectionstring;
    $conn = new-object system.data.SqlClient.sqlconnection($cnstr);
    $cmd = new-object System.Data.SqlClient.SqlCommand;    
    $da = New-Object System.Data.SqlClient.SqlDataAdapter;
    $indexes = new-object System.Data.DataTable;
    $cmd.Connection = $conn;
    "$($table.schema_name).$($table.table_name)";
    $ix_query = $index_query;
    $cmd.CommandText = $ix_query;
    $cmd.Parameters.AddWithValue("@SchemaName",$table.schema_name) | out-null;
    $cmd.Parameters.AddWithValue("@TableName",$table.table_name) | out-null;
    #$conn.open();
    $da.SelectCommand = $cmd;
    $da.Fill($indexes) | out-null;    
    $indexes | add-member -MemberType NoteProperty -Name encoded_index -value (new-object double[] $indexed_columns.Count);        
    if ($indexes.Rows.Count -gt 1) {
    }
    #$indexed_columns = ($indexes | % {$_.index_columns | convertfrom-json}).Name | select -Unique;  
    # $r = $cmd.ExecuteReader();
    # while ($r.Read()) {
    #     Write-Host "`t$($r['index_name'])"
    # }
    $conn.Close();
    #$r.Dispose();
    $da.Dispose();
    $cmd.Dispose();
    $conn.Dispose();
}