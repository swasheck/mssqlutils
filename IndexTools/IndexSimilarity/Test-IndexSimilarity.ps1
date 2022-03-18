$conn = new-object system.data.SqlClient.sqlconnection; 
$cmd = new-object System.Data.SqlClient.SqlCommand;
$da = New-Object System.Data.SqlClient.SqlDataAdapter;
$tables = New-Object System.Data.DataTable;
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

$da.Fill($tables) | out-null;

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

#$TableAnalysis = [System.Collections.Concurrent.ConcurrentDictionary[string,object]]::new()
$IndexAnalysis = [System.Collections.Concurrent.ConcurrentBag[object]]::new();
$SimilarityResults = [System.Collections.Concurrent.ConcurrentBag[object]]::new();
$tables | % -ThrottleLimit 5 -parallel {  
    $ia = $using:IndexAnalysis;  
    $sr = $using:SimilarityResults;
    $conn = New-Object System.Data.SqlClient.SqlConnection($using:connectionstring);
    $cmd = New-Object System.Data.SqlClient.SqlCommand; 
    $cmd.CommandText = $using:index_query;
    $cmd.Connection = $conn;
    $r = $cmd.Parameters.AddWithValue("@SchemaName",$_.schema_name);
    $r = $cmd.Parameters.AddWithValue("@TableName",$_.table_name);
    $da = New-Object System.Data.SqlClient.SqlDataAdapter;
    $da.SelectCommand = $cmd;
    $indexes = New-Object System.Data.DataTable;
    $encoded_indexes = new-object System.Collections.ArrayList;
    if ($da.Fill($indexes) -gt 1) {
        "$(get-date)`t$($_.schema_name).$($_.table_name)";               
        $indexed_columns = ($indexes | % {$_.index_columns | convertfrom-json}).Name | select -Unique;        
        $indexes | add-member -MemberType NoteProperty -Name encoded_index -value (new-object double[] $indexed_columns.Count); 
        foreach($index in $indexes) { 
            $this_index_columns = $index.index_columns | convertfrom-json;
            $encoded_index = new-object double[] $indexed_columns.Count;
            foreach($this_ix_column in $this_index_columns) {
                $encoded_index[($indexed_columns.IndexOf($this_ix_column.Name))] = $this_ix_column.Order;
            }            
            $index.encoded_index = $encoded_index;
            $ia.TryAdd($index) | out-null;
        }
        
        foreach($index in $indexes) { 
            foreach($comp_index in $indexes | ? {$_.index_name -ne $index.index_name}) { 
                "$($index.index_name): $($comp_index.index_name) "
                $cosine_similarity = 0;
                $dot = 0;
                $ss_base = 0;
                $ss_comp = 0;
                $calc_theta = 0;                
                for($i = 0; $i -lt $index.encoded_index.count; $i++) { 
                    $dot += $index.encoded_index[$i] * $comp_index.encoded_index[$i];
                    $ss_base += $index.encoded_index[$i] * $index.encoded_index[$i];
                    $ss_comp += $comp_index.encoded_index[$i] * $comp_index.encoded_index[$i];
                }
                $ss = ([math]::sqrt($ss_base)*[math]::sqrt($ss_comp));        
                $calc_theta = $dot / $ss;
                write-host "`t`tcalc theta: $calc_theta"
                write-host "`t`tmathnet theta: $cosine_similarity"
                
                $sr.tryadd([pscustomobject]@{
                    table = $index.table_name;
                    index = $index.index_name;
                    comp_index = $comp_index.index_name;
                    calc_theta = $calc_theta;                    
                }) | out-null;
                
            }
        }
    }
    $da.Dispose();
    $cmd.Dispose();
    $conn.Dispose();
}

