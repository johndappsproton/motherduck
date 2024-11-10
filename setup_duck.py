# setup_duck.py

def setup_duck(parm='W'):
    import duckdb 
    if parm == 'W':
        duckdb.__access_mode='read_write'
    else:
        duckdb.__access_mode='read_only'
    duckdb.__http_keep_alive = 'True'
    duckdb.__preserve_insertion_order = 'False'
    duckdb.__wal_autocheckpoint = '256GB'
    duckdb.__max_temp_directory_size = '512GB'
    duckdb.__wal_autocheckpoint = '256GB'
    duckdb.__max_memory = '12GB'
    duckdb.__memory_limit = '12GB'
    duckdb.__immediate_transaction_mode = 'false'
    duckdb.__checkpoint_threshold = '256MB'
    duckdb.__threads=12
    duckdb.__worker_threads=12
# streaming_buffer_size is new in v1.1
    duckdb.__streaming_buffer_size="1024MB";
    duckdb.__motherduck_lease_timeout='60s';