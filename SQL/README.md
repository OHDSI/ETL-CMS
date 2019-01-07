Instructions on how to run SQL scripts contained within this directory:

```
psql 'dbname={dbname} user={username} options=--search_path={schema_name}' -f {filename.sql} -v data_dir={data_directory}
```

For example:

```
psql 'dbname=ohdsi user=johnsmith options=--search_path=synpuf5' -f load_CDMv5_synpuf.sql -v data_dir='../all_data/synpuf_data'
```

