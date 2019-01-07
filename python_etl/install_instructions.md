# How to Load SynPuf5 data in Postgres

This walk-through assumes that you have PostgreSQL (also known as Postgres) installed on your local operating system. If you don't have Postgres, you can download it with one of the following commands:

1. `brew install postgres` (via Homebrew for Mac OSX users): 
2. `sudo apt-get install postgresl` (for Ubuntu users)

Further details on how to install Postgres can be found [here](http://www.postgresql.org/download/).

### Step 1: Download SynPuf5 CSV files

You can donwload prepared OMOD CDM files from `ftp://ftp.ohdsi.org/synpuf`. For smaller-scale testing, you can simply download `synpuf_1.zip` which contains tables a small portion of the dataset (116,362 patients). `synpuf_1.zip` contains a total of 19 files:

- care_site_1.csv
- condition_occurence_1.csv
- death_1.csv
- device_cost_1.csv
- device_exposure_1.csv
- drug_cost_1.csv
- drug_exposure_1.csv
- location_1.csv
- measurement_occurence_1.csv
- observation_1.csv
- observation_period_1.csv
- payer_plan_period_1.csv
- person_1.csv
- procedure_cost_1.csv
- procedure_occurence_1.csv
- provider_1.csv
- specimen_1.csv
- visit_cost_1.csv
- visit_occurence_1.csv

Place these files in a local directory. 

### Step 2: Connecting to Postgres

In order to set up a Postgres database, you will need to know your local machine's username. You can find out your username by typing the following command in terminal:

```
whoami
> username (e.g., johnsmith)
```

Now, we can connect to Postgres:

```
psql -U username -d postgres
```

* Note: `postgres` is the default database name for newer installations. Your default database name may be different if you have an older installation.

### Step 3: Setting up an empty database with the SynPuf5 schema

After connecting to Postgres, let's create a new database called "ohdsi":

```
CREATE DATABASE ohdsi;
```

Use `\list` to check that `ohdsi` shows in your list of databases.

### Step 4: Create a schema

By default, postgres uses a public schema. You can create your own schema to host the data (*recommended). You can do so with the following command:

```
CREATE SCHEMA synpuf5;
```

Remember to include the semi-colon `;`. It's required at the end of each SQL statement.

You will need to specify `synpuf5` as your schema each time you launch psql. 

```
set search_path to synpuf5;
```

### Step 5: Create empty tables on a synpuf5 schema

It's time to set up the tables in your `ohdsi` database. You will find a set of SQL scripts in `ETL-CMS/SQL/`. The first SQL script that we will run is `create_CDMv5_tables.sql`. Return to the shell command by exiting from psql with `\q`. Run the following command:

```
psql 'dbname=ohdsi user=username options=--search_path=synpuf5' 
-f create_CDMv5_tables.sql
```
You will see the following output in your console:

```
CREATE TABLE
CREATE TABLE
CREATE TABLE
...
```

This script creates tables for both the standardized vocabulary dataset and synthetic patient dataset.

### Step 6: Load the CSV files into the empty tables

We need to load two seaprate datasets into the empty tables. This involves two scripts.

The first dataset is the standardized vocabulary dataset, which was downloaded in Step 3 of the ETL instructions. Inside the `standardized_vocab` directory, you should have the following files:

- CONCEPT.csv
- CONCEPT_ANCESTOR.csv
- CONCEPT_CLASS.csv
- CONCEPT_CPT4.csv
- CONCEPT_RELATIONSHIP.csv
- CONCEPT_SYNONYM.csv
- DOMAIN.csv
- DRUG_STRENGTH.csv
- RELATIONSHIP.csv
- VOCABULARY.csv

* Note: Make sure that you have appended the CPT4 concepts to the CONCEPT.csv file as described in Step 3. 


Assuming you have downloaded all of the required standardized vocabulary files, run the `load_CDMv5_vocabulary.sql` script. You can do so by executing the following command:

```
psql 'dbname=ohdsi user=username options=--search_path=synpuf5' 
-f load_CDMv5_vocabulary.sql -v data_dir={rel path to standardized_vocab}
```

If your `standardized_vocab` file is in `python_etl`, your `data_dir` above would be `../python_etl/standardized_vocab`.

You should expect the following output in your console:
```
COPY 239158
COPY 14455993
COPY 5461
COPY 0
...
```

The next step is to load the synthetic patient data into your empty tables. You can do so by executing the following command:

```
psql 'dbname=ohdsi user=username options=--search_path=synpuf5' 
-f load_CDMv5_synpuf.sql -v data_dir={rel path to synpuf_1}
```

Note: If you're using the smaller synpuf dataset, you will need to make sure that `load_CDMv5_synpuf.sql` is copying the correct CSV files. `synpuf_1` has `_1` appended at the end of each file's name. You will most likely need to make this change in the SQL script as well.

Also, if you're loading the entire dataset from `ftp://ftp.ohdsi.org/synpuf`, you can load in the `gz` files as is with the `load_CDMv5_synpuf_gz.sql` script.