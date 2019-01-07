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

