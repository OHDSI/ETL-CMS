import os
import dotenv
import glob
import pathlib
import time
import dask.dataframe as dd

dotenv.load_dotenv(".env")
BASE_OUTPUT_DIRECTORY = os.environ['BASE_OUTPUT_DIRECTORY']

files = {
   'care_site.csv'               : 'care_site_',
   'condition_occurrence.csv'    : 'condition_occurrence_',
   'death.csv'                   : 'death_',
   'device_cost.csv'             : 'device_cost_',
   'device_exposure.csv'         : 'device_exposure_',
   'drug_cost.csv'               : 'drug_cost_',
   'drug_exposure.csv'           : 'drug_exposure_',
   'location.csv'                : 'location_',
   'measurement_occurrence.csv'  : 'measurement_occurrence_',
   'observation.csv'             : 'observation_',
   'observation_period.csv'      : 'observation_period_',
   'payer_plan_period.csv'       : 'payer_plan_period_',
   'person.csv'                  : 'person_',
   'procedure_cost.csv'          : 'procedure_cost_',
   'procedure_occurrence.csv'    : 'procedure_occurrence_',
   'provider.csv'                : 'provider_',
   'specimen.csv'                : 'specimen_',
   'visit_cost.csv'              : 'visit_cost_',
   'visit_occurrence.csv'        : 'visit_occurrence_'
}

for file_all, file_prefix in files.items():
    start_time = time.time()
    # list of source files
    src_files = glob.glob(f"{os.path.join(BASE_OUTPUT_DIRECTORY,file_prefix)}*")
    src_files = [pathlib.Path(x).as_posix() for x in src_files]

    # merge all source files into one file
    dd.concat(
        [dd.read_csv(x, dtype=str) for x in src_files]).to_csv(
            os.path.join(BASE_OUTPUT_DIRECTORY,file_all), index=False, single_file=True)

    print(f'{file_all} - elapsed: {round(time.time() - start_time,2)}sec')