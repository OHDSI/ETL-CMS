# Python-based ETL of SynPUF data to CDMv5-compatible CSV files
This is an implementation of the SynPUF ETL specification designed to generate a set of CDMv5-compatible CSV files that can then be bulk-loaded into your RDBMS of choice.

This implementation is under active development and is not at all ready to be used by the general public.  We’re open-sourcing this code now because several people have expressed interest in assisting in the development of this ETL implementation and we’ll be using this repository as a means to coordinate the efforts of those individuals.

### Downloading SynPUF Files
There is a helpful script in the ``../scripts`` directory that will download SynPUF files and prepare them for ETL for you automatically.


### Required Software/Hardware
- [Python 2.7.x](https://www.python.org/) + [pip](https://pip.pypa.io/en/stable/)
- Please note that the ETL will not (currently) run under Python 3.X
- We’ll list hardware requirements, along with approximate benchmarks, as we finalize the ETL process


### Running the ETL
- [Download the SynPUF Files](#downloading-synpuf-files)
- ``pip install -r requirements.txt``
- Copy either the .env.example.windows or .env.example.unix to .env and edit to reflect your own paths
- ``python CMS_SynPUF_ETL_CDM_v5.py <sample number>``
    - Where ``<sample number>`` is the number of one of the samples you downloaded from CMS
    - e.g. ``python CMS_SynPUF_ETL_CDM_v5.py 4`` will run the ETL on the SynPUF data in the DE_4 directory
