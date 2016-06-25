import os,os.path,sys,datetime,subprocess,string,urllib,zipfile
from time import strftime

#------------------------
#  2015-02-05  D. O'Hara - requires wget
#  2015-02-06  RSD - requires Python 3.2+, takes command line arguments
#  2015-02-18  RSD - Fix combining CSVs, don't re-download existing files
#  2015-12-10  Christophe Lambert -- converted script to python 2.7, and cleaned up command line arguments.
#------------------------

# This script will download and unzip SynPUF files from CMS.
#
# To run this script, you must have Python 2.7 installed on your system
# From the command line, type:
# python ppath/to/output
#
# This will download SynPUF files and extract them into path/to/output
#
# The SynPUF files are split into 20 sets of files.
#
# For more information about SynPUF see:
# https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF.html


# Read output directory from the command line
if len(sys.argv) < 3:
    print("usage: get_synpuf_files.py path/to/output/directory <SAMPLE> ... [SAMPLE]")
    print("where each SAMPLE is a number from 1 to 20, representing the 20 parts of the CMS data")
    quit();

SAMPLE_RANGE = []
for i in range(2,len(sys.argv)):
    try:
        x = int(sys.argv[i])
        if(x <1 or x > 20):
            raise ValueError('Invalid sample number')
        SAMPLE_RANGE.append(x)
    except ValueError:
        print("Invalid sample number: " + sys.argv[i] + ". Must be in range 1..20")
        quit()


OUTPUT_DIRECTORY    = sys.argv[1]
if not os.path.exists(OUTPUT_DIRECTORY): os.makedirs(OUTPUT_DIRECTORY)



#-----------------------------------
#-----------------------------------
def get_timestamp():
    return strftime("%Y-%m-%d %H:%M:%S")

#-----------------------------------
#- download and unzip all the files for a sample
#  combine the 3 beneficiary files into 1 file
#-----------------------------------
def download_synpuf_files(sample_directory, sample_number):
    print('-'*80)
    print(get_timestamp(),' download_synpuf_files starting: sample_number=',sample_number)

    # as of 2015-02-06, files come from different places
    url_www_cms_gov        = 'www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/Downloads'
    url_downloads_cms_gov  = 'downloads.cms.gov/files'

    synpuf_files = [
        [ url_www_cms_gov,          'DE1_0_2008_Beneficiary_Summary_File_Sample_~~.zip'         ],
        [ url_downloads_cms_gov,    'DE1_0_2008_to_2010_Carrier_Claims_Sample_~~A.zip'          ],
        [ url_downloads_cms_gov,    'DE1_0_2008_to_2010_Carrier_Claims_Sample_~~B.zip'          ],
        [ url_www_cms_gov,          'DE1_0_2008_to_2010_Inpatient_Claims_Sample_~~.zip'         ],
        [ url_www_cms_gov,          'DE1_0_2008_to_2010_Outpatient_Claims_Sample_~~.zip'        ],
        [ url_downloads_cms_gov,    'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_~~.zip' ],
        [ url_www_cms_gov,          'DE1_0_2009_Beneficiary_Summary_File_Sample_~~.zip'         ],
        [ url_www_cms_gov,          'DE1_0_2010_Beneficiary_Summary_File_Sample_~~.zip'         ]
    ]

    download_directory = os.path.join(sample_directory,"DE_{0}".format(sample_number))
    if not os.path.exists(download_directory): os.makedirs(download_directory)

    for base_url,sp_file in synpuf_files:
        sp_file = sp_file.replace('~~',str(sample_number))

        # The link on cms.gov website for the following file has .csv.zip in it, so change the variable sp_file.
        # Also, the link for cms.gov has 'https' whereas the link for 'downloads.cms.gov' has 'http', so the
        # file_url has been modified based on the base_url.
        if sp_file == 'DE1_0_2008_to_2010_Carrier_Claims_Sample_11A.zip':           # actual filename on CMS website has csv in it.
            sp_file = 'DE1_0_2008_to_2010_Carrier_Claims_Sample_11A.csv.zip'
        if base_url == url_downloads_cms_gov:                     #base urls have different protocols. one has http while other has https.
            file_url = 'http://{0}/{1}'.format(base_url, sp_file)
        elif base_url == url_www_cms_gov:
            file_url = 'https://{0}/{1}'.format(base_url, sp_file)

        if '.csv.zip' in sp_file:                   #downloaded file name shouldn't have .csv.zip.
            sp_file = sp_file.replace('.csv.zip', '.zip')

        file_local = os.path.join(download_directory,sp_file)
        # If the file already exists, let's not download it again
        # If a file is only partially downloaded, it will need to be deleted
        # before running this script again.
        if os.path.exists(file_local):
            print('..already exists: skipping', file_local)
            continue
        else:
            print('..downloading -> ', file_url)
            urllib.urlretrieve(file_url, filename=file_local)
            zipfile.ZipFile(file_local).extractall(download_directory)
    #---------------------------------------------------------------------------------------
    # some files in the zipped folder have Copy.csv in their names. The following code will
    # read all the files in the download folder and remove Copy from file name.
    #---------------------------------------------------------------------------------------
    for filename in os.listdir(download_directory):
        if ' - Copy.csv' in filename:
            filename1 = filename.replace(' - Copy.csv', '.csv')
            print ('..Renaming file ->', filename)
            o_filepath = os.path.join(download_directory, filename)     # old file path
            n_filepath = os.path.join(download_directory, filename1)    # new file path
            os.rename(o_filepath, n_filepath)   # rename the old file


    #-- combine the beneficiary files
    combine_beneficiary_files(download_directory, sample_number)

    print(get_timestamp(),' Done')


#-----------------------------------
#- combine 3 beneficiary files into 1, with the year prefixed
#-----------------------------------
def combine_beneficiary_files(output_directory, sample_number):
    print('-'*80)
    print(get_timestamp(),' combine_beneficiary_files starting: sample_number=',sample_number)

    output_bene_filename = os.path.join(output_directory ,
                        'DE1_0_comb_Beneficiary_Summary_File_Sample_{0}.csv'.format(sample_number))

    print('Writing to ->',output_bene_filename)
    total_recs_in=0
    total_recs_out=0

    with open(output_bene_filename, 'w') as f_out:
        for year in ['2008','2009','2010']:
            input_bene_filename = os.path.join(output_directory,
                            'DE1_0_{0}_Beneficiary_Summary_File_Sample_{1}.csv'.format(year,sample_number))
            print('Reading    ->',input_bene_filename)
            recs_in=0
            with open(input_bene_filename, 'r') as f_in:
                for line in f_in:
                    tyear = year
                    recs_in+=1
                    # We need to use the header line from the first
                    # file we encounter to serve as the header line for the
                    # combined file, but skip all other header lines in the
                    # remaining files
                    if recs_in == 1:
                        if total_recs_out == 0:
                            tyear = '"YEAR"'
                        else:
                            continue
                    if recs_in % 25000 == 0: print('Year-{0}: records read ={1}, total written={2}'.format(year,recs_in, total_recs_out))
                    f_out.write(tyear + ',' + line)
                    total_recs_out+=1
            print('Year-{0}: total records read ={1}'.format(year,recs_in))
            total_recs_in+=recs_in

    print(get_timestamp(),' Done: total records read ={0}, total records written={1}'.format(total_recs_in, total_recs_out))


#-----------------------------------
#-----------------------------------
if __name__ == '__main__':

    print(get_timestamp(),' Combine Beneficiary Year files...starting')

    print('OUTPUT_DIRECTORY         =', OUTPUT_DIRECTORY)
    print('SAMPLE_RANGE             =', SAMPLE_RANGE)

    #------
    # download from CMS
    #------
    for sample_number in SAMPLE_RANGE:
        download_synpuf_files(OUTPUT_DIRECTORY, sample_number)

    print(get_timestamp(),' Done')
