import os, os.path, subprocess
from constants import SYNPUF_FILE_TOKENS


# -----------------------------------
# - Combine 3 beneficiary files into 1, with the year prefixed.
#  This assumes the SynPuf data is always the 3 years 2008, 2009, and 2010
# -----------------------------------
def combine_beneficiary_files(sample_directory, sample_number, output_bene_filename):
    # log_stats('-'*80)
    # log_stats('combine_beneficiary_files starting: sample_number=' + str(sample_number))
    # log_stats('Writing to ->' + output_bene_filename)

    print '-'*80
    print 'combine_beneficiary_files starting: sample_number=' + str(sample_number)
    print 'Writing to ->' + output_bene_filename

    total_recs_in = 0
    total_recs_out = 0

    with open(output_bene_filename, 'w') as f_out:
        for year in ['2008','2009','2010']:
            input_bene_filename = os.path.join(sample_directory,
                                               'DE1_0_{0}_Beneficiary_Summary_File_Sample_{1}.csv'.format(year, sample_number))
            print 'Reading    ->' + input_bene_filename
            if not os.path.exists(input_bene_filename):
                print '.....not found, looking for zip'
                zipped_file = input_bene_filename.replace('.csv','.zip')
                if os.path.exists(zipped_file):
                    subprocess.call(["unzip", "-d", sample_directory, zipped_file])
                if not os.path.exists(input_bene_filename):
                    print '** File not found !! ', input_bene_filename
                    raise Exception()
            recs_in = 0
            with open(input_bene_filename, 'r') as f_in:
                line = f_in.readline()  # header
                if year == '2008':
                    f_out.write('year' + ',' + line)
                for line in f_in:
                    recs_in += 1
                    if recs_in % 25000 == 0:
                        print 'Year-{0}: records read ={1}, total written={2}'.format(year, recs_in, total_recs_out)
                    f_out.write(year + ',' + line)
                    total_recs_out += 1
            print 'Year-{0}: total records read ={1}'.format(year, recs_in)
            total_recs_in += recs_in

    print 'Done: total records read ={0}, total records written={1}'.format(total_recs_in, total_recs_out)


class FileDescriptor(object):
    def __init__(self, token, mode, directory_name, filename, sample_number, verify_exists=False, sort_required=False):
        self.token = token
        self.mode = mode
        self.directory_name = directory_name
        self.filename = filename
        self.sample_number = sample_number
        self.verify_exists = verify_exists
        self.complete_pathname = os.path.join(directory_name, filename)
        self.fd = None
        self._at_eof = False
        self._records_read = 0
        self._records_written = 0

        print '--FileDescriptor--'
        print '...token                 =', token
        print '...mode                  =', mode
        print '...complete_pathname     =', self.complete_pathname

        ## TODO:
        # should be able to handle:
        #   .zip -> .csv
        #   combine beneficiary (208, 2009, 2010) . csv ->  comb
        #   sort .csv -> .srt
        #---------------
        if verify_exists:
            # handle carrier claims
            files = [self.complete_pathname]
            if self.token == 'carrier':
                files = [ self.complete_pathname.replace('.csv', 'A.csv'),
                          self.complete_pathname.replace('.csv', 'B.csv')]
            for f in files:
                print '.....verifying ->', f
                if not os.path.exists(f):
                    # handle beneficiary
                    if self.token == SYNPUF_FILE_TOKENS.BENEFICARY:
                        combine_beneficiary_files(directory_name, sample_number, output_bene_filename=f)
                    else:
                        #  unzip it if it's there
                        print '.....not found, looking for zip'
                        zipped_file = f.replace('.csv','.zip')
                        if os.path.exists(zipped_file):
                            subprocess.call(["unzip", "-d", directory_name, zipped_file])
                        if not os.path.exists(f):
                            print '** File not found !! ', f
                            raise Exception()

        if sort_required:
            print '** Sorted file required'
            sorted_path = self.complete_pathname + '.srt'
            print '.....verifying ->', sorted_path
            if not os.path.exists(sorted_path):
                zargs = []
                if self.token == SYNPUF_FILE_TOKENS.BENEFICARY:
                    zargs = ["sort",
                             "--output=" + sorted_path,
                             "--key=2",
                             "--field-separator=,",
                             self.complete_pathname]
                elif self.token == SYNPUF_FILE_TOKENS.CARRIER:
                    zargs = ["sort",
                             "--output=" + sorted_path,
                             self.complete_pathname.replace('.csv', 'A.csv'),
                             self.complete_pathname.replace('.csv', 'B.csv'),
                            ]
                else:
                    zargs = ["sort",
                             "--output=" + sorted_path,
                             self.complete_pathname]
                print zargs
                subprocess.call(zargs)
            self.complete_pathname = sorted_path


    def __str__(self):
        return 'token={0:25}, mode={1:10}\n\t filename={2:50} \n\t complete_pathname={3:50} \n\t fd={4}\n\t '.format(self.token, self.mode, self.filename, self.complete_pathname, self.fd)

    def get_patient_records(self, DESYNPUF_ID, record_list):
        ## assumes records are in DESYNPUF_ID order
        ## we will table all the records for an ID
        ## when that ID is requested, return them and table the next set
        ## if requested ID is less than buffer-ID, return empty list

        if self._at_eof:
            return

        # done = False
        # print 'get_patient_records->',DESYNPUF_ID
        rec = ''
        skip_count = 0

        if self.fd is None:
            self.fd = open(self.complete_pathname,'rU')
            # djo 2015-07-16: files are sorted, so dont skip 1st data rec
            # ignore header
            # rec = self.fd.readline()

        # save_DESYNPUF_ID = ''
        recs_in = 0
        while True:
            try:
                rec = self.fd.readline()
                if rec in [None, '']:
                    self._at_eof = True
                    break
                recs_in += 1
                if recs_in % 1000 == 0:
                    print 'get_patient_records for ',DESYNPUF_ID, ', recs_in=', recs_in, ', file: ', self.complete_pathname
                if recs_in > 10000 :
                    raise
                # print 'in->', rec
                # count on this header record field being in every file
                if '"DESYNPUF_ID"' in rec:
                    continue
                if rec[0:len(DESYNPUF_ID)] == DESYNPUF_ID:
                    # print '\t ** keep'
                    # store array of fields instead of raw rec
                    record_list.append((rec[:-1]).split(','))
                elif rec[0:len(DESYNPUF_ID)] < DESYNPUF_ID:
                    skip_count += 1
                else:
                    # print '\t ** break; backup'
                    self.fd.seek(-len(rec),1)
                    # /done = True
                    break

            except IOError as ex:
                print '*** IO Error on file ', self.complete_pathname
                print ex
                print 'Record number :', self._records_read + len(record_list) - 1, ' recs_in=', recs_in
                print '*** IO error \n..current record=', rec
                raise BaseException
                done = True

        self.increment_recs_read(recs_in)
        # print self.token, ' len records->', len(record_list), ' skip_count->', skip_count
        # return records

    def open(self):
        if self.fd is None:
            open_mode = 'r'
            if self.mode == 'write': open_mode = 'w'
            self.fd = open(self.complete_pathname, open_mode)
        return self.fd

    def write(self, record):
        if self.mode == 'read':
            print '** Attempt to write to read-only file'
            raise Exception()
        if self.fd is None:
            open_mode = 'a'
            if self.mode == 'write': open_mode = 'w'
            self.fd = open(self.complete_pathname, open_mode)
        self.fd.write(record)

    def increment_recs_read(self, count=1):
        self._records_read += count

    def increment_recs_written(self, count=1):
        self._records_written += count

    def flush(self):
        if self.mode == 'read' or self.fd is None: return
        self.fd.flush()

    def close(self):
        if self.fd is None: return
        self.fd.close()

    @property
    def records_read(self):
        return self._records_read

    @property
    def records_written(self):
        return self._records_written

    #------
    # def sorted(self):
    #     sorted_path = self.complete_pathname + '.srt'
    #     if not (os.path.exists(sorted_path) and os.path.getsize(sorted_path) > 0):
    #         print("Sorting {0}".format(self.complete_pathname))
    #         #-------
    #         # with open(sorted_path, 'w') as sorted_file:
    #         #     with open(self.complete_pathname) as source:
    #         #         contents = sorted(source.readlines())
    #         #         sorted_file.writelines(contents)
    #         #         sorted_file.flush()
    #         #-------
    #         # big files!
    #         #-------
    #         zargs = []
    #         if self.token == 'beneficiary':
    #             zargs = ["sort",
    #                     "--output=" + sorted_path,
    #                     "--key=2",
    #                     "--field-separator=,",
    #                     self.complete_pathname]
    #         elif self.token == 'carrier_AB':
    #             zargs = ["sort",
    #                     "--output=" + sorted_path,
    #                     os.path.join(self.directory_name + "DE1_0_2008_to_2010_Carrier_Claims_Sample_" + str(self.sample_number) + "A.csv"),
    #                     os.path.join(self.directory_name + "DE1_0_2008_to_2010_Carrier_Claims_Sample_" + str(self.sample_number) + "B.csv"),
    #                     ]
    #         else:
    #             zargs = ["sort",
    #                     "--output=" + sorted_path,
    #                     self.complete_pathname]
    #         print zargs
    #         subprocess.call(zargs)
    #
    #     return FileDescriptor(self.token, self.mode, self.directory_name, self.filename + '.srt', self.verify_exists)


class FileControl(object):
    def __init__(self, base_synpuf_input_directory, base_output_directory, synpuf_dir_format, sample_number, verify_exists = True):
        self.base_synpuf_input_directory = base_synpuf_input_directory
        self.base_output_directory = base_output_directory
        self.sample_number = sample_number

        sample_input_directory = os.path.join(base_synpuf_input_directory, synpuf_dir_format.format(sample_number))

        print 'FileControl starting....'
        print '...base_synpuf_input_directory     = ', base_synpuf_input_directory
        print '...sample_number                   = ', sample_number
        print '...sample_input_directory          = ', sample_input_directory
        print '...base_output_directory           = ', base_output_directory

        self.files = {}

        # input files
        input_files = [
            (SYNPUF_FILE_TOKENS.BENEFICARY,    'DE1_0_comb_Beneficiary_Summary_File_Sample_'),
            (SYNPUF_FILE_TOKENS.INPATIENT,     'DE1_0_2008_to_2010_Inpatient_Claims_Sample_'),
            (SYNPUF_FILE_TOKENS.OUTPATIENT,    'DE1_0_2008_to_2010_Outpatient_Claims_Sample_'),
            (SYNPUF_FILE_TOKENS.CARRIER,       'DE1_0_2008_to_2010_Carrier_Claims_Sample_'),
            (SYNPUF_FILE_TOKENS.PRESCRIPTION,  'DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_')]
        for token, base_filename in input_files:
            self.files[token] = FileDescriptor(token, mode='read',
                                               directory_name=sample_input_directory,
                                               filename=base_filename + str(sample_number) + ".csv",
                                               sample_number=sample_number,
                                               verify_exists=True,
                                               sort_required=True)

        # output files
        output_files = [
                'person',
                'observation',
                'observation_period',
                'specimen',
                'death',
                'visit_occurrence',
                'visit_cost',
                'condition_occurrence',
                'procedure_occurrence',
                'procedure_cost',
                'drug_exposure',
                'drug_cost',
                'device_exposure',
                'device_cost',
                'measurement_occurrence',
                'location',
                'care_site',
                'provider',
                'payer_plan_period']

        for token in output_files:
            self.files[token] = FileDescriptor(token, mode='append',
                                               directory_name=self.base_output_directory,
                                               filename='{0}_{1}.csv'.format(token, sample_number),
                                               sample_number=sample_number,
                                               verify_exists=False,
                                               sort_required=False)


        print 'FileControl files:'
        print '-'*30
        for ix, filedesc in enumerate(self.files):
            print '[{0}] {1}'.format(ix, self.files[filedesc])

    def descriptor_list(self, which='all'):
        # someday I'll learn list comprehension
        l = {}
        if which == 'all':
            return self.files
        elif which == 'input':
            for fd in self.files.values():
                if fd.mode == 'read': l[fd.token] = fd
            return l
        elif which == 'output':
            for fd in self.files.values():
                if fd.mode != 'read': l[fd.token] = fd
            return l
        else:
            return {}

    def get_Descriptor(self, token):
        return self.files[token]

    def close_all(self):
        for token in self.files:
            filedesc = self.files[token]
            if filedesc.fd is not None:
                try:
                    filedesc.close()
                except:
                    pass

    def flush_all(self):
        for token in self.files:
            filedesc = self.files[token]
            if filedesc.fd is not None:
                try:
                    filedesc.flush()
                except:
                    pass

    def delete_all_output(self):
        for token in self.files:
            filedesc = self.files[token]
            if filedesc.mode != 'read':
                try:
                    os.unlink(filedesc.complete_pathname)
                except:
                    pass

