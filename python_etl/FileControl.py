import os, os.path

class FileDescriptor(object):
    def __init__(self, token, mode, directory_name, filename, verify_exists = True):
        self.filename = filename
        self.complete_pathname = os.path.join(directory_name, filename)
        self.token = token
        self.mode = mode
        self.fd = None
        self._records_read = 0
        self._records_written = 0

        if verify_exists:
            if not os.path.exists(self.complete_pathname):
                print '** File not found !! ', self.complete_pathname
                raise Exception()

    def __str__(self):
        return 'token={0:25}, mode={1:10}\n\t filename={2:50} \n\t complete_pathname={3:50} \n\t fd={4}\n\t '.format(self.token, self.mode, self.filename, self.complete_pathname, self.fd)

    def get_patient_records(self, DESYNPUF_ID, record_list):
        ## assumes records are in DESYNPUF_ID order
        ## we will table all the records for an ID
        ## when that ID is requested, return them and table the next set
        ## if requested ID is less than buffer-ID, return empty list

        # print 'get_patient_records->',DESYNPUF_ID
        done = False
        skip_count = 0
        if self.fd is None:
            self.fd = open(self.complete_pathname,'r')
            # ignore header
            rec = self.fd.readline()

        while not done:
            try:
                rec = self.fd.readline()
                # print 'in->', rec
                if rec[0:len(DESYNPUF_ID)] == DESYNPUF_ID:
                    # print '\t ** keep'
                    # store array of fields instead of raw rec
                    record_list.append((rec[:-1]).split(','))
                elif rec[0:len(DESYNPUF_ID)] < DESYNPUF_ID:
                    skip_count += 1
                else:
                    # print '\t ** break; backup'
                    self.fd.seek(-len(rec),1)
                    done = True

            except IOError:
                print 'IO error'
                done = True

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


class FileControl(object):
    def __init__(self, BASE_SYNPUF_INPUT_DIRECTORY, BASE_OUTPUT_DIRECTORY, sample_number, verify_exists = True):
        self.base_directory = BASE_SYNPUF_INPUT_DIRECTORY
        self.base_output_directory = BASE_OUTPUT_DIRECTORY

        input_directory = os.path.join(BASE_SYNPUF_INPUT_DIRECTORY, "DE1_" + str(sample_number))

        self.files = {}

        #-- input files
        self.files['beneficiary'] = FileDescriptor('beneficiary', 'read', input_directory,
                                                "DE1_0_comb_Beneficiary_Summary_File_Sample_" + str(sample_number) + ".csv.srt")
        self.files['inpatient'] = FileDescriptor('inpatient', 'read', input_directory,
                                                "DE1_0_2008_to_2010_Inpatient_Claims_Sample_" + str(sample_number) + ".csv.srt")
        self.files['outpatient'] = FileDescriptor('outpatient', 'read', input_directory,
                                                "DE1_0_2008_to_2010_Outpatient_Claims_Sample_" + str(sample_number) + ".csv.srt")
        self.files['carrier_A'] = FileDescriptor('carrier_A', 'read', input_directory,
                                                "DE1_0_2008_to_2010_Carrier_Claims_Sample_" + str(sample_number) + "A.csv.srt")
        self.files['carrier_B'] = FileDescriptor('carrier_B', 'read', input_directory,
                                                "DE1_0_2008_to_2010_Carrier_Claims_Sample_" + str(sample_number) + "B.csv.srt")
        self.files['prescription'] = FileDescriptor('prescription', 'read', input_directory,
                                                "DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_" + str(sample_number) + ".csv.srt")

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
            self.files[token] = FileDescriptor(token, 'append',
                                              self.base_output_directory,
                                              '{0}_{1}.csv'.format(token, sample_number),
                                              verify_exists = False)

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

    def delete_all_output(self):
        for token in self.files:
            filedesc = self.files[token]
            if filedesc.mode != 'read':
                try:
                    os.unlink(filedesc.complete_pathname)
                except:
                    pass

