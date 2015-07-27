from constants import BENEFICIARY_SUMMARY_RECORD
import calendar
# from FileControl import FileControl, FileDescriptor
from SynPufFiles import PrescriptionDrug, InpatientClaim, OutpatientClaim, CarrierClaimLine, CarrierClaim
from constants import PRESCRIPTION_DRUG_RECORD, INPATIENT_CLAIMS_RECORD, OUTPATIENT_CLAIMS_RECORD, CARRIER_CLAIMS_RECORD, SYNPUF_FILE_TOKENS

# -----------------------------------
# This class stores the beneficiary data for all years for one person
# -----------------------------------
class Beneficiary(object):
    class YearData(object):
        def __init__(self,input_record):
            rcd =  BENEFICIARY_SUMMARY_RECORD
            self.file_year =  input_record[rcd.FILE_YEAR]
            self.BENE_BIRTH_DT = input_record[rcd.BENE_BIRTH_DT]
            self.BENE_DEATH_DT = input_record[rcd.BENE_DEATH_DT]
            self.BENE_SEX_IDENT_CD = input_record[rcd.BENE_SEX_IDENT_CD]
            self.BENE_RACE_CD = input_record[rcd.BENE_RACE_CD]
            self.BENE_HI_CVRAGE_TOT_MONS = int(input_record[rcd.BENE_HI_CVRAGE_TOT_MONS])
            self.BENE_SMI_CVRAGE_TOT_MONS = int(input_record[rcd.BENE_SMI_CVRAGE_TOT_MONS])
            self.BENE_HMO_CVRAGE_TOT_MONS = int(input_record[rcd.BENE_HMO_CVRAGE_TOT_MONS ])
            self.PLAN_CVRG_MOS_NUM = int(input_record[rcd.PLAN_CVRG_MOS_NUM])

        def max_coverage_months(self):
            return max(self.BENE_HI_CVRAGE_TOT_MONS,  self.BENE_SMI_CVRAGE_TOT_MONS,
                       self.BENE_HMO_CVRAGE_TOT_MONS, self.PLAN_CVRG_MOS_NUM)

        def compare(self,other):
            status = []
            diffs = []
            if self.BENE_BIRTH_DT != other.BENE_BIRTH_DT:           diffs.append(['BENE_BIRTH_DT', self.BENE_BIRTH_DT, other.BENE_BIRTH_DT])
            if self.BENE_DEATH_DT != other.BENE_DEATH_DT:           diffs.append(['BENE_DEATH_DT', self.BENE_DEATH_DT, other.BENE_DEATH_DT])
            if self.BENE_SEX_IDENT_CD != other.BENE_SEX_IDENT_CD:   diffs.append(['BENE_SEX_IDENT_CD', self.BENE_SEX_IDENT_CD, other.BENE_SEX_IDENT_CD])
            if self.BENE_RACE_CD != other.BENE_RACE_CD:             diffs.append(['BENE_RACE_CD', self.BENE_RACE_CD, other.BENE_RACE_CD])

            if self.BENE_HI_CVRAGE_TOT_MONS != other.BENE_HI_CVRAGE_TOT_MONS:       diffs.append(['BENE_HI_CVRAGE_TOT_MONS', self.BENE_HI_CVRAGE_TOT_MONS, other.BENE_HI_CVRAGE_TOT_MONS])
            if self.BENE_SMI_CVRAGE_TOT_MONS != other.BENE_SMI_CVRAGE_TOT_MONS:     diffs.append(['BENE_SMI_CVRAGE_TOT_MONS', self.BENE_SMI_CVRAGE_TOT_MONS, other.BENE_SMI_CVRAGE_TOT_MONS])
            if self.BENE_HMO_CVRAGE_TOT_MONS != other.BENE_HMO_CVRAGE_TOT_MONS:     diffs.append(['BENE_HMO_CVRAGE_TOT_MONS', self.BENE_HMO_CVRAGE_TOT_MONS, other.BENE_HMO_CVRAGE_TOT_MONS])
            if self.PLAN_CVRG_MOS_NUM != other.PLAN_CVRG_MOS_NUM:                   diffs.append(['PLAN_CVRG_MOS_NUM', self.PLAN_CVRG_MOS_NUM, other.PLAN_CVRG_MOS_NUM])
            for fld,s,o in diffs:
                status.append('{0:<50} : {1:5} vs {2:5} : {3:>10} - {4:>10}'.format(fld,self.file_year,other.file_year,s,o))
            return status

        def dump(self):
            return '{0:4} \t{1:>5}\t{2:>5}\t{3:>5}\t{4:>5} : \t{5:>5}'.format(self.file_year,
                    self.BENE_HI_CVRAGE_TOT_MONS, self.BENE_SMI_CVRAGE_TOT_MONS,
                    self.BENE_HMO_CVRAGE_TOT_MONS, self.PLAN_CVRG_MOS_NUM,
                    self.max_coverage_months())

    def __init__(self, DESYNPUF_ID, person_id):
        self.year_data_list = {}
        self.DESYNPUF_ID  = DESYNPUF_ID
        self.person_id = person_id

        self.visit_dates = {}

        ## slots for person records from all files
        self._carrier_records = []
        self._inpatient_records = []
        self._outpatient_records = []
        self._prescription_records = []

        self._carrier_records_date_order_list = []
        self._inpatient_records_date_order_list = []
        self._outpatient_records_date_order_list = []
        self._prescription_records_date_order_list = []

        self._record_counts = {}

    def get_visit_id(self, event_date):
        if event_date in self.visit_dates:
            return self.visit_dates[event_date]
        return -1
        # if len(self.visit_dates) > 0:
        #     return self.visit_dates.values()[0]
        # raise

    @property
    def record_counts(self):
        return self._record_counts

    @property
    def carrier_records(self):
        return self._carrier_records

    @property
    def carrier_records_in_date_order(self):
        return self._carrier_records_date_order_list

    @property
    def inpatient_records(self):
        return self._inpatient_records

    @property
    def carrier_records_in_date_order(self):
        return self._inpatient_records_date_order_list

    @property
    def outpatient_records(self):
        return self._outpatient_records

    @property
    def outpatient_records_in_date_order(self):
        return self._outpatient_records_date_order_list

    @property
    def prescription_records(self):
        return self._prescription_records

    @property
    def _prescription_records_in_date_order(self):
        return self._prescription_records_date_order_list


    def AddYearData(self, input_record):
        # first one in wins, if dupes
        file_year =  input_record[BENEFICIARY_SUMMARY_RECORD.FILE_YEAR]
        if file_year not in self.year_data_list: self.year_data_list[file_year] = self.YearData(input_record)

    def LatestYearData(self):
        y = None
        for year in ['2010','2009','2008']:
            if year in self.year_data_list:
                y = self.year_data_list[year]
                break
        return y

    #--------------------
    #--------------------
    def ObservationPeriodList(self):

        obs_period_list = []

        for year in ['2008','2009','2010']:
            if year in self.year_data_list:
                y = self.year_data_list[year]
                if y.max_coverage_months() > 0:
                    start_month = 12 - int(y.max_coverage_months()) + 1
                    end_month = y.max_coverage_months()
                    if start_month > 1: end_month = min(12,start_month + y.max_coverage_months())
                    start_date = '{0}-{1:02d}-01'.format(year, start_month)
                    ## get proper end date
                    weekday, numdays = calendar.monthrange(int(year), int(end_month))
                    end_date = '{0}-{1:02d}-{2:02d}'.format(year, end_month, numdays)
                    obs_period_list.append((start_date, end_date))

        return obs_period_list

        # start_year = ''
        # start_month = ''
        # end_year = ''
        # end_month = ''
        #
        # for year in ['2008','2009','2010']:
        #     if year in self.year_data_list:
        #         y = self.year_data_list[year]
        #         #- keep earliest year with coverage for start
        #         if start_year == '' and y.max_coverage_months() > 0:
        #             start_year = year
        #             start_month = 12 - int(y.max_coverage_months()) + 1
        #
        #         #- keep last year with coverage for end
        #         if y.max_coverage_months() > 0:
        #             end_year = year
        #             end_month = y.max_coverage_months()
        #             if start_month > 1: end_month = min(12,start_month + y.max_coverage_months())
        #
        # from_date = ''
        # to_date = ''
        #
        # if start_year != '' and start_month != '':
        #     from_date = '{0}-{1}-01'.format(start_year, start_month)
        #
        # ## get proper end date
        # if end_year != '' and end_month != '':
        #     weekday, numdays = calendar.monthrange(int(end_year), int(end_month))
        #     to_date = '{0}-{1}-{2}'.format(end_year, end_month, numdays )
        #
        # return from_date, to_date

    #--------------------
    # assumes file is in synpuf-id
    #--------------------
    def LoadClaimData(self, file_control):

        data_file_list = [
                    (self._carrier_records,       SYNPUF_FILE_TOKENS.CARRIER),
                    (self._inpatient_records,     SYNPUF_FILE_TOKENS.INPATIENT),
                    (self._outpatient_records,    SYNPUF_FILE_TOKENS.OUTPATIENT),
                    (self._prescription_records,  SYNPUF_FILE_TOKENS.PRESCRIPTION)
                    ]

        for record_list, file_token in data_file_list:
            data_file = file_control.get_Descriptor(file_token)
            data_file.get_patient_records(self.DESYNPUF_ID, record_list)
            # print file_token, ' recs=', len(record_list)
            data_file.increment_recs_read(len(record_list))

        #------
        def sort_by_date(record_list, record_list_date_order, fld_index):
            dates = []
            ## ugh -- need to learn the pythonic ways to do things !! sort list w/ dupes?
            # print 'input---'
            for ix,rec in enumerate(record_list):
                date_ix =  rec[fld_index] + '_' + str(ix)
                dates.append(date_ix)
                # print '\t',date_ix

            # print 'sorted---'
            for date_ix in sorted(dates):
                ix = date_ix[:9]
                record_list_date_order.append(ix)
                # print '\t',date_ix

        sort_by_date(self._carrier_records, self._carrier_records_date_order_list, CARRIER_CLAIMS_RECORD.CLM_FROM_DT)
        sort_by_date(self._inpatient_records, self._inpatient_records_date_order_list, INPATIENT_CLAIMS_RECORD.CLM_FROM_DT)
        sort_by_date(self._outpatient_records, self._outpatient_records_date_order_list, OUTPATIENT_CLAIMS_RECORD.CLM_FROM_DT)
        sort_by_date(self._prescription_records, self._prescription_records_date_order_list, PRESCRIPTION_DRUG_RECORD.SRVC_DT)








