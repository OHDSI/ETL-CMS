from constants import PRESCRIPTION_DRUG_RECORD, INPATIENT_CLAIMS_RECORD, OUTPATIENT_CLAIMS_RECORD, CARRIER_CLAIMS_RECORD

#-=================================
#-=================================
class PrescriptionDrug(object):
    def __init__(self, record):
        # self.record = record
        # self.fields = record.split(',')
        # it's already split
        self.fields = record

    @property
    def DESYNPUF_ID(self):
        return self.fields[PRESCRIPTION_DRUG_RECORD.DESYNPUF_ID]

    @property
    def PDE_ID(self):
        return self.fields[PRESCRIPTION_DRUG_RECORD.PDE_ID]

    @property
    def SRVC_DT(self):
        return self.fields[PRESCRIPTION_DRUG_RECORD.SRVC_DT]

    @property
    def PROD_SRVC_ID(self):
        return self.fields[PRESCRIPTION_DRUG_RECORD.PROD_SRVC_ID]

    @property
    def QTY_DSPNSD_NUM(self):
        return self.fields[PRESCRIPTION_DRUG_RECORD.QTY_DSPNSD_NUM]

    @property
    def DAYS_SUPLY_NUM(self):
        return self.fields[PRESCRIPTION_DRUG_RECORD.DAYS_SUPLY_NUM]

    @property
    def PTNT_PAY_AMT(self):
        return self.fields[PRESCRIPTION_DRUG_RECORD.PTNT_PAY_AMT]

    @property
    def TOT_RX_CST_AMT(self):
        return self.fields[PRESCRIPTION_DRUG_RECORD.TOT_RX_CST_AMT]

#-=================================
#-=================================
class InpatientClaim(object):
    def __init__(self, record):
        self.fields = record

    @property
    def DESYNPUF_ID(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.DESYNPUF_ID]

    @property
    def CLM_ID(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.CLM_ID]

    @property
    def SEGMENT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.SEGMENT]

    @property
    def CLM_FROM_DT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.CLM_FROM_DT]

    @property
    def CLM_THRU_DT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.CLM_THRU_DT]

    @property
    def PRVDR_NUM(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.PRVDR_NUM]

    @property
    def CLM_PMT_AMT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.CLM_PMT_AMT]

    @property
    def NCH_PRMRY_PYR_CLM_PD_AMT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.NCH_PRMRY_PYR_CLM_PD_AMT]

    @property
    def AT_PHYSN_NPI(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.AT_PHYSN_NPI]

    @property
    def OP_PHYSN_NPI(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.OP_PHYSN_NPI]

    @property
    def OT_PHYSN_NPI(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.OT_PHYSN_NPI]

    @property
    def CLM_ADMSN_DT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.CLM_ADMSN_DT]

    @property
    def ADMTNG_ICD9_DGNS_CD(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.ADMTNG_ICD9_DGNS_CD]

    @property
    def CLM_ADMSN_DT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.CLM_ADMSN_DT]

    @property
    def CLM_PASS_THRU_PER_DIEM_AMT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.CLM_PASS_THRU_PER_DIEM_AMT]

    @property
    def NCH_BENE_IP_DDCTBL_AMT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.NCH_BENE_IP_DDCTBL_AMT]

    @property
    def NCH_BENE_PTA_COINSRNC_LBLTY_AM(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.NCH_BENE_PTA_COINSRNC_LBLTY_AM]

    @property
    def NCH_BENE_BLOOD_DDCTBL_LBLTY_AM(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.NCH_BENE_BLOOD_DDCTBL_LBLTY_AM]

    @property
    def CLM_UTLZTN_DAY_CNT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.CLM_UTLZTN_DAY_CNT]

    @property
    def NCH_BENE_DSCHRG_DT(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.NCH_BENE_DSCHRG_DT]

    @property
    def CLM_DRG_CD(self):
        return self.fields[INPATIENT_CLAIMS_RECORD.CLM_DRG_CD]

    @property
    def ICD9_DGNS_CD_list(self):
        codes = []
        for ix in range(INPATIENT_CLAIMS_RECORD.ICD9_DGNS_CD_1, INPATIENT_CLAIMS_RECORD.ICD9_DGNS_CD_10 + 1):
            if len(self.fields[ix]) > 0: codes.append(self.fields[ix])
        return codes

    @property
    def ICD9_PRCDR_CD_list(self):
        codes = []
        for ix in range(INPATIENT_CLAIMS_RECORD.ICD9_PRCDR_CD_1, INPATIENT_CLAIMS_RECORD.ICD9_PRCDR_CD_6 + 1):
            if len(self.fields[ix]) > 0: codes.append(self.fields[ix])
        return codes

    @property
    def HCPCS_CD_list(self):
        codes = []
        for ix in range(INPATIENT_CLAIMS_RECORD.HCPCS_CD_1, INPATIENT_CLAIMS_RECORD.HCPCS_CD_45 + 1):
            if len(self.fields[ix]) > 0: codes.append(self.fields[ix])
        return codes

#-=================================
#-=================================
class OutpatientClaim(object):
    def __init__(self, record):
        self.fields = record

    @property
    def DESYNPUF_ID(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.DESYNPUF_ID]

    @property
    def CLM_ID(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.CLM_ID]

    @property
    def SEGMENT(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.SEGMENT]

    @property
    def CLM_FROM_DT(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.CLM_FROM_DT]

    @property
    def CLM_THRU_DT(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.CLM_THRU_DT]

    @property
    def PRVDR_NUM(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.PRVDR_NUM]

    @property
    def CLM_PMT_AMT(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.CLM_PMT_AMT]

    @property
    def NCH_PRMRY_PYR_CLM_PD_AMT(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.NCH_PRMRY_PYR_CLM_PD_AMT]

    @property
    def AT_PHYSN_NPI(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.AT_PHYSN_NPI]

    @property
    def OP_PHYSN_NPI(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.OP_PHYSN_NPI]

    @property
    def OT_PHYSN_NPI(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.OT_PHYSN_NPI]

    @property
    def NCH_BENE_BLOOD_DDCTBL_LBLTY_AM(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.NCH_BENE_BLOOD_DDCTBL_LBLTY_AM]

    @property
    def ICD9_DGNS_CD_list(self):
        codes = []
        for ix in range(OUTPATIENT_CLAIMS_RECORD.ICD9_DGNS_CD_1, OUTPATIENT_CLAIMS_RECORD.ICD9_DGNS_CD_10 + 1):
            if len(self.fields[ix]) > 0: codes.append(self.fields[ix])
        return codes

    @property
    def ICD9_PRCDR_CD_list(self):
        codes = []
        for ix in range(OUTPATIENT_CLAIMS_RECORD.ICD9_PRCDR_CD_1, OUTPATIENT_CLAIMS_RECORD.ICD9_PRCDR_CD_6 + 1):
            if len(self.fields[ix]) > 0: codes.append(self.fields[ix])
        return codes

    @property
    def NCH_BENE_PTB_DDCTBL_AMT(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.NCH_BENE_PTB_DDCTBL_AMT]

    @property
    def NCH_BENE_PTB_COINSRNC_AMT(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.NCH_BENE_PTB_COINSRNC_AMT]

    @property
    def ADMTNG_ICD9_DGNS_CD(self):
        return self.fields[OUTPATIENT_CLAIMS_RECORD.ADMTNG_ICD9_DGNS_CD]

    @property
    def HCPCS_CD_list(self):
        codes = []
        for ix in range(OUTPATIENT_CLAIMS_RECORD.HCPCS_CD_1, OUTPATIENT_CLAIMS_RECORD.HCPCS_CD_45 + 1):
            if len(self.fields[ix]) > 0: codes.append(self.fields[ix])
        return codes

#-=================================
#-=================================
class CarrierClaimLine(object):
    def __init__(self, PRF_PHYSN_NPI, TAX_NUM, HCPCS_CD, LINE_NCH_PMT_AMT, LINE_BENE_PTB_DDCTBL_AMT,
                LINE_BENE_PRMRY_PYR_PD_AMT, LINE_COINSRNC_AMT, LINE_ALOWD_CHRG_AMT, LINE_PRCSG_IND_CD, LINE_ICD9_DGNS_CD):
        self.PRF_PHYSN_NPI = PRF_PHYSN_NPI
        self.TAX_NUM = TAX_NUM
        self.HCPCS_CD = HCPCS_CD
        self.LINE_NCH_PMT_AMT = LINE_NCH_PMT_AMT
        self.LINE_BENE_PTB_DDCTBL_AMT = LINE_BENE_PTB_DDCTBL_AMT
        self.LINE_BENE_PRMRY_PYR_PD_AMT = LINE_BENE_PRMRY_PYR_PD_AMT
        self.LINE_COINSRNC_AMT = LINE_COINSRNC_AMT
        self.LINE_ALOWD_CHRG_AMT = LINE_ALOWD_CHRG_AMT
        self.LINE_PRCSG_IND_CD = LINE_PRCSG_IND_CD
        self.LINE_ICD9_DGNS_CD = LINE_ICD9_DGNS_CD

    def has_nonzero_amount(self):
        if  self.LINE_NCH_PMT_AMT  + \
            self.LINE_BENE_PTB_DDCTBL_AMT + \
            self.LINE_BENE_PRMRY_PYR_PD_AMT + \
            self.LINE_COINSRNC_AMT + \
            self.LINE_ALOWD_CHRG_AMT     != '':
            return True
        return False

#-=================================
#-=================================
class CarrierClaim(object):
    def __init__(self, record):
        self.fields = record

    @property
    def TAX_NUM(self):
        return self.fields[CARRIER_CLAIMS_RECORD.TAX_NUM_1]

    @property
    def DESYNPUF_ID(self):
        return self.fields[CARRIER_CLAIMS_RECORD.DESYNPUF_ID]

    @property
    def CLM_ID(self):
        return self.fields[CARRIER_CLAIMS_RECORD.CLM_ID]

    @property
    def CLM_FROM_DT(self):
        return self.fields[CARRIER_CLAIMS_RECORD.CLM_FROM_DT]

    @property
    def CLM_THRU_DT(self):
        return self.fields[CARRIER_CLAIMS_RECORD.CLM_THRU_DT]

    @property
    def ICD9_DGNS_CD_list(self):
        codes = []
        for ix in range(CARRIER_CLAIMS_RECORD.ICD9_DGNS_CD_1, CARRIER_CLAIMS_RECORD.ICD9_DGNS_CD_8 + 1):
            if len(self.fields[ix]) > 0: codes.append(self.fields[ix])
        return codes

    @property
    def HCPCS_CD_list(self):
        codes = []
        for ix in range(CARRIER_CLAIMS_RECORD.HCPCS_CD_1, CARRIER_CLAIMS_RECORD.HCPCS_CD_13 + 1):
            if len(self.fields[ix]) > 0: codes.append(self.fields[ix])
        return codes

    @property
    def LINE_ICD9_DGNS_CD_list(self):
        codes = []
        for ix in range(CARRIER_CLAIMS_RECORD.LINE_ICD9_DGNS_CD_1, CARRIER_CLAIMS_RECORD.LINE_ICD9_DGNS_CD_13 + 1):
            if len(self.fields[ix]) > 0: codes.append(self.fields[ix])
        return codes

    @property
    def CarrierClaimLine_list(self):
        claim_lines = []
        for ix in range(0, 13):
            if self.fields[CARRIER_CLAIMS_RECORD.PRF_PHYSN_NPI_1 + ix] != '':
                line = CarrierClaimLine(self.fields[CARRIER_CLAIMS_RECORD.PRF_PHYSN_NPI_1 + ix],
                                        self.fields[CARRIER_CLAIMS_RECORD.TAX_NUM_1 + ix],
                                        self.fields[CARRIER_CLAIMS_RECORD.HCPCS_CD_1 + ix],
                                        self.fields[CARRIER_CLAIMS_RECORD.LINE_NCH_PMT_AMT_1 + ix],
                                        self.fields[CARRIER_CLAIMS_RECORD.LINE_BENE_PTB_DDCTBL_AMT_1 + ix],
                                        self.fields[CARRIER_CLAIMS_RECORD.LINE_BENE_PRMRY_PYR_PD_AMT_1 + ix],
                                        self.fields[CARRIER_CLAIMS_RECORD.LINE_COINSRNC_AMT_1 + ix],
                                        self.fields[CARRIER_CLAIMS_RECORD.LINE_ALOWD_CHRG_AMT_1 + ix],
                                        self.fields[CARRIER_CLAIMS_RECORD.LINE_PRCSG_IND_CD_1 + ix],
                                        self.fields[CARRIER_CLAIMS_RECORD.LINE_ICD9_DGNS_CD_1 + ix])
                claim_lines.append(line)
        return claim_lines
