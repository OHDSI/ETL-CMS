#   -----------------------------------
# Various OMOP Constants
# -----------------------------------
class OMOP_CONSTANTS(object):
    DEFAULT_CONCEPT_VALID_END_DATE      = "2099-12-31"

    GENDER_MALE                         = "8507"
    GENDER_FEMALE                       = "8532"

    RACE_BLACK                          = "8516"
    RACE_OTHER                          = "0" # 8522 deprecated
    RACE_WHITE                          = "8527"
    RACE_NON_WHITE                      = "0" # 9178 deprecated

    ETHNICITY_HISPANIC                  = "38003563"
    ETHNICITY_NON_HISPANIC              = "38003564"

    DRUG_TYPE_PRESCRIPTION              = "38000175"
    CURRENCY_US_DOLLAR                  = "44818668"

    # we'll add to these for the subsequent positions
    INPAT_PROCEDURE_1ST_POSITION        = 38000251
    INPAT_CONDITION_1ST_POSITION        = 38000200

    OUTPAT_PROCEDURE_1ST_POSITION       = 38000269
    OUTPAT_CONDITION_1ST_POSITION       = 38000230
    CC_PROCEDURE_1ST_POSITION           = 45756900
    CC_CONDITION_1ST_POSITION           = 45756835
    INPAT_VISIT_1ST_POSITION            = 44818517
    OUTPAT_VISIT_1ST_POSITION           = 44818517
    CARRIER_CLAIMS_VISIT_1ST_POSITION   = 44818517
    INPAT_VISIT_CONCEPT_ID              = 9201
    OUTPAT_VISIT_CONCEPT_ID             = 9202
    CARRIER_CLAIMS_VISIT_CONCEPT_ID     = 0

    OBS_PERIOD_ENROLLED_INSURANCE       = "44814722"
    OBS_PERIOD_ENROLLED_STUDY           = "44814723"
    OBS_PERIOD_HEALTH_CARE_ENCOUNTERS   = "44814724"
    OBS_PERIOD_ALGORITHM                = "44814725"

    DEATH_TYPE_PAYER_ENR_STATUS         = "38003565"
    DEATH_TYPE_CONDITION                = "38003567"

    DEVICE_INFERRED_PROCEDURE_CLAIM     = "44818705"
    MEASUREMENT_DERIVED_VALUE           = "45754907"
    OBSERVATION_CHIEF_COMPLAINT         = "38000282"

    ICD_9_VOCAB_ID                      = 'ICD9'
    ICD_9_DIAGNOSIS_VOCAB_ID            = 'ICD9CM'
    ICD_9_PROCEDURES_VOCAB_ID           = 'ICD9Proc'
    HCPCS_VOCABULARY_ID                 = 'HCPCS'
    CPT4_VOCABULARY_ID                  = 'CPT4'
    NDC_VOCABULARY_ID                   = 'NDC'

    INPATIENT_PLACE_OF_SERVICE          = 8717      # Inpatient Hospital
    OUTPATIENT_PLACE_OF_SERVICE         = 8756      # Outpatient Hospital
    CARRIER_CLAIMS_PLACE_OF_SERVICE     = 8940      # Office

    INPATIENT_PLACE_OF_SERVICE_SOURCE         = "Inpatient Facility"
    OUTPATIENT_PLACE_OF_SERVICE_SOURCE        = "Outpatient Facility"
    CARRIER_CLAIMS_PLACE_OF_SERVICE_SOURCE    = " "
    GENDER_SOURCE_CONCEPT_ID                  = 0    #gender_source_concept_id

# --------------------
# Record layout for the CONCEPT_RELATIONSHIP.csv file
# It is used to map: OMOP (NDC) -> OMOP (RXNORM)
# It is used to map: OMOP (ICD9) -> OMOP (SNOMED)
# It is used to map: OMOP (HCPCS) -> OMOP (CPT4)
# --------------------
class OMOP_CONCEPT_RELATIONSHIP_RECORD(object):
    CONCEPT_ID_1      = 0
    CONCEPT_ID_2      = 1
    RELATIONSHIP_ID   = 2
    VALID_START_DATE  = 3
    VALID_END_DATE    = 4
    INVALID_REASON    = 5
    fieldCount = INVALID_REASON + 1

# --------------------
# Record layout for ICD9 mapping file
# --------------------
class PHVS_ICD9_MAP_RECORD(object):
    CONCEPT_CODE                = 0
    CONCEPT_NAME                = 1
    CONCEPT_NAME2               = 2
    PREFERRED_CONCEPT_NAME      = 3
    PREFERRED_ALTERNATE_CODE    = 4
    # other fields not used

# --------------------
# Record layout for OMOP CONCEPT v5
# --------------------
class OMOP_CONCEPT_RECORD(object):
    CONCEPT_ID          = 0
    CONCEPT_NAME        = 1
    DOMAIN_ID           = 2
    VOCABULARY_ID       = 3
    CONCEPT_CLASS_ID    = 4
    STANDARD_CONCEPT    = 5
    CONCEPT_CODE        = 6
    VALID_START_DATE    = 7
    VALID_END_DATE      = 8
    INVALID_REASON      = 9

    fieldCount = INVALID_REASON + 1


# --------------------
# Record layout for OMOP ETL Mapping Xref
# --------------------
class OMOP_MAPPING_RECORD(object):
    SOURCE_CONCEPT_CODE       = 0
    SOURCE_CONCEPT_ID         = 1
    SOURCE_VOCABULARY_ID      = 3
    SOURCE_CONCEPT_NAME       = 4
    TARGET_CONCEPT_ID         = 5
    TARGET_DOMAIN_ID          = 6
    TARGET_VOCABULARY_ID      = 7
    TARGET_CONCEPT_NAME       = 8

    fieldCount = TARGET_CONCEPT_NAME + 1


# --------------------
#  SynPuf file tokens
# --------------------
class SYNPUF_FILE_TOKENS(object):
    BENEFICARY       = 'beneficiary'
    CARRIER          = 'carrier'
    INPATIENT        = 'inpatient'
    OUTPATIENT       = 'outpatient'
    PRESCRIPTION     = 'prescription'

# --------------------
# Record layouts for CMS SynPuf files
# --------------------
#  Beneficiary
# --------------------
class BENEFICIARY_SUMMARY_RECORD(object):
    FILE_YEAR                   = 0
    DESYNPUF_ID                 = 1
    BENE_BIRTH_DT               = 2
    BENE_DEATH_DT               = 3
    BENE_SEX_IDENT_CD           = 4
    BENE_RACE_CD                = 5
    BENE_ESRD_IND               = 6
    SP_STATE_CODE               = 7
    BENE_COUNTY_CD              = 8
    BENE_HI_CVRAGE_TOT_MONS     = 9
    BENE_SMI_CVRAGE_TOT_MONS    = 10
    BENE_HMO_CVRAGE_TOT_MONS    = 11
    PLAN_CVRG_MOS_NUM           = 12
    SP_ALZHDMTA                 = 13
    SP_CHF                      = 14
    SP_CHRNKIDN                 = 15
    SP_CNCR                     = 16
    SP_COPD                     = 17
    SP_DEPRESSN                 = 18
    SP_DIABETES                 = 19
    SP_ISCHMCHT                 = 20
    SP_OSTEOPRS                 = 21
    SP_RA_OA                    = 22
    SP_STRKETIA                 = 23
    MEDREIMB_IP                 = 24
    BENRES_IP                   = 25
    PPPYMT_IP                   = 26
    MEDREIMB_OP                 = 27
    BENRES_OP                   = 28
    PPPYMT_OP                   = 29
    MEDREIMB_CAR                = 30
    BENRES_CAR                  = 31
    PPPYMT_CAR                  = 32

    fieldCount = PPPYMT_CAR + 1

# -----------------------------------
# Inpatient claim
# -----------------------------------
class INPATIENT_CLAIMS_RECORD(object):
    DESYNPUF_ID                     = 0
    CLM_ID                          = 1
    SEGMENT                         = 2
    CLM_FROM_DT                     = 3
    CLM_THRU_DT                     = 4
    PRVDR_NUM                       = 5
    CLM_PMT_AMT                     = 6
    NCH_PRMRY_PYR_CLM_PD_AMT        = 7
    AT_PHYSN_NPI                    = 8
    OP_PHYSN_NPI                    = 9
    OT_PHYSN_NPI                    = 10
    CLM_ADMSN_DT                    = 11
    ADMTNG_ICD9_DGNS_CD             = 12
    CLM_PASS_THRU_PER_DIEM_AMT      = 13
    NCH_BENE_IP_DDCTBL_AMT          = 14
    NCH_BENE_PTA_COINSRNC_LBLTY_AM  = 15
    NCH_BENE_BLOOD_DDCTBL_LBLTY_AM  = 16
    CLM_UTLZTN_DAY_CNT              = 17
    NCH_BENE_DSCHRG_DT              = 18
    CLM_DRG_CD                      = 19
    ICD9_DGNS_CD_1                  = 20
    ICD9_DGNS_CD_2                  = 21
    ICD9_DGNS_CD_3                  = 22
    ICD9_DGNS_CD_4                  = 23
    ICD9_DGNS_CD_5                  = 24
    ICD9_DGNS_CD_6                  = 25
    ICD9_DGNS_CD_7                  = 26
    ICD9_DGNS_CD_8                  = 27
    ICD9_DGNS_CD_9                  = 28
    ICD9_DGNS_CD_10                 = 29
    ICD9_PRCDR_CD_1                 = 30
    ICD9_PRCDR_CD_2                 = 31
    ICD9_PRCDR_CD_3                 = 32
    ICD9_PRCDR_CD_4                 = 33
    ICD9_PRCDR_CD_5                 = 34
    ICD9_PRCDR_CD_6                 = 35
    HCPCS_CD_1                      = 36
    HCPCS_CD_2                      = 37
    HCPCS_CD_3                      = 38
    HCPCS_CD_4                      = 39
    HCPCS_CD_5                      = 40
    HCPCS_CD_6                      = 41
    HCPCS_CD_7                      = 42
    HCPCS_CD_8                      = 43
    HCPCS_CD_9                      = 44
    HCPCS_CD_10                     = 45
    HCPCS_CD_11                     = 46
    HCPCS_CD_12                     = 47
    HCPCS_CD_13                     = 48
    HCPCS_CD_14                     = 49
    HCPCS_CD_15                     = 50
    HCPCS_CD_16                     = 51
    HCPCS_CD_17                     = 52
    HCPCS_CD_18                     = 53
    HCPCS_CD_19                     = 54
    HCPCS_CD_20                     = 55
    HCPCS_CD_21                     = 56
    HCPCS_CD_22                     = 57
    HCPCS_CD_23                     = 58
    HCPCS_CD_24                     = 59
    HCPCS_CD_25                     = 60
    HCPCS_CD_26                     = 61
    HCPCS_CD_27                     = 62
    HCPCS_CD_28                     = 63
    HCPCS_CD_29                     = 64
    HCPCS_CD_30                     = 65
    HCPCS_CD_31                     = 66
    HCPCS_CD_32                     = 67
    HCPCS_CD_33                     = 68
    HCPCS_CD_34                     = 69
    HCPCS_CD_35                     = 70
    HCPCS_CD_36                     = 71
    HCPCS_CD_37                     = 72
    HCPCS_CD_38                     = 73
    HCPCS_CD_39                     = 74
    HCPCS_CD_40                     = 75
    HCPCS_CD_41                     = 76
    HCPCS_CD_42                     = 77
    HCPCS_CD_43                     = 78
    HCPCS_CD_44                     = 79
    HCPCS_CD_45                     = 80

    fieldCount = HCPCS_CD_45 + 1

# -----------------------------------
# Outpatient claim
# -----------------------------------
class OUTPATIENT_CLAIMS_RECORD(object):
    DESYNPUF_ID                     = 0
    CLM_ID                          = 1
    SEGMENT                         = 2
    CLM_FROM_DT                     = 3
    CLM_THRU_DT                     = 4
    PRVDR_NUM                       = 5
    CLM_PMT_AMT                     = 6
    NCH_PRMRY_PYR_CLM_PD_AMT        = 7
    AT_PHYSN_NPI                    = 8
    OP_PHYSN_NPI                    = 9
    OT_PHYSN_NPI                    = 10
    NCH_BENE_BLOOD_DDCTBL_LBLTY_AM  = 11
    ICD9_DGNS_CD_1                  = 12
    ICD9_DGNS_CD_2                  = 13
    ICD9_DGNS_CD_3                  = 14
    ICD9_DGNS_CD_4                  = 15
    ICD9_DGNS_CD_5                  = 16
    ICD9_DGNS_CD_6                  = 17
    ICD9_DGNS_CD_7                  = 18
    ICD9_DGNS_CD_8                  = 19
    ICD9_DGNS_CD_9                  = 20
    ICD9_DGNS_CD_10                 = 21
    ICD9_PRCDR_CD_1                 = 22
    ICD9_PRCDR_CD_2                 = 23
    ICD9_PRCDR_CD_3                 = 24
    ICD9_PRCDR_CD_4                 = 25
    ICD9_PRCDR_CD_5                 = 26
    ICD9_PRCDR_CD_6                 = 27
    NCH_BENE_PTB_DDCTBL_AMT         = 28
    NCH_BENE_PTB_COINSRNC_AMT       = 29
    ADMTNG_ICD9_DGNS_CD             = 30
    HCPCS_CD_1                      = 31
    HCPCS_CD_2                      = 32
    HCPCS_CD_3                      = 33
    HCPCS_CD_4                      = 34
    HCPCS_CD_5                      = 35
    HCPCS_CD_6                      = 36
    HCPCS_CD_7                      = 37
    HCPCS_CD_8                      = 38
    HCPCS_CD_9                      = 39
    HCPCS_CD_10                     = 40
    HCPCS_CD_11                     = 41
    HCPCS_CD_12                     = 42
    HCPCS_CD_13                     = 43
    HCPCS_CD_14                     = 44
    HCPCS_CD_15                     = 45
    HCPCS_CD_16                     = 46
    HCPCS_CD_17                     = 47
    HCPCS_CD_18                     = 48
    HCPCS_CD_19                     = 49
    HCPCS_CD_20                     = 50
    HCPCS_CD_21                     = 51
    HCPCS_CD_22                     = 52
    HCPCS_CD_23                     = 53
    HCPCS_CD_24                     = 54
    HCPCS_CD_25                     = 55
    HCPCS_CD_26                     = 56
    HCPCS_CD_27                     = 57
    HCPCS_CD_28                     = 58
    HCPCS_CD_29                     = 59
    HCPCS_CD_30                     = 60
    HCPCS_CD_31                     = 61
    HCPCS_CD_32                     = 62
    HCPCS_CD_33                     = 63
    HCPCS_CD_34                     = 64
    HCPCS_CD_35                     = 65
    HCPCS_CD_36                     = 66
    HCPCS_CD_37                     = 67
    HCPCS_CD_38                     = 68
    HCPCS_CD_39                     = 69
    HCPCS_CD_40                     = 70
    HCPCS_CD_41                     = 71
    HCPCS_CD_42                     = 72
    HCPCS_CD_43                     = 73
    HCPCS_CD_44                     = 74
    HCPCS_CD_45                     = 75

    fieldCount = HCPCS_CD_45 + 1

# -----------------------------------
# Carrier claim
# -----------------------------------
class CARRIER_CLAIMS_RECORD(object):
    DESYNPUF_ID                      = 0
    CLM_ID                          = 1
    CLM_FROM_DT                     = 2
    CLM_THRU_DT                     = 3
    ICD9_DGNS_CD_1                  = 4
    ICD9_DGNS_CD_2                  = 5
    ICD9_DGNS_CD_3                  = 6
    ICD9_DGNS_CD_4                  = 7
    ICD9_DGNS_CD_5                  = 8
    ICD9_DGNS_CD_6                  = 9
    ICD9_DGNS_CD_7                  = 10
    ICD9_DGNS_CD_8                  = 11
    PRF_PHYSN_NPI_1                 = 12
    PRF_PHYSN_NPI_2                 = 13
    PRF_PHYSN_NPI_3                 = 14
    PRF_PHYSN_NPI_4                 = 15
    PRF_PHYSN_NPI_5                 = 16
    PRF_PHYSN_NPI_6                 = 17
    PRF_PHYSN_NPI_7                 = 18
    PRF_PHYSN_NPI_8                 = 19
    PRF_PHYSN_NPI_9                 = 20
    PRF_PHYSN_NPI_10                = 21
    PRF_PHYSN_NPI_11                = 22
    PRF_PHYSN_NPI_12                = 23
    PRF_PHYSN_NPI_13                = 24
    TAX_NUM_1                       = 25
    TAX_NUM_2                       = 26
    TAX_NUM_3                       = 27
    TAX_NUM_4                       = 28
    TAX_NUM_5                       = 29
    TAX_NUM_6                       = 30
    TAX_NUM_7                       = 31
    TAX_NUM_8                       = 32
    TAX_NUM_9                       = 33
    TAX_NUM_10                      = 34
    TAX_NUM_11                      = 35
    TAX_NUM_12                      = 36
    TAX_NUM_13                      = 37
    HCPCS_CD_1                      = 38
    HCPCS_CD_2                      = 39
    HCPCS_CD_3                      = 40
    HCPCS_CD_4                      = 41
    HCPCS_CD_5                      = 42
    HCPCS_CD_6                      = 43
    HCPCS_CD_7                      = 44
    HCPCS_CD_8                      = 45
    HCPCS_CD_9                      = 46
    HCPCS_CD_10                     = 47
    HCPCS_CD_11                     = 48
    HCPCS_CD_12                     = 49
    HCPCS_CD_13                     = 50
    LINE_NCH_PMT_AMT_1              = 51
    LINE_NCH_PMT_AMT_2              = 52
    LINE_NCH_PMT_AMT_3              = 53
    LINE_NCH_PMT_AMT_4              = 54
    LINE_NCH_PMT_AMT_5              = 55
    LINE_NCH_PMT_AMT_6              = 56
    LINE_NCH_PMT_AMT_7              = 57
    LINE_NCH_PMT_AMT_8              = 58
    LINE_NCH_PMT_AMT_9              = 59
    LINE_NCH_PMT_AMT_10             = 60
    LINE_NCH_PMT_AMT_11             = 61
    LINE_NCH_PMT_AMT_12             = 62
    LINE_NCH_PMT_AMT_13             = 63
    LINE_BENE_PTB_DDCTBL_AMT_1      = 64
    LINE_BENE_PTB_DDCTBL_AMT_2      = 65
    LINE_BENE_PTB_DDCTBL_AMT_3      = 66
    LINE_BENE_PTB_DDCTBL_AMT_4      = 67
    LINE_BENE_PTB_DDCTBL_AMT_5      = 68
    LINE_BENE_PTB_DDCTBL_AMT_6      = 69
    LINE_BENE_PTB_DDCTBL_AMT_7      = 70
    LINE_BENE_PTB_DDCTBL_AMT_8      = 71
    LINE_BENE_PTB_DDCTBL_AMT_9      = 72
    LINE_BENE_PTB_DDCTBL_AMT_10     = 73
    LINE_BENE_PTB_DDCTBL_AMT_11     = 74
    LINE_BENE_PTB_DDCTBL_AMT_12     = 75
    LINE_BENE_PTB_DDCTBL_AMT_13     = 76
    LINE_BENE_PRMRY_PYR_PD_AMT_1    = 77
    LINE_BENE_PRMRY_PYR_PD_AMT_2    = 78
    LINE_BENE_PRMRY_PYR_PD_AMT_3    = 79
    LINE_BENE_PRMRY_PYR_PD_AMT_4    = 80
    LINE_BENE_PRMRY_PYR_PD_AMT_5    = 81
    LINE_BENE_PRMRY_PYR_PD_AMT_6    = 82
    LINE_BENE_PRMRY_PYR_PD_AMT_7    = 83
    LINE_BENE_PRMRY_PYR_PD_AMT_8    = 84
    LINE_BENE_PRMRY_PYR_PD_AMT_9    = 85
    LINE_BENE_PRMRY_PYR_PD_AMT_10   = 86
    LINE_BENE_PRMRY_PYR_PD_AMT_11   = 87
    LINE_BENE_PRMRY_PYR_PD_AMT_12   = 88
    LINE_BENE_PRMRY_PYR_PD_AMT_13   = 89
    LINE_COINSRNC_AMT_1             = 90
    LINE_COINSRNC_AMT_2             = 91
    LINE_COINSRNC_AMT_3             = 92
    LINE_COINSRNC_AMT_4             = 93
    LINE_COINSRNC_AMT_5             = 94
    LINE_COINSRNC_AMT_6             = 95
    LINE_COINSRNC_AMT_7             = 96
    LINE_COINSRNC_AMT_8             = 97
    LINE_COINSRNC_AMT_9             = 98
    LINE_COINSRNC_AMT_10            = 99
    LINE_COINSRNC_AMT_11            = 100
    LINE_COINSRNC_AMT_12            = 101
    LINE_COINSRNC_AMT_13            = 102
    LINE_ALOWD_CHRG_AMT_1           = 103
    LINE_ALOWD_CHRG_AMT_2           = 104
    LINE_ALOWD_CHRG_AMT_3           = 105
    LINE_ALOWD_CHRG_AMT_4           = 106
    LINE_ALOWD_CHRG_AMT_5           = 107
    LINE_ALOWD_CHRG_AMT_6           = 108
    LINE_ALOWD_CHRG_AMT_7           = 109
    LINE_ALOWD_CHRG_AMT_8           = 110
    LINE_ALOWD_CHRG_AMT_9           = 111
    LINE_ALOWD_CHRG_AMT_10          = 112
    LINE_ALOWD_CHRG_AMT_11          = 113
    LINE_ALOWD_CHRG_AMT_12          = 114
    LINE_ALOWD_CHRG_AMT_13          = 115
    LINE_PRCSG_IND_CD_1             = 116
    LINE_PRCSG_IND_CD_2             = 117
    LINE_PRCSG_IND_CD_3             = 118
    LINE_PRCSG_IND_CD_4             = 119
    LINE_PRCSG_IND_CD_5             = 120
    LINE_PRCSG_IND_CD_6             = 121
    LINE_PRCSG_IND_CD_7             = 122
    LINE_PRCSG_IND_CD_8             = 123
    LINE_PRCSG_IND_CD_9             = 124
    LINE_PRCSG_IND_CD_10            = 125
    LINE_PRCSG_IND_CD_11            = 126
    LINE_PRCSG_IND_CD_12            = 127
    LINE_PRCSG_IND_CD_13            = 128
    LINE_ICD9_DGNS_CD_1             = 129
    LINE_ICD9_DGNS_CD_2             = 130
    LINE_ICD9_DGNS_CD_3             = 131
    LINE_ICD9_DGNS_CD_4             = 132
    LINE_ICD9_DGNS_CD_5             = 133
    LINE_ICD9_DGNS_CD_6             = 134
    LINE_ICD9_DGNS_CD_7             = 135
    LINE_ICD9_DGNS_CD_8             = 136
    LINE_ICD9_DGNS_CD_9             = 137
    LINE_ICD9_DGNS_CD_10            = 138
    LINE_ICD9_DGNS_CD_11            = 139
    LINE_ICD9_DGNS_CD_12            = 140
    LINE_ICD9_DGNS_CD_13            = 141

    fieldCount = LINE_ICD9_DGNS_CD_13 + 1

# -----------------------------------
# Prescription drug
# -----------------------------------
class PRESCRIPTION_DRUG_RECORD(object):
    DESYNPUF_ID                     = 0
    PDE_ID                          = 1
    SRVC_DT                         = 2
    PROD_SRVC_ID                    = 3
    QTY_DSPNSD_NUM                  = 4
    DAYS_SUPLY_NUM                  = 5
    PTNT_PAY_AMT                    = 6
    TOT_RX_CST_AMT                  = 7

    fieldCount = TOT_RX_CST_AMT + 1
