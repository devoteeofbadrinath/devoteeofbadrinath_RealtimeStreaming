CREATE SCHEMA IF NOT EXISTS BRDJ_USECASE_REFERENCE_FILES;

CREATE TABLE IF NOT EXISTS BRDJ_USECASE_REFERENCE_FILES.ACNT (
    ACNT_ID_NUM_KEY varchar(255),
    ACNT_ID_NUM varchar(255),
    ACNT_ID_TYP_CD varchar(255),
    ACNT_PRTY_REL varchar(255),
    ACNT_NUM varchar(255),
    PARNT_NSC_NUM varchar(255),
    ACNT_OPN_DT DATE,
    ACNT_NAME varchar(255),
    ACNT_STS_CD varchar(255),
    ACNT_CLSED_IND varchar(255),
    JURIS_CD varchar(255),
    SRC_SYS_ACNT_TYP varchar(255),
    SRC_SYS_ACNT_SUB_TYP varchar(255),
    ACNT_CCY_REF varchar(255),
    LDGR_BAL_EOD DECIMAL,
    ACNT_MRKT_CLAS_CD varchar(255),
    CUST_CR_CNTRL_STRTGY_CD varchar(255),
    BUS_CTR_CNTRLD_IND varchar(255),
    UNQ_ACNT_GRP_NUM varchar(255),
    ACNT_SUB_OFC_CD varchar(255),
    DR_INT_RATE_TYP varchar(255),
    DR_INT_RATE DECIMAL,
    ACNT_DATA_SRC_CD varchar(255),
    BATCH_PROCESS_NAME varchar(255),
    START_DATE TIMESTAMP,
    LAST_UPDT_DATE TIMESTAMP,
    RECORD_DELETED_FLAG TINYINT,
    ACNT_ROLE_UPDT_SEQ varchar(255),
    ACNT_ROLE_UPDT_DTTM TIMESTAMP,
    SHDW_BAL_AMT DECIMAL,
    SHDW_BAL_DTTM TIMESTAMP,
    SHDW_BAL_SEQ varchar(255),
    LDGR_BAL_AMT DECIMAL,
    LDGR_BAL_DTTM TIMESTAMP,
    LDGR_BAL_SEQ varchar(255)
);
