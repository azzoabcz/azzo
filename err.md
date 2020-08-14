-- error
-- Error while processing statement: FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.MoveTask. Unable to fetch table aws_mi1_avg_wd. Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient

-- * : Error while compiling statement: FAILED: RuntimeException Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient




---------------------------------------------------------------- MI1_AVG_WD
create external table aws.aws_MI1_AVG_WD
(
TM string,
STN_ID string,
MI1_AVG_WD float,
MI1_AVG_WD_QCFLG int,
MI1_AVG_WD_CROB int
)
partitioned by (YYYY string, MM string)
stored as PARQUET
LOCATION '/data/databases/aws'
;


-----------------------------------------------------------------
-- insert into aws.aws_mi1_avg_ta partition(yyyy, mm)
-- select
--     TM,
--     STN_ID,
--     MI1_AVG_TA,
--     MI1_AVG_TA_QCFLG,
--     MI1_AVG_TA_CROB,
--     concat('20', substr(tm, 1, 2)),
--     substr(tm, 4, 2)
-- from aws.aws_raw_data
-- ;

insert into aws.aws_MI1_AVG_WD partition(yyyy, mm)
select
    TM,
    STN_ID,
    MI1_AVG_WD,
    MI1_AVG_WD_QCFLG,
    MI1_AVG_WD_CROB,
    concat('20', substr(tm, 1, 2)),
    substr(tm, 4, 2)
from aws.aws_raw_data
;

insert into aws.aws_MI1_AVG_WS partition(yyyy, mm)
select
    TM,
    STN_ID,
    MI1_AVG_WS,
    MI1_AVG_WS_QCFLG,
    MI1_AVG_WS_CROB,
    concat('20', substr(tm, 1, 2)),
    substr(tm, 4, 2)
from aws.aws_raw_data
;

insert into aws.aws_MI1_GUST_WD partition(yyyy, mm)
select
    TM,
    STN_ID,
    MI1_GUST_WD,
    MI1_GUST_WD_QCFLG,
    MI1_GUST_WD_CROB,
    concat('20', substr(tm, 1, 2)),
    substr(tm, 4, 2)
from aws.aws_raw_data
;

insert into aws.aws_MI1_GUST_WS partition(yyyy, mm)
select
    TM,
    STN_ID,
    MI1_GUST_WS,
    MI1_GUST_WS_QCFLG,
    MI1_GUST_WS_CROB,
    concat('20', substr(tm, 1, 2)),
    substr(tm, 4, 2)
from aws.aws_raw_data
;

insert into aws.aws_MI1_RN partition(yyyy, mm)
select
    TM,
    STN_ID,
    MI1_RN,
    MI1_RN_QCFLG,
    MI1_RN_CROB,
    concat('20', substr(tm, 1, 2)),
    substr(tm, 4, 2)
from aws.aws_raw_data
;

insert into aws.aws_MI60_MV_ACM_RN partition(yyyy, mm)
select
    TM,
    STN_ID,
    MI60_MV_ACM_RN,
    MI60_MV_ACM_RN_QCFLG,
    MI60_MV_ACM_RN_CROB,
    concat('20', substr(tm, 1, 2)),
    substr(tm, 4, 2)
from aws.aws_raw_data
;

insert into aws.aws_HR12_MV_ACM_RN partition(yyyy, mm)
select
    TM,
    STN_ID,
    HR12_MV_ACM_RN,
    HR12_MV_ACM_RN_QCFLG,
    HR12_MV_ACM_RN_CROB,
    concat('20', substr(tm, 1, 2)),
    substr(tm, 4, 2)
from aws.aws_raw_data
;

insert into aws.aws_MI1_ICSR partition(yyyy, mm)
select
    TM,
    STN_ID,
    MI1_ICSR,
    MI1_ICSR_QCFLG,
    MI1_ICSR_CROB,
    concat('20', substr(tm, 1, 2)),
    substr(tm, 4, 2)
from aws.aws_raw_data
;

insert into aws.aws_HR1_ICSR partition(yyyy, mm)
select
    TM,
    STN_ID,
    HR1_ICSR,
    HR1_ICSR_QCFLG,
    HR1_ICSR_CROB,
    concat('20', substr(tm, 1, 2)),
    substr(tm, 4, 2)
from aws.aws_raw_data
;

insert into aws.aws_DD_ACM_ICSR partition(yyyy, mm)
select
    TM,
    STN_ID,
    DD_ACM_ICSR,
    DD_ACM_ICSR_QCFLG,
    DD_ACM_ICSR_CROB,
    concat('20', substr(tm, 1, 2)),
    substr(tm, 4, 2)
from aws.aws_raw_data
;
