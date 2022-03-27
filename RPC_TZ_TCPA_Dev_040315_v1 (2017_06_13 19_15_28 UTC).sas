libname odscacs oracle user="GMALATHU" password="Ph03n1x357" path=prdBIDW schema=ods_cacs;
libname dwprd oracle user="GMALATHU" password="Ph03n1x357" path=prdBIDW schema=dwprd;
libname cxprd oracle user="GMALATHU" password="Ph03n1x357" path=prdBIDW schema=cxprd;
options compress=yes;
option sastrace = ',,,d' sastraceloc=saslog nostsuffix;
libname barb 'E:\barb';
libname rohini 'E:\Rohini';
libname score 'd:\Daily_Score';
libname datalib 'E:\End User Computing\Data Files';
libname scoremid 'd:\Daily_score_Mid_Late';
libname final 'E:\Barb\Final Sale Files';
libname fflow 'd:\Forward Flow';
libname asset 'E:\Barb\Asset Jan 2012';
libname mike 'E:\Mike';
libname gogie 'E:\Gogie';
libname routed 'D:\Forward Flow Test\Routed to Legal';

data workstate_lookup;
    infile '\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\workstate_lookup.csv' delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2;
informat code $3.;
informat description $44.;
format code $3.;
format description $44.;

input code $
description $
;
run;

%macro TZ_RPC_Compliance(date_d,date_dt,outfile1,outfile2,outfile3,outfile4,outfile5,outfile6);

proc sql;
create table d2_att as select D2_ACCOUNT_NUMBER
,a.D2_AGENCY
,a.D2_AGENT_ID_CACS
,a.D2_ATTEMPT_OUTCOME_CODE
,b.OUTCOME_CD
,b.LONG_DESC
,a.D2_ATTEMPT_OUTCOME_CODE_RAW
,a.D2_CALL_ID
,a.D2_CALL_LIST_ID
,a.D2_CALL_TYPE
,a.D2_CALL_TYPE_RAW as campaign label='campaign'
,a.D2_CAMPAIGN_ID as list label='list'
,a.D2_CONNECT_TIME
,a.D2_CONTACT_OUTCOME_CODE
,a.D2_CONTACT_OUTCOME_CODE_RAW
,a.D2_DATETIME_CALL_END
,a.D2_DATETIME_CALL_START
,a.D2_PHONE_NUMBER
,a.D2_PHONE_TYPE
,a.D2_TIME_ZONE
,a.D2_TRANSACTION_DATE
,case when d2_contact_outcome_code in ('31','32','33','34','35','36','37','38', '39','40')
            then 1 else 0 end as result_rpc
from odscacs.d2_dialer_attempts as a
left join cxprd.d_outcome_cd as b on a.D2_ATTEMPT_OUTCOME_CODE=b.outcome_cd
where d2_transaction_date = /* intnx('dtday',datetime(),-3) */ &date_dt /* '16OCT2014:00:00:00'dt */
and substr(D2_CALL_TYPE_RAW,4,1) ne 'I'
and d2_agency in ('ARA','AR5','AR6')
;
quit;

proc sql;
create table autodial_src1 as select *
,'autodialer' as attempt_source
from d2_att
;
quit;

proc sql;
create table max_customer_stzip as select c14_acct_num
,c14_processing_date
,c14_customer_state
,c14_customer_zip_code
from odscacs.C14_CACS_P327EXT_DAILY
where c14_acct_num in (select distinct d2_account_number from d2_att)
and c14_processing_date = /* intnx('dtday',datetime(),-3) */ &date_dt /* '16OCT2014:00:00:00'dt */
;
quit;

proc sql;
create table zip_state1 as select c14_customer_zip_code as customer_zip_code label='customer_zip_code'
,c14_customer_state as customer_state label='customer_state'
from max_customer_stzip
;
quit;

proc sort data=zip_state1 nodupkey out=zip_state2;
by customer_zip_code;
run;

proc sql;
create table autodial_src2 as select a.*
,substr(a.D2_PHONE_NUMBER,1,3) as area_code
,b.c14_customer_state as acct_state
,b.c14_customer_zip_code as customer_zip_code
from autodial_src1 as a left join 
max_customer_stzip as b on a.D2_ACCOUNT_NUMBER=b.c14_acct_num
and a.d2_transaction_date=b.c14_processing_date
;
quit;

proc sql;
create table autodial_src_tz1 as select a.*
,b.areacode
,b.exchange
,b.state as area_cd_state
,case when b.adjust_cst = -300 then 'SST'
when b.adjust_cst = -240 then 'HAST'
when b.adjust_cst = -180 then 'AKST'
when b.adjust_cst = -120 then 'PST'
when b.adjust_cst = -60 then 'MST'
when b.adjust_cst = 0 then 'CST'
when b.adjust_cst = 60 then 'EST'
when b.adjust_cst = 120 then 'ATL'
else 'nil' end as Area_Code_TZ
from autodial_src2 as a
left join score.d_areacode_ex_timezone as b
on a.area_code=b.areacode
and substr(a.d2_phone_number,4,3)=b.exchange
;
quit;

proc sql;
create table autodial_src_tz2 as select a.*
,c.customer_zip_code as zip_code
,c.customer_state as zip_cd_state
,case when b.adjust_cst = -300 then 'SST'
when b.adjust_cst = -240 then 'HAST'
when b.adjust_cst = -180 then 'AKST'
when b.adjust_cst = -120 then 'PST'
when b.adjust_cst = -60 then 'MST'
when b.adjust_cst = 0 then 'CST'
when b.adjust_cst = 60 then 'EST'
when b.adjust_cst = 120 then 'ATL'
else 'nil' end as zip_cd_tz
from autodial_src_tz1 as a
left join score.d_areacode_zip_timezone as b
on a.area_code=b.areacode
and substr(a.customer_zip_code,1,5)=b.zip_code
left join zip_state2 as c
on a.customer_zip_code=c.customer_zip_code
;
quit;

proc sql;
create table autodial_src_tz3 as select *
,case 
when area_code_tz in ('SST') then input(put(hour(timepart(d2_datetime_call_start))-5, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Alaska TZ   */
when area_code_tz in ('HAST') then input(put(hour(timepart(d2_datetime_call_start))-4, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Atlantic TZ */
when area_code_tz in ('AKST') then input(put(hour(timepart(d2_datetime_call_start))-3, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when area_code_tz in ('PST') then input(put(hour(timepart(d2_datetime_call_start))-2, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when area_code_tz in ('MST') then input(put(hour(timepart(d2_datetime_call_start))-1, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Hawaii TZ  */
when area_code_tz in ('CST') then input(put(hour(timepart(d2_datetime_call_start)), z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when area_code_tz in ('EST') then input(put(hour(timepart(d2_datetime_call_start))+1, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when area_code_tz in ('ATL') then input(put(hour(timepart(d2_datetime_call_start))+2, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when area_code_tz in ('nil') and zip_cd_tz = 'SST' then input(put(hour(timepart(d2_datetime_call_start))-5, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Hawaii TZ  */
when area_code_tz in ('nil') and zip_cd_tz = 'HAST' then input(put(hour(timepart(d2_datetime_call_start))-4, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Between Hawaii and Alaska TZ   */
when area_code_tz in ('nil') and zip_cd_tz = 'AKST' then input(put(hour(timepart(d2_datetime_call_start))-3, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Alaska TZ   */
when area_code_tz in ('nil') and zip_cd_tz = 'PST' then input(put(hour(timepart(d2_datetime_call_start))-2, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Atlantic TZ */
when area_code_tz in ('nil') and zip_cd_tz = 'MST' then input(put(hour(timepart(d2_datetime_call_start))-1, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when area_code_tz in ('nil') and zip_cd_tz = 'CST' then input(put(hour(timepart(d2_datetime_call_start)), z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when area_code_tz in ('nil') and zip_cd_tz = 'EST' then input(put(hour(timepart(d2_datetime_call_start))+1, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when area_code_tz in ('nil') and zip_cd_tz = 'ATL' then input(put(hour(timepart(d2_datetime_call_start))+2, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
else timepart(d2_datetime_call_start) end as dt_strt_by_area_cd format=time8.
,case when zip_cd_tz = 'SST' then input(put(hour(timepart(d2_datetime_call_start))-5, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Hawaii TZ  */
when zip_cd_tz = 'HAST' then input(put(hour(timepart(d2_datetime_call_start))-4, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Between Hawaii and Alaska TZ   */
when zip_cd_tz = 'AKST' then input(put(hour(timepart(d2_datetime_call_start))-3, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Alaska TZ   */
when zip_cd_tz = 'PST' then input(put(hour(timepart(d2_datetime_call_start))-2, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Atlantic TZ */
when zip_cd_tz = 'MST' then input(put(hour(timepart(d2_datetime_call_start))-1, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when zip_cd_tz = 'CST' then input(put(hour(timepart(d2_datetime_call_start)), z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when zip_cd_tz = 'EST' then input(put(hour(timepart(d2_datetime_call_start))+1, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when zip_cd_tz = 'ATL' then input(put(hour(timepart(d2_datetime_call_start))+2, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when zip_cd_tz in ('nil') and area_code_tz = 'SST' then input(put(hour(timepart(d2_datetime_call_start))-5, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Alaska TZ   */
when zip_cd_tz in ('nil') and area_code_tz = 'HAST' then input(put(hour(timepart(d2_datetime_call_start))-4, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Atlantic TZ */
when zip_cd_tz in ('nil') and area_code_tz = 'AKST' then input(put(hour(timepart(d2_datetime_call_start))-3, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when zip_cd_tz in ('nil') and area_code_tz = 'PST' then input(put(hour(timepart(d2_datetime_call_start))-2, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when zip_cd_tz in ('nil') and area_code_tz = 'MST' then input(put(hour(timepart(d2_datetime_call_start))-1, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.) /* Hawaii TZ  */
when zip_cd_tz in ('nil') and area_code_tz = 'CST' then input(put(hour(timepart(d2_datetime_call_start)), z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when zip_cd_tz in ('nil') and area_code_tz = 'EST' then input(put(hour(timepart(d2_datetime_call_start))+1, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
when zip_cd_tz in ('nil') and area_code_tz = 'ATL' then input(put(hour(timepart(d2_datetime_call_start))+2, z2.)||':'||put(minute(timepart(d2_datetime_call_start)),z2.)||':'||put(second(timepart(d2_datetime_call_start)),z2.),time8.)
else timepart(d2_datetime_call_start) end as dt_strt_by_zip_cd format=time8.
from autodial_src_tz2
/* from tz_cons_eval_data5 >> used this dataset for testing records only having data for both area code and zip code. */ 
;
quit;

proc sql;
create table autodial_src_tz4 as select *
,case when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd < dt_strt_by_zip_cd then 'area_dt'
when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd > dt_strt_by_zip_cd then 'zip_dt' 
when dt_strt_by_area_cd = dt_strt_by_zip_cd then 'same' else 'nill' end as early_time_ind
,case when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd > dt_strt_by_zip_cd then 'area_dt'
when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd < dt_strt_by_zip_cd then 'zip_dt' 
when dt_strt_by_area_cd = dt_strt_by_zip_cd then 'same' else 'nill' end as late_time_ind
from autodial_src_tz3
;
quit;

proc sql;
create table autodial_src_tz5 as select *
,hour(timepart(d2_datetime_call_start)) as cst_attempt_hour
,datepart(d2_datetime_call_start) as datetime_datepart format=mmddyy8.
,case when hour(timepart(d2_datetime_call_start)) < 12 and early_time_ind = 'area_dt' then dt_strt_by_area_cd
when hour(timepart(d2_datetime_call_start)) < 12 and early_time_ind = 'zip_dt' then dt_strt_by_zip_cd
when hour(timepart(d2_datetime_call_start)) > 11 and late_time_ind = 'area_dt' then dt_strt_by_area_cd
when hour(timepart(d2_datetime_call_start)) > 11 and late_time_ind = 'zip_dt' then dt_strt_by_zip_cd 
else dt_strt_by_area_cd end as est_local_time format=time8.
from autodial_src_tz4
;
quit;

proc sql;
create table autodial_src_tz6 as select *
,case when est_local_time < input('07:59:00',time8.) then 1 else 0 end as before_8AM_misses
,case when est_local_time > input('21:01:00',time8.) then 1 else 0 end as after_9PM_misses
,case when est_local_time >= input('07:59:00',time8.)
and est_local_time <= input('21:01:00',time8.) then 1 else 0 end as tz_compliant
from autodial_src_tz5
;
quit;

proc sql;
create table autodial_src_tz7 as select *
,case when before_8AM_misses > 0 and after_9PM_misses =0 then 'Before 8AM'
when before_8AM_misses = 0 and after_9PM_misses = 0 then 'OK'
when after_9PM_misses > 0 and before_8AM_misses =0 then 'After 9PM'
else 'nil' end as dialer_bucket
from autodial_src_tz6
;
quit;

proc sort data=autodial_src_tz7;
by d2_datetime_call_start;
run;

proc sql;
create table pbx1 as select call_start_datetime as callstart_datetime format=datetime20. label='callstart_datetime'
,channel
,extension
,user_name
,phone_number
,disposition
,duration
,call_time_sk
,call_end_sk
from score.pbx_nonconsent_cell_attempts_v2
where datepart(call_start_datetime) = /* intnx('day',today(),-3) >>*/ &date_d /* '16OCT2014'd */
;
quit;

data pbx;
set pbx1;
run;

proc sort data=pbx;
by callstart_datetime;
run;

proc sql;
create table dfsmanual1 as select entry_date
,entry_time
,acct
,phone
,agent
,state
,status
,ext
,call_time_sk
from score.dfsmanual
where entry_date =  /* intnx('day',today(),-3) >>*/ &date_d /* '16OCT2014'd */
;
quit;

data dfsmanual;
set dfsmanual1;
run;

proc sort data=dfsmanual;
by phone acct;
run;

proc sql;
create table pbx_dfsmanual_join as select a.callstart_datetime as pbx_dial_dt_start label='pbx_dial_dt_start'
,a.channel as pbx_channel label='pbx_channel'
,a.extension as pbx_extension label='pbx_extension'
,c.agent_name
,a.user_name as pbx_user_name label='pbx_user_name'
,a.phone_number as pbx_phone_number label='pbx_phone_number'
,a.disposition as pbx_disposition label='pbx_disposition'
,a.duration as pbx_duration label='pbx_duration'
,a.call_time_sk as pbx_call_time_start label='pbx_call_time_start'
,a.call_end_sk as pbx_call_time_end label='pbx_call_time_end'
,b.entry_date as dfsman_entry_date label='dfsman_entry_date'
,b.entry_time as dfsman_entry_time label='dfsman_entry_time'
,b.acct as dfsman_account label='dfsman_account'
,b.phone as dfsman_phone label='dfsman_phone'
,b.agent as dfsman_agent label='dfsman_agent'
,b.state as dfsman_state label='dfsman_state'
,b.status as dfsman_status label='dfsman_status'
,case when b.status = 'RPC' then 'Right Party Contact'
when b.status = 'TPC' then 'Third Party Contact'
when b.status = 'ONC' then 'No Connect'
when b.status = 'OPC' then 'Other Party Contact'
when b.status = 'WPC' then 'Wrong Party Contact'
when b.status = 'PTP' then 'Promise To Pay'
when b.status = 'CPU' then 'CPU'
else 'nil' end as outcome_description label='outcome_description'
,b.ext as dfsman_extension label='dfsman_extension'
,b.call_time_sk as dfsman_call_time_start label='dfsman_call_time_start'
from pbx as a 
left join dfsmanual as b on a.extension=b.ext
and a.phone_number=b.phone
and datepart(a.callstart_datetime)=b.entry_date
left join score.d_aras_manual_agents as c on input(a.extension,best12.)=c.ext
;
quit;

/*
proc sql;
create table join_pbx2cnst as select a.*
,b.phone as consent_ph_num
,case when b.acct_num not in (' ') then 'Y' else 'N' end as consent_given
from pbx_dfsmanual_join as a
left join consent_mstr as b on a.dfsman_account=substr(b.acct_num,2,18)
                                    and a.pbx_phone_number=b.phone;
 quit;
 */
 
proc sql;
create table cust_stzip as select c14_acct_num
,c14_processing_date
,c14_customer_state
,c14_customer_zip_code
from odscacs.C14_CACS_P327EXT_DAILY
where c14_acct_num in (select distinct dfsman_account from pbx_dfsmanual_join)
and c14_processing_date = /* intnx('dtday',datetime(),-3) */ &date_dt /* '16OCT2014:00:00:00'dt */
;
quit;

proc sql;
create table zip_state1 as select c14_customer_zip_code as customer_zip_code label='customer_zip_code'
,c14_customer_state as customer_state label='customer_state'
from cust_stzip
;
quit;

proc sort data=zip_state1 nodupkey out=zip_state2;
by customer_zip_code;
run;

proc sql;
create table dfsman_pbx_stzip as select a.*
,b.c14_customer_state as customer_state
,b.c14_customer_zip_code as customer_zip_code
from pbx_dfsmanual_join as a left join cust_stzip as b
on a.dfsman_account=b.c14_acct_num
;
quit;

proc sql;
create table dfsman_pbx_attsrc_rpc as select *
,substr(pbx_phone_number,1,3) as area_code
,'manual' as attempt_source
,case when dfsman_status in ('RPC','PTP','CPU') then 1 else 0 end as result_rpc
from dfsman_pbx_stzip
;
quit;

proc sql;
create table dfsman_pbx_tz1 as select a.*
,b.areacode as area_cd_code
,b.exchange as area_cd_exchange
,b.state as area_cd_state
,case when b.adjust_cst = -300 then 'SST'
when b.adjust_cst = -240 then 'HAST'
when b.adjust_cst = -180 then 'AKST'
when b.adjust_cst = -120 then 'PST'
when b.adjust_cst = -60 then 'MST'
when b.adjust_cst = 0 then 'CST'
when b.adjust_cst = 60 then 'EST'
when b.adjust_cst = 120 then 'ATL'
else 'nil' end as area_code_TZ
from dfsman_pbx_attsrc_rpc as a
/*left join mike.areacode_timezone as b*/
left join score.d_areacode_ex_timezone as b
on a.area_code=b.areacode
and substr(a.pbx_phone_number,4,3)=b.exchange
;
quit;

proc sql;
create table dfsman_pbx_tz2 as select a.*
,b.zip_code as zip_cd_zip_code
,c.customer_zip_code as zip_code
,c.customer_state as zip_cd_state
,case when b.adjust_cst = -300 then 'SST'
when b.adjust_cst = -240 then 'HAST'
when b.adjust_cst = -180 then 'AKST'
when b.adjust_cst = -120 then 'PST'
when b.adjust_cst = -60 then 'MST'
when b.adjust_cst = 0 then 'CST'
when b.adjust_cst = 60 then 'EST'
when b.adjust_cst = 120 then 'ATL'
else 'nil' end as Zip_Code_TZ
from dfsman_pbx_tz1 as a
left join score.d_areacode_zip_timezone as b on 
a.area_code=b.areacode
and substr(a.customer_zip_code,1,5)=b.zip_code
left join zip_state2 as c on a.customer_zip_code=c.customer_zip_code
;
quit;

proc sql;
create table dfsman_pbx_tz3 as select *
,case when area_code_tz in ('SST') then input(put(hour(timepart(pbx_dial_dt_start))-5, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Alaska TZ   */
when area_code_tz in ('HAST') then input(put(hour(timepart(pbx_dial_dt_start))-4, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Atlantic TZ */
when area_code_tz in ('AKST') then input(put(hour(timepart(pbx_dial_dt_start))-3, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('PST') then input(put(hour(timepart(pbx_dial_dt_start))-2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('MST') then input(put(hour(timepart(pbx_dial_dt_start))-1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Hawaii TZ  */
when area_code_tz in ('CST') then input(put(hour(timepart(pbx_dial_dt_start)), z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('EST') then input(put(hour(timepart(pbx_dial_dt_start))+1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('ATL') then input(put(hour(timepart(pbx_dial_dt_start))+2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('nil') and Zip_Code_TZ in ('SST') then input(put(hour(timepart(pbx_dial_dt_start))-5, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Hawaii TZ  */
when area_code_tz in ('nil') and Zip_Code_TZ in ('HAST') then input(put(hour(timepart(pbx_dial_dt_start))-4, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Between Hawaii and Alaska TZ   */
when area_code_tz in ('nil') and Zip_Code_TZ in ('AKST') then input(put(hour(timepart(pbx_dial_dt_start))-3, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Alaska TZ   */
when area_code_tz in ('nil') and Zip_Code_TZ in ('PST') then input(put(hour(timepart(pbx_dial_dt_start))-2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Atlantic TZ */
when area_code_tz in ('nil') and Zip_Code_TZ in ('MST') then input(put(hour(timepart(pbx_dial_dt_start))-1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('nil') and Zip_Code_TZ in ('CST') then input(put(hour(timepart(pbx_dial_dt_start)), z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('nil') and Zip_Code_TZ in ('EST') then input(put(hour(timepart(pbx_dial_dt_start))+1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('nil') and Zip_Code_TZ in ('ATL') then input(put(hour(timepart(pbx_dial_dt_start))+2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)     
else timepart(pbx_dial_dt_start) end as dt_strt_by_area_cd format=time8.
,case when Zip_Code_TZ in ('SST') then input(put(hour(timepart(pbx_dial_dt_start))-5, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Hawaii TZ  */
when Zip_Code_TZ in ('HAST') then input(put(hour(timepart(pbx_dial_dt_start))-4, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Between Hawaii and Alaska TZ   */
when Zip_Code_TZ in ('AKST') then input(put(hour(timepart(pbx_dial_dt_start))-3, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Alaska TZ   */
when Zip_Code_TZ in ('PST') then input(put(hour(timepart(pbx_dial_dt_start))-2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Atlantic TZ */
when Zip_Code_TZ in ('MST') then input(put(hour(timepart(pbx_dial_dt_start))-1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('CST') then input(put(hour(timepart(pbx_dial_dt_start)), z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('EST') then input(put(hour(timepart(pbx_dial_dt_start))+1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('ATL') then input(put(hour(timepart(pbx_dial_dt_start))+2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('nil') and area_code_tz in ('SST') then input(put(hour(timepart(pbx_dial_dt_start))-5, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Alaska TZ   */
when Zip_Code_TZ in ('nil') and area_code_tz in ('HAST') then input(put(hour(timepart(pbx_dial_dt_start))-4, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Atlantic TZ */
when Zip_Code_TZ in ('nil') and area_code_tz in ('AKST') then input(put(hour(timepart(pbx_dial_dt_start))-3, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('nil') and area_code_tz in ('PST') then input(put(hour(timepart(pbx_dial_dt_start))-2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('nil') and area_code_tz in ('MST') then input(put(hour(timepart(pbx_dial_dt_start))-1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Hawaii TZ  */
when Zip_Code_TZ in ('nil') and area_code_tz in ('CST') then input(put(hour(timepart(pbx_dial_dt_start)), z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('nil') and area_code_tz in ('EST') then input(put(hour(timepart(pbx_dial_dt_start))+1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('nil') and area_code_tz in ('ATL') then input(put(hour(timepart(pbx_dial_dt_start))+2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
else timepart(pbx_dial_dt_start) end as dt_strt_by_zip_cd format=time8.
from dfsman_pbx_tz2
/* from tz_cons_eval_data5 >> used this dataset for testing records only having data for both area code and zip code. */ 
;
quit;

proc sql;
create table dfsman_pbx_tz4 as select *
,case when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd < dt_strt_by_zip_cd then 'area_dt'
when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd > dt_strt_by_zip_cd then 'zip_dt' 
when dt_strt_by_area_cd = dt_strt_by_zip_cd then 'same' else 'nill' end as early_time_ind
,case when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd > dt_strt_by_zip_cd then 'area_dt'
when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd < dt_strt_by_zip_cd then 'zip_dt' 
when dt_strt_by_area_cd = dt_strt_by_zip_cd then 'same' else 'nill' end as late_time_ind
from dfsman_pbx_tz3
;
quit;

proc sql;
create table dfsman_pbx_tz5 as select *
,hour(timepart(pbx_dial_dt_start)) as cst_attempt_hour
,datepart(pbx_dial_dt_start) as datetime_datepart format=date7.
,case when hour(timepart(pbx_dial_dt_start)) < 12 and early_time_ind = 'area_dt' then dt_strt_by_area_cd
when hour(timepart(pbx_dial_dt_start)) < 12 and early_time_ind = 'zip_dt' then dt_strt_by_zip_cd
when hour(timepart(pbx_dial_dt_start)) > 11 and late_time_ind = 'area_dt' then dt_strt_by_area_cd
when hour(timepart(pbx_dial_dt_start)) > 11 and late_time_ind = 'zip_dt' then dt_strt_by_zip_cd 
else dt_strt_by_area_cd end as est_local_time format=time8.
from dfsman_pbx_tz4
;
quit;

proc sql;
create table dfsman_pbx_tz6 as select *
,case when est_local_time < input('07:59:00',time8.) then 1 else 0 end as before_8AM_misses
,case when est_local_time > input('21:00:00',time8.) then 1 else 0 end as after_9PM_misses
,case when est_local_time >= input('07:59:00',time8.)
and est_local_time <= input('21:01:00',time8.) then 1 else 0 end as tz_compliant
from dfsman_pbx_tz5
;
quit;

proc sql;
create table dfsman_pbx_tz7 as select *
,case when before_8AM_misses > 0 and after_9PM_misses =0 then 'Before 8AM'
when before_8AM_misses = 0 and after_9PM_misses = 0 then 'OK'
when after_9PM_misses > 0 and before_8AM_misses =0 then 'After 9PM'
else 'nil' end as dialer_bucket
from dfsman_pbx_tz6
;
quit;

proc sql;
create table auto_dial_col_arr as select D2_ACCOUNT_NUMBER as account_number label='account_number'
,d2_agency as agency label='agency'
,d2_agent_id_cacs as agent_id label='agent_id'
,list as list label='list'
,campaign as campaign label='campaign'
,datetime_datepart as attempt_date label='attempt_date'
,D2_DATETIME_CALL_START as datetime_call_start label='datetime_call_start'
,'n/a' as Extension label='Extension'
/*,consent_given as consent_given label='consent_given'
,no_consent_flag as no_consent_flag label='no_consent_flag'*/
,d2_connect_time as duration label='duration'
,d2_phone_number as phone_number label='phone_number'
,area_code as area_code label='area_code'
,exchange as area_code_exchange label='area_code_exchange'
,area_cd_state as area_code_state label='area_code_state'
,Area_Code_TZ as area_code_TZ label='area_code_TZ'
,zip_code as zip_code label='zip_code'
,zip_cd_state as zip_code_state label='zip_code_state'
,zip_cd_tz as zip_code_TZ label='zip_code_TZ'
,dt_strt_by_area_cd as area_cd_local_time label='area_cd_local_time'
,dt_strt_by_zip_cd as zip_cd_local_time label='zip_cd_local_time'
,est_local_time as est_local_time label='est_local_time'
,result_rpc as rpc_ind label='rpc_ind'
,attempt_source as attempt_source label='attempt_source'
,1 as attempt_ind label='attempt_ind'
,early_time_ind as early_time_ind label='early_time_ind'
,late_time_ind as late_time_ind label='late_time_ind'
,before_8AM_misses as before_8AM_misses label='before_8AM_misses'
,after_9PM_misses as after_9PM_misses label='after_9PM_misses'
,tz_compliant as tz_compliant label='tz_compliant'
,dialer_bucket as dialer_bucket label='dialer_bucket'
,outcome_cd||'_' as outcome_code label='outcome_code'
,case when result_rpc=1 then 'Right Party Contact' else long_desc end as outcome_description label='outcome_description'
from autodial_src_tz7
;
quit;

proc sql;
create table pbx_dial_col_arr as select dfsman_account as account_number label='account_number'
,'aras manual' as agency label='agency'
,agent_name as agent_id label='agent_id'
,'n/a' as list label='list'
,'n/a' as campaign label='campaign'
,datetime_datepart as attempt_date label='attempt_date'
,pbx_dial_dt_start as datetime_call_start label='datetime_call_start'
,pbx_extension as Extension label='Extension'
/*,'Y' as consent_given label='consent_given'
,0 as no_consent_flag label='no_consent_flag'*/
,pbx_duration as duration label='duration'
,pbx_phone_number as phone_number label='phone_number'
,area_code as area_code label='area_code'
,area_cd_exchange as area_code_exchange label='area_code_exchange'
,customer_state as area_code_state label='area_code_state'
,Area_Code_TZ as area_code_TZ label='area_code_TZ'
,zip_code as zip_code label='zip_code'
,zip_cd_state as zip_code_state label='zip_code_state'
,Zip_Code_TZ as zip_code_TZ label='zip_code_TZ'
,dt_strt_by_area_cd as area_cd_local_time label='area_cd_local_time'
,dt_strt_by_zip_cd as zip_cd_local_time label='zip_cd_local_time'
,est_local_time as est_local_time label='est_local_time'
,result_rpc as rpc_ind label='rpc_ind'
,attempt_source as attempt_source label='attempt_source'
,1 as attempt_ind label='attempt_ind'
,early_time_ind as early_time_ind label='early_time_ind'
,late_time_ind as late_time_ind label='late_time_ind'
,before_8AM_misses as before_8AM_misses label='before_8AM_misses'
,after_9PM_misses as after_9PM_misses label='after_9PM_misses'
,tz_compliant as tz_compliant label='tz_compliant'
,dialer_bucket as dialer_bucket label='dialer_bucket'
,dfsman_status as outcome_code label='outcome_code'
,outcome_description
from dfsman_pbx_tz7
;
quit;

data join;
set auto_dial_col_arr pbx_dial_col_arr;
run;

/* Start of RPC Reporting Code  */

data rpc_one_a_day_src1;
set join;
if account_number not in (' ');
run;

proc sort data=rpc_one_a_day_src1;
by datetime_call_start;
run;

/*Data set containing attempts data which are all manual pbx attempts with no dfs screen data*/
data no_acct_rpc;
set join;
if account_number in (' ');
run;

proc sql;
create table rpc_one_a_day1 as select account_number
,agency
,attempt_date
,phone_number
,timepart(datetime_call_start) as attempt_time format=time8.
,attempt_source
,outcome_code
,outcome_description
,case when outcome_description in ('Right Party Contact','Promise To Pay','CPU') then 1 else 0 end as rpc_ind
from rpc_one_a_day_src1;
quit;

proc sort data=rpc_one_a_day1;
by account_number phone_number attempt_time;
run;

proc sql;
create table rpc_one_a_day2 as select distinct account_number
from rpc_one_a_day1
where outcome_description in ('Right Party Contact','Promise To Pay','CPU')
;
quit;

proc sql;
create table rpc_one_a_day3 as select compress(account_number||'-'||phone_number) as acct_phone
,*
from rpc_one_a_day1
where account_number in (select account_number from rpc_one_a_day2)
;
quit;

proc sort data=rpc_one_a_day3;
by account_number phone_number attempt_time;
run;

proc sql;
create table rpc_one_a_day4a as select account_number
,phone_number
,attempt_date
,sum(rpc_ind) as total_rpc
from rpc_one_a_day3
group by account_number, phone_number, attempt_date
having sum(rpc_ind) > 1
;
quit;

proc sql;
create table rpc_one_a_day4b as select a.account_number
,a.phone_number
,b.attempt_source
,b.attempt_date
,b.attempt_time
,b.rpc_ind
from rpc_one_a_day4a as a
left join rpc_one_a_day3 as b on a.account_number=b.account_number
and a.phone_number=b.phone_number
;
quit;

proc sql;
create table rpc_one_a_day4c as select account_number
,phone_number
,attempt_source
,attempt_date
,sum(rpc_ind) as total_rpc
from rpc_one_a_day4b
group by account_number, phone_number, attempt_source, attempt_date
;
quit;

proc sql;
create table rpc_one_a_day5 as select account_number
,phone_number
,attempt_date
,attempt_time
,outcome_description as result
,attempt_source as source
from rpc_one_a_day1
where account_number in (select account_number from rpc_one_a_day4a)
and outcome_description in ('Right Party Contact','Promise To Pay','CPU')
;
quit;

options orientation=portrait
		papersize=letter;

/*ods pdf file='\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\rpc_morethan1aday_sum.pdf'*/
ods pdf file=&outfile1
style=minimal;

proc print data=rpc_one_a_day4c NOOBS;
 title 'Report#6 Account Summary with Attempts Flagged as Having More Than 1 RPC/Day';
run;

ods pdf close;

options orientation=portrait
		papersize=letter;

/*ods pdf file='\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\rpc_morethan1aday_detail.pdf'*/
ods pdf file=&outfile2
style=minimal;

proc print data=rpc_one_a_day5 NOOBS;
 title 'Report#6 Account Detail with Attempts Flagged as Having More Than 1 RPC/Day';
run;

ods pdf close;

data rpc_one_a_day6a;
set rpc_one_a_day3;
by acct_phone;
if first. acct_phone then n=1;
else n+1;
run;

proc sql;
create table rpc_one_a_day6b as select acct_phone
,max(n) as max_acct_attempts
,max(attempt_time) as last_attempt_time format=time8.
from rpc_one_a_day6a
group by acct_phone;
quit;

proc sql;
create table rpc_one_a_day6c as select a.acct_phone
,a.agency
,a.attempt_date
,a.attempt_time
,a.attempt_source
,a.outcome_code
,a.outcome_description
,a.rpc_ind
,a.n
,b.max_acct_attempts
,b.last_attempt_time
from rpc_one_a_day6a as a
left join rpc_one_a_day6b as b on a.acct_phone=b.acct_phone
;
quit;

proc sql;
create table rpc_one_a_day6d as select distinct acct_phone
,attempt_date
,n as rpc_in_attempt_sequence
,max_acct_attempts as total_acct_attempts
from rpc_one_a_day6c
where outcome_description in ('Right Party Contact','Promise To Pay','CPU') and 
last_attempt_time > attempt_time
;
quit;

proc sql;
create table referred as select c13_acct_num
,c13_processing_date
,c13_referred_ind
,c13_a3p_tp_id
from odscacs.C13_CACS_P325PRI_DAILY
where c13_acct_num in (select substr(acct_phone,1,18) from rpc_one_a_day6d)
and c13_processing_date =  intnx('dtday',&date_dt,-1) /* >> Get agency acct referred to the day prior to processing date being queried >> '15OCT2014:00:00:00'dt */
and c13_referred_ind = 'Y'
;
quit;

proc sql;
create table rpc_one_a_day6g as select substr(a.acct_phone,1,18) as account_number
,b.c13_a3p_tp_id
,a.attempt_date
,a.total_acct_attempts
from rpc_one_a_day6d as a
left join referred as b on substr(a.acct_phone,1,18)=b.c13_acct_num
;
quit;

proc sql;
create table all_attempts1 as select distinct account_number
from rpc_one_a_day_src1
;
quit;

proc sql;
create table referred2 as select c13_acct_num
,c13_processing_date
,c13_referred_ind
,c13_a3p_tp_id
from odscacs.C13_CACS_P325PRI_DAILY
where c13_acct_num in (select account_number from all_attempts1)
and c13_processing_date = intnx('dtday',&date_dt,-1) /* >> Get agency acct referred to the day prior to processing date being queried >> '15OCT2014:00:00:00'dt */
and c13_referred_ind = 'Y'
;
quit;

proc sql;
create table all_attempts2 as select c13_a3p_tp_id as agency
,count(c13_acct_num) as total_accts_attempted
from referred2
group by c13_a3p_tp_id
;
quit;

proc sql;
create table rpc_one_a_day6h as select c13_a3p_tp_id as agency label='agency'
,count(account_number) as post_att_acct_phone label='post_attempt_acct_phone' /* GM101714, Updated label to post_att_acct_phone   */
from rpc_one_a_day6g
group by c13_a3p_tp_id
;
quit;

proc sql;
create table report7sum as select a.agency
,a.total_accts_attempted
,case when b.post_att_acct_phone = (.) then 0 else b.post_att_acct_phone end as post_att_acct_phone /* GM101714, Updated label to post_att_acct_phone   */
,case when b.post_att_acct_phone = (.) then 0 else (b.post_att_acct_phone/a.total_accts_attempted) end as miss_percentage format=percent10.2 /* GM101714, Updated label to post_att_acct_phone   */
from all_attempts2 as a
left join rpc_one_a_day6h as b on a.agency=b.agency
;
quit;

options orientation=portrait
		papersize=letter;

/*ods pdf file='\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\post_rpc_attempts_summary.pdf'*/
ods pdf file=&outfile3
style=minimal;

proc print data=report7sum NOOBS;
 title 'Summary - Accounts w/ at least 1 RPC/Day with post RPC attempts';
run;

ods pdf close;

proc sql;
create table rpc_one_a_day6e as select substr(acct_phone,1,18) as account_number
,substr(acct_phone,20,10) as phone_number
,attempt_date
,attempt_time
,outcome_description
,attempt_source
from rpc_one_a_day6c
where substr(acct_phone,1,18) in (select substr(acct_phone,1,18) from rpc_one_a_day6d)
order by account_number, attempt_time descending
;
quit;

options orientation=portrait
		papersize=letter;

/*ods pdf file='\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\post_rpc_attempts_detail.pdf'*/
ods pdf file=&outfile4
style=minimal;

proc print data=rpc_one_a_day6e NOOBS;
 title 'Detail - Accounts w/ at least 1 RPC/Day with post RPC attempts';
run;

ods pdf close;

/* Start of TZ compliance reporting code.  */

proc sql;
create table tz_compliance_src as select account_number as account
,attempt_source
,attempt_date
,Extension
,agent_id as agent
,phone_number
,area_code
,area_code_exchange
,area_code_state
,area_code_TZ
,zip_code
,zip_code_state as zip_state
,zip_code_TZ
,est_local_time as conservative_local_time
,dialer_bucket
,before_8AM_misses
,after_9PM_misses
,attempt_ind as attempt
from join
where attempt_ind > 0
and est_local_time ne timepart(input('00:00:00',time8.))
;
quit;

proc sql;
create table tz_compliance_summary as select attempt_source
,attempt_date
,sum(attempt) as total_attempts
,sum(before_8AM_misses) as Missed_Before_8AM_local
,sum(after_9PM_misses) as Missed_After_9PM_local
,(sum(before_8AM_misses) + sum(after_9PM_misses))/sum(attempt) as Compliance_Risk_Percentage format=percent10.2
from tz_compliance_src
group by attempt_source, attempt_date
;
quit;

options orientation=landscape
		papersize=letter;

/*ods pdf file='\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\tz_compliance_summary.pdf'*/
ods pdf file=&outfile5
style=minimal;

proc print data=tz_compliance_summary NOOBS;
 title 'Summary - Time Zone Out of Compliance Based on Conservative Local TZ(area code vs. zip code)';
run;

ods pdf close;

proc sql;
create table tz_compliance_detail as select account
,attempt_source
,attempt_date
,Extension
,agent
,phone_number
,area_code
,area_code_exchange
,area_code_state
,area_code_TZ
,zip_code
,zip_state
,zip_code_TZ
,conservative_local_time
,dialer_bucket
from tz_compliance_src
where attempt > 0
and (before_8AM_misses > 0 or after_9PM_misses > 0)
;
quit;

PROC EXPORT DATA= tz_compliance_detail                                                                                                                                                                                                                                                                                                                                    
            /*OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\tz_compliance_detail.txt"*/
			OUTFILE=&outfile6
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

%mend;

%TZ_RPC_Compliance('03MAR2015'd,'03MAR2015:00:00:00'dt,
'\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\Report6_RPC_MoreThan1aDay_Sum_040315.pdf',
'\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\Report6_RPC_MoreThan1aDay_Detail_040315.pdf',
'\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\Report7_Post_RPC_Attempts_Summary_040315.pdf',
'\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\Report7_Post_RPC_Attempts_Detail_040315.pdf',
'\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\Report1_TZ_Compliance_Summary_040315.pdf',
"\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\Report1_TZ_Compliance_Detail_040315.txt");
 
proc sql;
create table cch_disp as select acct_num /* &dataset2 */
,input(put(activity_date,z6.),yymmdd6.) as activity_date format=yymmdd6.
,input(substr(put(TIME_OF_ACTIVITY,z4.),1,2)||':'||substr(put(time_of_activity,z4.),3,4),time5.) as activity_time format=time5.
,ROUTE_TO_STATE_CD
,coll_activity_code
,location_code
,coded_hist_seq_num
,user_id

,PARTY_CNTCT_CODE
,PLACE_CALLED
,PROMISE_AMT_1
,input(put(PROMISE_DATE_1,z6.),yymmdd6.) as PROMISE_DATE_1 format=mmddyy10.
,PAYMENT_METHOD_1
,PROMISE_AMT_2
,input(put(PROMISE_DATE_2,z6.),yymmdd6.) as PROMISE_DATE_2 format=mmddyy10.
,PAYMENT_METHOD_2
,NONPAY_EXCUSE_CODE

from odscacs.p325cch
where acct_num in (select distinct account_number from rpc_one_a_day6e)
and activity_date = 150330
/*and user_id not in (' ','AGCYPOST')*/
/*and compress(substr(user_id,1,7)) not in ('WEBPUSR')*/
order by acct_num, activity_time descending
;
quit;

proc sql;
create table cch_disp1 as select a.acct_num
,a.activity_date
,a.activity_time
,a.ROUTE_TO_STATE_CD
,a.coll_activity_code
,b.LONG_DESC as coll_act_desc label='coll_act_desc'
,a.location_code
,a.coded_hist_seq_num
,a.user_id

,a.PARTY_CNTCT_CODE
,c.LONG_DESC as party_contact_desc label='party_contact_desc'
,a.PLACE_CALLED
,d.LONG_DESC as place_called_desc label='place_called_desc'
,a.PROMISE_AMT_1
,a.PROMISE_DATE_1 format=mmddyy10.
,a.PAYMENT_METHOD_1
,a.PROMISE_AMT_2
,a.PROMISE_DATE_2 format=mmddyy10.
,a.PAYMENT_METHOD_2
,a.NONPAY_EXCUSE_CODE
,e.LONG_DESC as excuse_description label='excuse_description'

from cch_disp as a
left join cxprd.D_COLLECT_ACTIVITY_CD as b on a.coll_activity_code=b.COLLECT_ACTIVITY_CD
left join cxprd.D_PARTY_CONTACTED_CD as c on a.PARTY_CNTCT_CODE=c.PARTY_CONTACTED_CD
left join cxprd.D_PLACE_CALLED_CD as d on a.PLACE_CALLED=d.PLACE_CALLED_CD
left join cxprd.d_excuse_cd as e on a.NONPAY_EXCUSE_CODE=e.EXCUSE_CD
order by a.acct_num, a.activity_date descending, a.activity_time descending
;
quit;

/*Get raw attempts from rpc_source data for accounts in list of accounts w/ post RPC attempts*/
/*Note: This query returns the same 19 records as the original detail set*/
proc sql;
create table all_post_rpc_att1 as select *

from rpc_one_a_day_src1
where account_number in (select distinct account_number from rpc_one_a_day6e)
order by account_number, datetime_call_start descending
;
quit;

proc sql;
create table all_post_rpc_att2 as select *

from pbx_dfsmanual_join
where dfsman_account is null
and pbx_phone_number in (select distinct compress(phone_number) from rpc_one_a_day6e)
;
quit;

data rpc_man_post_att_seq;
set rpc_one_a_day6e_1;
by account_number;
if first. account_number then n=1;
else n+1;
run;

proc sql;
create table min_diff1 as select account_number
,phone_number
,attempt_time
,outcome_description
,attempt_source
,n-1 as join_n

from rpc_man_post_att_seq
where n > 1
;
quit;

proc sql;
create table min_diff2 as select a.account_number
,a.phone_number
,a.attempt_time
,a.outcome_description
,b.attempt_time as nxt_att_time
,b.outcome_description as nxt_att_desc
,b.attempt_time - a.attempt_time as att_lag_time

from rpc_man_post_att_seq as a
left join min_diff1 as b on a.account_number=b.account_number
and a.n=b.join_n
where b.outcome_description is not null
;
quit;

proc sql;
create table min_diff3 as select account_number
,min(att_lag_time) as min_att_lag_time

from min_diff2
group by account_number
;
quit;

proc sql;
create table min_diff4 as select a.*

from min_diff2 as a
inner join min_diff3 as b on a.account_number=b.account_number
and a.att_lag_time=b.min_att_lag_time
;
quit;

PROC EXPORT DATA= min_diff4                                                                                                                                                                                                                                                                                                                                  
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\min_diff4.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

PROC EXPORT DATA= cch_disp1                                                                                                                                                                                                                                                                                                                                  
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\cch_disp1.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;
