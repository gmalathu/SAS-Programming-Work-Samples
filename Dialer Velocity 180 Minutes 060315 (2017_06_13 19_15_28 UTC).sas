libname odscacs oracle user="GMALATHU" password="Ph03n1x762223" path=prdBIDW schema=ods_cacs;
libname dwprd oracle user="GMALATHU" password="Ph03n1x762223" path=prdBIDW schema=dwprd;
libname cxprd oracle user="GMALATHU" password="Ph03n1x762223" path=prdBIDW schema=cxprd;
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

%macro dv_1hr(date_dt1,date_dt2,date_d);

proc sql;
create table attempts1a as select D2_ACCOUNT_NUMBER as account_number label='account_number'
,D2_AGENCY as agency label='agency'
,D2_AGENT_ID_CACS as agent label='agent'
,D2_CALL_ID as call_id label='call_id'
,D2_CALL_LIST_ID as call_list_id label='call_list_id'
,D2_CALL_TYPE as call_type label='call_type'
,D2_CALL_TYPE_RAW as campaign label='campaign'
,D2_CAMPAIGN_ID as list label='list'
,D2_DATETIME_CALL_END as datetime_call_end label='datetime_call_end'
,D2_DATETIME_CALL_START as datetime_call_start label='datetime_call_start'
,d2_contact_outcome_code as outcome_code label='outcome_code'
,D2_PHONE_NUMBER as phone_number label='phone_number'
,D2_PHONE_TYPE as phone_type label='phone_type'
,D2_TIME_ZONE as time_zone label='time_zone'
,D2_TRANSACTION_DATE as attempt_date label='attempt_date'
,case when d2_contact_outcome_code not in ('00','01','02','03','04','05','06','11','20') then 1 else 0 end as cfpb_attempt
,case when d2_contact_outcome_code in ('31','32','33','34','35','36','37','38', '39','40')
            then 1 else 0 end as rpc

from odscacs.d2_dialer_attempts

where d2_transaction_date = &date_dt1
and d2_time_zone in ('EST','CST','MST','PST')
and substr(D2_CALL_TYPE_RAW,4,1) ne 'I'
and d2_agency in ('ARA','AR5','AR6')
and d2_contact_outcome_code not in ('00','01','02','03','04','05','06','11','20')
order by d2_account_number, d2_datetime_call_start
;
quit;

/*

proc sql;
create table join as select a.*
,b.join_n as b_join_n
,round(((b.hour_call_start-a.hour_call_start)/60)/60,.01) as time_btw_attempts1 format=best12.
,b.hour_call_start-a.hour_call_start as time_btw_attempts2 format=hhmm.
from attempts2b as a
inner join attempts2c as b on a.account_number=b.account_number
and a.n=b.join_n
where a.join_n > 0
and b.join_n > 1
and round(((b.hour_call_start-a.hour_call_start)/60)/60,.01) < 1
;
quit;

proc sql;
create table join2 as select a.*
,b.time_btw_attempts1
,b.time_btw_attempts2
,case when b.time_btw_attempts1 > 0 or b.time_btw_attempts2 > 0 then 1 else 0 end as below_3hour_flag
from attempts2b as a
left join join as b on a.account_number=b.account_number
and a.n=b.n
;
quit;

proc sql;
create table stat_sum as select datepart(attempt_date) as date format=date7.
,agency
,list
,campaign
,sum(attempt) as total_attempts
,sum(below_3hour_flag) as below_3hour_attempts
from join2
group by date, agency, list, campaign
;
quit;

proc sql;
create table stat_detail as select datepart(attempt_date) as attempt_date format=date7.
,account_number
,agency
,list
,campaign
,time_zone
,hour_call_start
,n as attempt_sequence_number
,time_btw_attempts2
from join2
where account_number in (select account_number from accounts)
;
quit;
*/

proc sql;
create table d2_agency_chk as select c13_acct_num
,c13_a3p_tp_id as agency
,c13_location_code as location_code

from odscacs.C13_CACS_P325PRI_DAILY
where c13_acct_num in (select distinct D2_ACCOUNT_NUMBER from attempts1a)
and c13_processing_date = &date_dt2
;
quit;

proc sql;
create table autodial_src1 as select *
,agency
,'autodialer' as attempt_source
from attempts1a
;
quit;

proc sql;
create table max_customer_stzip as select c14_acct_num
,c14_processing_date
,c14_customer_state
,c14_customer_zip_code
from odscacs.C14_CACS_P327EXT_DAILY
where c14_acct_num in (select distinct account_number from attempts1a)
and c14_processing_date = &date_dt2
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
,substr(a.PHONE_NUMBER,1,3) as area_code
,b.c14_customer_state as acct_state
,b.c14_customer_zip_code as customer_zip_code
from autodial_src1 as a left join 
max_customer_stzip as b on a.ACCOUNT_NUMBER=b.c14_acct_num
and a.attempt_date=b.c14_processing_date
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
and substr(a.phone_number,4,3)=b.exchange
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
when area_code_tz in ('SST') then input(put(hour(timepart(datetime_call_start))-5, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Alaska TZ   */
when area_code_tz in ('HAST') then input(put(hour(timepart(datetime_call_start))-4, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Atlantic TZ */
when area_code_tz in ('AKST') then input(put(hour(timepart(datetime_call_start))-3, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when area_code_tz in ('PST') then input(put(hour(timepart(datetime_call_start))-2, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when area_code_tz in ('MST') then input(put(hour(timepart(datetime_call_start))-1, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Hawaii TZ  */
when area_code_tz in ('CST') then input(put(hour(timepart(datetime_call_start)), z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when area_code_tz in ('EST') then input(put(hour(timepart(datetime_call_start))+1, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when area_code_tz in ('ATL') then input(put(hour(timepart(datetime_call_start))+2, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when area_code_tz in ('nil') and zip_cd_tz = 'SST' then input(put(hour(timepart(datetime_call_start))-5, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Hawaii TZ  */
when area_code_tz in ('nil') and zip_cd_tz = 'HAST' then input(put(hour(timepart(datetime_call_start))-4, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Between Hawaii and Alaska TZ   */
when area_code_tz in ('nil') and zip_cd_tz = 'AKST' then input(put(hour(timepart(datetime_call_start))-3, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Alaska TZ   */
when area_code_tz in ('nil') and zip_cd_tz = 'PST' then input(put(hour(timepart(datetime_call_start))-2, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Atlantic TZ */
when area_code_tz in ('nil') and zip_cd_tz = 'MST' then input(put(hour(timepart(datetime_call_start))-1, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when area_code_tz in ('nil') and zip_cd_tz = 'CST' then input(put(hour(timepart(datetime_call_start)), z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when area_code_tz in ('nil') and zip_cd_tz = 'EST' then input(put(hour(timepart(datetime_call_start))+1, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when area_code_tz in ('nil') and zip_cd_tz = 'ATL' then input(put(hour(timepart(datetime_call_start))+2, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
else timepart(datetime_call_start) end as dt_strt_by_area_cd format=time8.
,case when zip_cd_tz = 'SST' then input(put(hour(timepart(datetime_call_start))-5, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Hawaii TZ  */
when zip_cd_tz = 'HAST' then input(put(hour(timepart(datetime_call_start))-4, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Between Hawaii and Alaska TZ   */
when zip_cd_tz = 'AKST' then input(put(hour(timepart(datetime_call_start))-3, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Alaska TZ   */
when zip_cd_tz = 'PST' then input(put(hour(timepart(datetime_call_start))-2, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Atlantic TZ */
when zip_cd_tz = 'MST' then input(put(hour(timepart(datetime_call_start))-1, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when zip_cd_tz = 'CST' then input(put(hour(timepart(datetime_call_start)), z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when zip_cd_tz = 'EST' then input(put(hour(timepart(datetime_call_start))+1, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when zip_cd_tz = 'ATL' then input(put(hour(timepart(datetime_call_start))+2, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when zip_cd_tz in ('nil') and area_code_tz = 'SST' then input(put(hour(timepart(datetime_call_start))-5, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Alaska TZ   */
when zip_cd_tz in ('nil') and area_code_tz = 'HAST' then input(put(hour(timepart(datetime_call_start))-4, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Atlantic TZ */
when zip_cd_tz in ('nil') and area_code_tz = 'AKST' then input(put(hour(timepart(datetime_call_start))-3, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when zip_cd_tz in ('nil') and area_code_tz = 'PST' then input(put(hour(timepart(datetime_call_start))-2, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when zip_cd_tz in ('nil') and area_code_tz = 'MST' then input(put(hour(timepart(datetime_call_start))-1, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.) /* Hawaii TZ  */
when zip_cd_tz in ('nil') and area_code_tz = 'CST' then input(put(hour(timepart(datetime_call_start)), z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when zip_cd_tz in ('nil') and area_code_tz = 'EST' then input(put(hour(timepart(datetime_call_start))+1, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
when zip_cd_tz in ('nil') and area_code_tz = 'ATL' then input(put(hour(timepart(datetime_call_start))+2, z2.)||':'||put(minute(timepart(datetime_call_start)),z2.)||':'||put(second(timepart(datetime_call_start)),z2.),time8.)
else timepart(datetime_call_start) end as dt_strt_by_zip_cd format=time8.
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
,hour(timepart(datetime_call_start)) as cst_attempt_hour
,datepart(datetime_call_start) as datetime_datepart format=mmddyy8.
,case when hour(timepart(datetime_call_start)) < 12 and early_time_ind = 'area_dt' then dt_strt_by_area_cd
when hour(timepart(datetime_call_start)) < 12 and early_time_ind = 'zip_dt' then dt_strt_by_zip_cd
when hour(timepart(datetime_call_start)) > 11 and late_time_ind = 'area_dt' then dt_strt_by_area_cd
when hour(timepart(datetime_call_start)) > 11 and late_time_ind = 'zip_dt' then dt_strt_by_zip_cd 
else dt_strt_by_area_cd end as est_local_time format=time8.
from autodial_src_tz4
;
quit;

proc sql;
create table autodial_src_tz6a as select *
,case when est_local_time < input('07:59:00',time8.) then 1 else 0 end as before_8AM_misses
,case when est_local_time > input('21:01:00',time8.) then 1 else 0 end as after_9PM_misses
,case when est_local_time >= input('07:59:00',time8.)
and est_local_time <= input('21:01:00',time8.) then 1 else 0 end as tz_compliant
from autodial_src_tz5
;
quit;

proc sql;
create table autodial_src_tz6b as select *
,case when before_8AM_misses > 0 and after_9PM_misses =0 then 'Before 8AM'
when before_8AM_misses = 0 and after_9PM_misses = 0 then 'OK'
when after_9PM_misses > 0 and before_8AM_misses =0 then 'After 9PM'
else 'nil' end as dialer_bucket
from autodial_src_tz6a
;
quit;

proc sort data=autodial_src_tz6b;
by account_number datetime_call_start;
run;

proc sql;
create table autodial_src_tz6d as select *
,input(put(hour(est_local_time),z2.)||':'||put(minute(est_local_time),z2.),time5.) as hour_call_start format=time5.
from autodial_src_tz6b
;
quit;

data outcome_cd_desc;
set cxprd.d_outcome_cd;
run;

proc sort data=outcome_cd_desc nodupkey out=outcome;
by outcome_cd long_desc;
run;

proc sql;
create table join_2 as select a.*
,b.long_desc as outcome_description
from autodial_src_tz6d as a
left join outcome as b on a.outcome_code=b.outcome_cd
;
quit;

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
where datepart(call_start_datetime) = &date_d
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
where entry_date = &date_d
;
quit;

proc sql;
create table dfs_man_agency as select c13_acct_num
,c13_location_code as location_code
,c13_a3p_tp_id as agency

from odscacs.C13_CACS_P325PRI_DAILY
where c13_acct_num in (select distinct acct from dfsmanual1)
and c13_processing_date = &date_dt2
;
quit;

proc sql;
create table dfsmanual2 as select a.*
,b.agency

from dfsmanual1 as a
inner join dfs_man_agency as b on a.acct=b.c13_acct_num
;
quit;

data dfsmanual;
set dfsmanual2;
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
,b.agency as dfsman_agency label='dfsman_agency'
,b.phone as dfsman_phone label='dfsman_phone'
,b.agent as dfsman_agent label='dfsman_agent'
,b.state as dfsman_state label='dfsman_state'
,b.status as dfsman_status label='dfsman_status'
,case when b.status = 'RPC' then 'Right Party Contact'
when b.status = 'TPC' then 'Third Party Contact'
when b.status = 'ONC' then 'No Connect'
when b.status = 'OPC' then 'Other Party Contact'
when b.status = 'WPC' then 'Wrong Party Contact'
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

/* Exclude records from manual where the account number is missing (Missing DFS screen records)*/

data pbx_dfsmanual_join_a;
set pbx_dfsmanual_join;
if dfsman_account not in (' ');
run;

proc sort data=pbx_dfsmanual_join_a;
by dfsman_account pbx_dial_dt_start;
run;

proc sql;
create table cust_stzip as select c14_acct_num
,c14_processing_date
,c14_customer_state
,c14_customer_zip_code
from odscacs.C14_CACS_P327EXT_DAILY
where c14_acct_num in (select distinct dfsman_account from pbx_dfsmanual_join_a)
and c14_processing_date = &date_dt2
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
from pbx_dfsmanual_join_a as a 
left join cust_stzip as b on a.dfsman_account=b.c14_acct_num
;
quit;

proc sql;
create table dfsman_pbx_attsrc_rpc as select *
,substr(pbx_phone_number,1,3) as area_code
,'manual' as attempt_source
,case when dfsman_status = 'RPC' then 1 else 0 end as result_rpc
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
create table dfsman_pbx_tz6a as select *
,case when est_local_time < input('07:59:00',time8.) then 1 else 0 end as before_8AM_misses
,case when est_local_time > input('21:01:00',time8.) then 1 else 0 end as after_9PM_misses
,case when est_local_time >= input('07:59:00',time8.)
and est_local_time <= input('21:01:00',time8.) then 1 else 0 end as tz_compliant
from dfsman_pbx_tz5
;
quit;

proc sql;
create table dfsman_pbx_tz6b as select *
,case when before_8AM_misses > 0 and after_9PM_misses =0 then 'Before 8AM'
when before_8AM_misses = 0 and after_9PM_misses = 0 then 'OK'
when after_9PM_misses > 0 and before_8AM_misses =0 then 'After 9PM'
else 'nil' end as dialer_bucket
,input(put(hour(est_local_time),z2.)||':'||put(minute(est_local_time),z2.),time5.) as hour_call_start format=time5.
from dfsman_pbx_tz6a
;
quit;

proc sort data=dfsman_pbx_tz6b;
by dfsman_account est_local_time;
run;

data join_2a;
set dfsman_pbx_tz6b;
if dfsman_account not in (' ');
run;

proc sql;
create table col_arr_autodial as select compress(account_number||'-'||phone_number) as acct_phone
,account_number as account_number label='account_number'
,compress(agency) as agency label='agency'
,agent as agent label='agent'
,list as list label='list'
,campaign as campaign label='campaign'
,datetime_call_start as CST_Att_Start_Time label='CST_Att_Start_Time'
,est_local_time as Local_Att_Start_Time label='Local_Att_Start_Time'
,hour_call_start as hour_call_start label='hour_call_start'
,phone_number as phone_number label='phone_number'
,Area_Code_TZ as area_code_TZ label='area_code_TZ'
,zip_cd_tz as zip_code_TZ label='zip_code_TZ'
,datetime_datepart as attempt_date as attempt_date label='attempt_date'
,rpc as rpc label='rpc'
,outcome_description
from join_2
;
quit;

proc sql;
create table col_arr_manual as select compress(dfsman_account||'-'||pbx_phone_number) as acct_phone
,dfsman_account as account_number label='account_number'
,compress(dfsman_agency) as agency label='agency'
,agent_name as agent label='agent'
,'manual' as list label='list'
,'manual' as campaign label='campaign'
,pbx_dial_dt_start as CST_Att_Start_Time label='CST_Att_Start_Time'
,est_local_time as Local_Att_Start_Time label='Local_Att_Start_Time'
,hour_call_start as hour_call_start label='hour_call_start'
,pbx_phone_number as phone_number label='phone_number'
,area_code_TZ as area_code_TZ label='area_code_TZ'
,Zip_Code_TZ as zip_code_TZ label='zip_code_TZ'
,datetime_datepart as attempt_date label='attempt_date'
,result_rpc as rpc label='rpc'
,outcome_description
from join_2a;
;
quit;

data join3;
set col_arr_autodial col_arr_manual;
run;

proc sort data=join3;
by acct_phone Local_att_start_time;
run;

data dial_sequence1;
set join3;
by acct_phone;
if first.acct_phone then n=1;
else n+1;
run;

proc sql;
create table dial_sequence2 as select *
,1 as attempt
,n-1 as join_n
from dial_sequence1
;
quit;

data dial_sequence2a;
set dial_sequence2;
run;

/* Step that joins identical copy of tables to each other (dial_sequence2 and dial_sequence2a) to measure the time between attempts and calculate where the time is less than 1 hour. */

proc sql;
create table dial_sequence3 as select a.*
,b.outcome_description as next_att_disposition label='next_att_disposition'
,b.join_n as b_join_n
,round(((b.hour_call_start-a.hour_call_start)/60)/60,.01) as time_btw_attempts1 format=best12.
,b.hour_call_start-a.hour_call_start as time_btw_attempts2 format=hhmm.

from dial_sequence2 as a
inner join dial_sequence2a as b on a.acct_phone=b.acct_phone
and a.n=b.join_n
where a.join_n > 0
and b.join_n > 1
and round(((b.hour_call_start-a.hour_call_start)/60)/60,.01) < 3
;
quit;

proc sql;
create table join4 as select a.*
,b.next_att_disposition
,b.time_btw_attempts1
,b.time_btw_attempts2
,case when b.time_btw_attempts1 > 0 or b.time_btw_attempts2 > 0 then 1 else 0 end as below_3hour_flag
from dial_sequence2 as a
left join dial_sequence3 as b on a.acct_phone=b.acct_phone
and a.n=b.n
;
quit;

proc sql;
create table stat_sum1a as select c13_acct_num as acct_num label='acct_num'
,c13_referred_ind as referred_ind label='referred_ind'
,c13_a3p_tp_id as agency label='agency'
from odscacs.C13_CACS_P325PRI_DAILY
where acct_num in (select distinct account_number from join4)
and c13_processing_date = &date_dt2
and referred_ind = 'Y'
;
quit;

proc sql;
create table join4a as select a.acct_phone
,a.account_number
,b.agency
,a.agent
,a.list
,a.campaign
,a.CST_Att_Start_Time
,a.Local_Att_Start_Time
,a.hour_call_start
,a.phone_number
,a.area_code_TZ
,a.zip_code_TZ
,a.attempt_date
,a.rpc
,a.outcome_description
,a.n
,a.attempt
,a.join_n
,a.time_btw_attempts1
,a.time_btw_attempts2
,a.below_3hour_flag
from join4 as a
left join stat_sum1a as b on a.account_number=b.acct_num
;
quit;

proc sql;
create table accounts as select distinct account_number
from join4a
where below_3hour_flag > 0
;
quit;

proc sql;
create table hour_flag as select sum(below_3hour_flag) as sum_below_3hour_attempts
from join4a
;
quit;

proc sql;
create table stat_sum_rcvy as select attempt_date as date format=mmddyy8.
,agency
,list
,campaign
,sum(attempt) as total_attempts
,sum(below_3hour_flag) as below_3hour_attempts
,sum(below_3hour_flag)/sum(attempt) as miss_percentage format=percent10.2
from join4a
where agency in ('AR5')
group by date, agency, list, campaign
;
quit;

proc sql;
create table stat_sum_coll as select attempt_date as date format=mmddyy8.
,agency
,list
,campaign
,sum(attempt) as total_attempts
,sum(below_3hour_flag) as below_3hour_attempts
,sum(below_3hour_flag)/sum(attempt) as miss_percentage format=percent10.2
from join4a
where agency in ('ARA','AR6')
group by date, agency, list, campaign
;
quit;

proc sql;
create table stat_sum2 as select sum(below_3hour_flag)/sum(attempt) as miss_percentage format=percent10.2
from join4a
;
quit;

/*options orientation=landscape*/
/*		papersize=letter;*/
/**/
/*ods pdf file='\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\Dialer_Velocity_3HR_Summary_Rcvy_061315.pdf'*/
/*style=minimal;*/
/**/
/*proc print data=stat_sum_rcvy NOOBS;*/
/* title 'Recovery Summary - More Than 1 Attempt/Hour per Phone Number by Account';*/
/*run;*/
/**/
/*ods pdf close;*/

/*options orientation=landscape*/
/*		papersize=letter;*/
/**/
/*ods pdf file=&outfile1*/
/*style=minimal;*/
/**/
/*proc print data=stat_sum_coll NOOBS;*/
/* title '180 Minute Dialer Velocity for Yesterday Dials';*/
/*run;*/
/**/
/*ods pdf close;*/

proc sql;
create table stat_detail as select attempt_date as date format=mmddyy8.
,account_number
,phone_number
,list
,campaign
,case when substr(agency,1,6)='Manual' then agency else substr(agency,1,3) end as agency /* GM101714, Added agency to detail. */
,agent
,hour_call_start as prev_attempt label='prev_attempt'
,n as attempt_sequence_number
,time_btw_attempts2
,hour_call_start + time_btw_attempts2 as next_attempt format=time5.
,outcome_description as prev_disposition
,next_att_disposition as next_disposition
from join4
where account_number in (select account_number from accounts)
and time_btw_attempts2 not in (.)
;
quit;

/*PROC EXPORT DATA= stat_detail                                                                                                                                                                                                                                                                                                                                    */
/*            OUTFILE=&outfile2"\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\Dialer_Velocity_3HR_Detail_Coll_061315.txt"*/
/*            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          */
/*     PUTNAMES=YES;                                                        */
/*                                                                                                                                                                                                                                                                                                                                               */
/*RUN;*/

%mend;

%dv_1hr('03AUG2015:00:00:00'dt,'02AUG2015:00:00:00'dt,'03AUG2015'd);
data monthly_defects;
set stat_detail;
run;
%dv_1hr('04Aug2015:00:00:00'dt,'03Aug2015:00:00:00'dt,'04Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;

%dv_1hr('05Aug2015:00:00:00'dt,'04Aug2015:00:00:00'dt,'05Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('06Aug2015:00:00:00'dt,'05Aug2015:00:00:00'dt,'06Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('07Aug2015:00:00:00'dt,'06Aug2015:00:00:00'dt,'07Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('08Aug2015:00:00:00'dt,'07Aug2015:00:00:00'dt,'08Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('10Aug2015:00:00:00'dt,'09Aug2015:00:00:00'dt,'10Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('11Aug2015:00:00:00'dt,'10Aug2015:00:00:00'dt,'11Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('12Aug2015:00:00:00'dt,'11Aug2015:00:00:00'dt,'12Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('13Aug2015:00:00:00'dt,'12Aug2015:00:00:00'dt,'13Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('14Aug2015:00:00:00'dt,'13Aug2015:00:00:00'dt,'14Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('15Aug2015:00:00:00'dt,'14Aug2015:00:00:00'dt,'15Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('17Aug2015:00:00:00'dt,'16Aug2015:00:00:00'dt,'17Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('18Aug2015:00:00:00'dt,'17Aug2015:00:00:00'dt,'18Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('19Aug2015:00:00:00'dt,'18Aug2015:00:00:00'dt,'19Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('20Aug2015:00:00:00'dt,'19Aug2015:00:00:00'dt,'20Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('21Aug2015:00:00:00'dt,'20Aug2015:00:00:00'dt,'21Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('22Aug2015:00:00:00'dt,'21Aug2015:00:00:00'dt,'22Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('24Aug2015:00:00:00'dt,'23Aug2015:00:00:00'dt,'24Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('25Aug2015:00:00:00'dt,'24Aug2015:00:00:00'dt,'25Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('26Aug2015:00:00:00'dt,'25Aug2015:00:00:00'dt,'26Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('27Aug2015:00:00:00'dt,'26Aug2015:00:00:00'dt,'27Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('28Aug2015:00:00:00'dt,'27Aug2015:00:00:00'dt,'28Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('29Aug2015:00:00:00'dt,'28Aug2015:00:00:00'dt,'29Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('31Aug2015:00:00:00'dt,'30Aug2015:00:00:00'dt,'31Aug2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('01Sep2015:00:00:00'dt,'31Aug2015:00:00:00'dt,'01Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('02Sep2015:00:00:00'dt,'01Sep2015:00:00:00'dt,'02Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('03Sep2015:00:00:00'dt,'02Sep2015:00:00:00'dt,'03Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('04Sep2015:00:00:00'dt,'03Sep2015:00:00:00'dt,'04Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('05Sep2015:00:00:00'dt,'04Sep2015:00:00:00'dt,'05Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('07Sep2015:00:00:00'dt,'06Sep2015:00:00:00'dt,'07Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('08Sep2015:00:00:00'dt,'07Sep2015:00:00:00'dt,'08Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('09Sep2015:00:00:00'dt,'08Sep2015:00:00:00'dt,'09Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('10Sep2015:00:00:00'dt,'09Sep2015:00:00:00'dt,'10Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('11Sep2015:00:00:00'dt,'10Sep2015:00:00:00'dt,'11Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('12Sep2015:00:00:00'dt,'11Sep2015:00:00:00'dt,'12Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('14Sep2015:00:00:00'dt,'13Sep2015:00:00:00'dt,'14Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('15Sep2015:00:00:00'dt,'14Sep2015:00:00:00'dt,'15Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('16Sep2015:00:00:00'dt,'15Sep2015:00:00:00'dt,'16Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('17Sep2015:00:00:00'dt,'16Sep2015:00:00:00'dt,'17Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('18Sep2015:00:00:00'dt,'17Sep2015:00:00:00'dt,'18Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('19Sep2015:00:00:00'dt,'18Sep2015:00:00:00'dt,'19Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('21Sep2015:00:00:00'dt,'20Sep2015:00:00:00'dt,'21Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('22Sep2015:00:00:00'dt,'21Sep2015:00:00:00'dt,'22Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('23Sep2015:00:00:00'dt,'22Sep2015:00:00:00'dt,'23Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('24Sep2015:00:00:00'dt,'23Sep2015:00:00:00'dt,'24Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('25Sep2015:00:00:00'dt,'24Sep2015:00:00:00'dt,'25Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('26Sep2015:00:00:00'dt,'25Sep2015:00:00:00'dt,'26Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('28Sep2015:00:00:00'dt,'27Sep2015:00:00:00'dt,'28Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('29Sep2015:00:00:00'dt,'28Sep2015:00:00:00'dt,'29Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;
%dv_1hr('30Sep2015:00:00:00'dt,'29Sep2015:00:00:00'dt,'30Sep2015'd);
proc append base=monthly_defects data=stat_detail;
run;

PROC EXPORT DATA= monthly_defects                                                                                                                                                                                                                                                                                                                                  
			OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\monthly_defects.txt"
			DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
	 PUTNAMES=YES;                                                        
																																																																																			   
RUN;
