libname odscacs oracle user="GMALATHU" password="Ph03n1x135" path=prdBIDW schema=ods_cacs;
libname dwprd oracle user="GMALATHU" password="Ph03n1x135" path=prdBIDW schema=dwprd;
libname cxprd oracle user="GMALATHU" password="Ph03n1x135" path=prdBIDW schema=cxprd;
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

/* Cell phone types available to dial from max_number_list */
proc sql;
create table max_list as select *

from score.max_number_list /* Pull account/numbers in max_number_list to bounce against Jon's file and cellconsentfinal  */

where phone_type in ('PHN1CELL','PHN2CELL','PHN3CELL','PHN4CELL','opcell1','opcell2','opcell3','opcell4','scell1','scell2','scell3','scell4',
'scell5','scell6','scell7','scell8','scell9')
and agency in ('ARA','AR6','AR5')
and date = '01Oct2014'd
/* date = case when weekday(today())=2 then intnx('day',today(),-2) else intnx('day',today(),-1) end*/
;
quit;

proc sql;
create table phone_type2 as select *

from score.d_cellphone_id_file
where date = '01Oct2014'd
and consent_code='C';
quit;

data phone_numbers (keep=acct_num phone_number date); set max_list phone_type2;
run;

proc sort data=phone_numbers nodupkey;
by acct_num phone_number date;
run;

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
where D2_ACCOUNT_NUMBER in (select distinct acct_num from phone_numbers)
and d2_transaction_date = '01Oct2014:00:00:00'dt
and substr(D2_CALL_TYPE_RAW,4,1) ne 'I'
and d2_agency in ('ARA','AR5','AR6')
;
quit;

proc sql;
create table cell_attempts as select a.*
,case when a.d2_datetime_call_start not in (.) then 1 else 0 end as attempt
from d2_att as a
inner join phone_numbers as b on a.d2_account_number=b.acct_num
                                          and a.d2_phone_number=b.phone_number;
                                          quit;
proc sql;
create table jon1 as select substr(acct_nbr,2,18) as acct_num
,phone_num as phone label='phone'
from score.app_ph_num
where acct_nbr in (select distinct '6'||d2_account_number from cell_attempts)
;
quit;

proc sql;
create table ccf1 as select substr(acct_num,2,18) as acct_num
,phone
from score.cellconsentfinal
where report_date = '01Oct2014'd
and acct_num in (select distinct '6'||d2_account_number from cell_attempts)
;
quit;

data consent_mstr1;
set jon1 ccf1;
run;

proc sort data=consent_mstr1 nodupkey out=consent_mstr;
by acct_num phone;
run;

proc sql;
create table consent_join as select a.*
,b.acct_num as consent_acct label='consent_acct'
,b.phone as consent_phone label='consent_phone'
,case when b.acct_num not in (' ') then 'Y' else 'N' end as consent_given
,case when b.acct_num not in (' ') then 0 else 1 end as no_consent_flag
,datepart(a.d2_datetime_call_start) as attempt_date format=date9. label='attempt_date'
from cell_attempts as a
left join consent_mstr as b on a.d2_account_number=b.acct_num
                                          and a.d2_phone_number=b.phone
										  order by a.d2_account_number, a.d2_phone_number;
                                          quit;
										  
proc sql;
create table autodial_src1 as select *
,'autodialer' as attempt_source
from consent_join
;
quit;

proc sql;
create table max_customer_stzip as select c14_acct_num
,c14_processing_date
,c14_customer_state
,c14_customer_zip_code
from odscacs.C14_CACS_P327EXT_DAILY
where c14_acct_num in (select distinct acct_num from phone_numbers)
and c14_processing_date = '01Oct2014:00:00:00'dt
;
quit;

proc sql;
create table autodial_src2 as select a.*
,substr(a.D2_PHONE_NUMBER,1,3) as area_code label='area_code'
,b.c14_customer_state as acct_state label='acct_state'
,b.c14_customer_zip_code as acct_zip_code label='acct_zip_code'
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
,b.zip_code as zip_code
,c.state as zip_cd_state
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
and substr(a.acct_zip_code,1,5)=b.zip_code
left join mike.zip_timezone as c
on a.acct_zip_code=c.zipcode
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
,case when before_8AM_misses > 0 then 'Before 8AM'
when before_8AM_misses = 0 then 'OK'
when after_9PM_misses > 0 then 'After 9PM'
when after_9PM_misses = 0 then 'OK'
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
where datepart(call_start_datetime) = '01Oct2014'd
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
where entry_date = '01Oct2014'd
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
,case when substr(b.acct,1,1) = '6' then substr(b.acct,2,18) else b.acct end as dfsman_account label='dfsman_account'
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

proc sql;
create table join_pbx2cnst as select a.*
,b.phone as consent_ph_num
,case when b.acct_num not in (' ') then 'Y' else 'N' end as consent_given
from pbx_dfsmanual_join as a
left join consent_mstr as b on a.dfsman_account=substr(b.acct_num,2,18)
                                    and a.pbx_phone_number=b.phone;
                                    quit;
proc sql;
create table cust_stzip as select c14_acct_num
,c14_processing_date
,c14_customer_state
,c14_customer_zip_code
from odscacs.C14_CACS_P327EXT_DAILY
where c14_acct_num in (select distinct dfsman_account from pbx_dfsmanual_join)
and c14_processing_date = '01Oct2014:00:00:00'dt
;
quit;

proc sql;
create table dfsman_pbx_stzip1 as select a.*
,b.c14_customer_state as acct_state label='acct_state'
,substr(b.c14_customer_zip_code,1,5) as acct_zip_code label='acct_zip_code'
,c.zonediff
from join_pbx2cnst as a left join cust_stzip as b
on a.dfsman_account=b.c14_acct_num
left join mike.zip_timezone as c on substr(b.c14_customer_zip_code,1,5)=c.zipcode
;
quit;

proc sql;
create table dfsman_pbx_stzip as select *
,case when  zonediff = -4 then 'HAST'
when zonediff = -3 then 'AKST'
when zonediff = -2 then 'PST'
when zonediff = -1 then 'MST'
when zonediff = 0 then 'CST'
when zonediff = 1 then 'EST'
when zonediff = 2 then 'ATL'
when zonediff = (.) then 'n/a' else 'n/a' end as acct_TZ
from dfsman_pbx_stzip1
;
quit;

proc sql;
create table dfsman_pbx_attsrc_rpc as select *
,substr(pbx_phone_number,1,3) as area_code label='area_code'
,'manual' as attempt_source label='attempt_source'
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

proc sort data=mike.zip_timezone nodupkey out=mike_zip_dedupe;
by zipcode;
run;

proc sql;
create table dfsman_pbx_tz2 as select a.*
,b.zip_code as zip_cd_zip_code
,c.zipcode as mike_zip_code
,c.state as zip_cd_state
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
and substr(a.acct_zip_code,1,5)=b.zip_code
left join mike_zip_dedupe as c on substr(a.acct_zip_code,1,5)=c.zipcode
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
when area_code_tz in ('nil') and Zip_Code_TZ in ('nil') and acct_TZ in ('HAST') then input(put(hour(timepart(pbx_dial_dt_start))-4, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Between Hawaii and Alaska TZ   */
when area_code_tz in ('nil') and Zip_Code_TZ in ('nil') and acct_TZ in ('AKST') then input(put(hour(timepart(pbx_dial_dt_start))-3, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Alaska TZ   */
when area_code_tz in ('nil') and Zip_Code_TZ in ('nil') and acct_TZ in ('PST') then input(put(hour(timepart(pbx_dial_dt_start))-2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Atlantic TZ */
when area_code_tz in ('nil') and Zip_Code_TZ in ('nil') and acct_TZ in ('MST') then input(put(hour(timepart(pbx_dial_dt_start))-1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('nil') and Zip_Code_TZ in ('nil') and acct_TZ in ('CST') then input(put(hour(timepart(pbx_dial_dt_start)), z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('nil') and Zip_Code_TZ in ('nil') and acct_TZ in ('EST') then input(put(hour(timepart(pbx_dial_dt_start))+1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when area_code_tz in ('nil') and Zip_Code_TZ in ('nil') and acct_TZ in ('ATL') then input(put(hour(timepart(pbx_dial_dt_start))+2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
else timepart(input('00:00:00',time8.)) end as dt_strt_by_area_cd format=time8.

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
when Zip_Code_TZ in ('nil') and area_code_tz in ('nil') and acct_TZ in ('HAST') then input(put(hour(timepart(pbx_dial_dt_start))-4, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Between Hawaii and Alaska TZ   */
when Zip_Code_TZ in ('nil') and area_code_tz in ('nil') and acct_TZ in ('AKST') then input(put(hour(timepart(pbx_dial_dt_start))-3, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Alaska TZ   */
when Zip_Code_TZ in ('nil') and area_code_tz in ('nil') and acct_TZ in ('PST') then input(put(hour(timepart(pbx_dial_dt_start))-2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.) /* Atlantic TZ */
when Zip_Code_TZ in ('nil') and area_code_tz in ('nil') and acct_TZ in ('MST') then input(put(hour(timepart(pbx_dial_dt_start))-1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('nil') and area_code_tz in ('nil') and acct_TZ in ('CST') then input(put(hour(timepart(pbx_dial_dt_start)), z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('nil') and area_code_tz in ('nil') and acct_TZ in ('EST') then input(put(hour(timepart(pbx_dial_dt_start))+1, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
when Zip_Code_TZ in ('nil') and area_code_tz in ('nil') and acct_TZ in ('ATL') then input(put(hour(timepart(pbx_dial_dt_start))+2, z2.)||':'||put(minute(timepart(pbx_dial_dt_start)),z2.)||':'||put(second(timepart(pbx_dial_dt_start)),z2.),time8.)
else timepart(input('00:00:00',time8.)) end as dt_strt_by_zip_cd format=time8.
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
,case when est_local_time > input('21:01:00',time8.) then 1 else 0 end as after_9PM_misses
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
,consent_given as consent_given label='consent_given'
,no_consent_flag as no_consent_flag label='no_consent_flag'
,d2_connect_time as duration label='duration'
,d2_phone_number as phone_number label='phone_number'
,acct_state as account_state label='account_state'
,acct_zip_code as account_zip_code label='account_zip_code'
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
,attempt as attempt_ind label='attempt_ind'
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
,pbx_call_time_start as datetime_call_start label='datetime_call_start'
,pbx_extension as Extension label='Extension'
,'Y' as consent_given label='consent_given'
,0 as no_consent_flag label='no_consent_flag'
,pbx_duration as duration label='duration'
,pbx_phone_number as phone_number label='phone_number'
,acct_state as account_state label='account_state'
,acct_zip_code as account_zip_code label='account_zip_code'
,area_code as area_code label='area_code'
,area_cd_exchange as area_code_exchange label='area_code_exchange'
,area_cd_state as area_code_state label='area_code_state'
,Area_Code_TZ as area_code_TZ label='area_code_TZ'
,zip_cd_zip_code as zip_code label='zip_code'
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

data rpc_one_a_day_src1;
set join;
if account_number not in (' ');
run;

/*Data set containing attempts data which are all manual pbx attempts with no dfs screen data*/
data no_acct_rpc;
set join;
if account_number in (' ');
run;

proc sql;
create table rpc_one_a_day1 as select account_number
,phone_number
,est_local_time
,attempt_source
,outcome_code
,outcome_description
,case when outcome_description = 'Right Party Contact' then 1 else 0 end as rpc_ind
from rpc_one_a_day_src1;
quit;

proc sort data=rpc_one_a_day1;
by account_number est_local_time;
run;

proc sql;
create table rpc_one_a_day2 as select distinct account_number
from rpc_one_a_day1
where outcome_description = 'Right Party Contact'
;
quit;

proc sql;
create table rpc_one_a_day3 as select *
from rpc_one_a_day1
where account_number in (select account_number from rpc_one_a_day2)
;
quit;

proc sort data=rpc_one_a_day3;
by account_number est_local_time;
run;

proc sql;
create table rpc_one_a_day4 as select account_number
,attempt_source
,sum(rpc_ind) as total_rpc
from rpc_one_a_day3
group by account_number, attempt_source
having sum(rpc_ind) > 1
;
quit;

proc sql;
create table rpc_one_a_day5 as select account_number
,est_local_time as attempt_time
,outcome_description as result
,attempt_source as source
from rpc_one_a_day1
where account_number in (select account_number from rpc_one_a_day4)
and outcome_description = 'Right Party Contact'
;
quit;

options orientation=portrait
		papersize=letter;

ods pdf file='\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\rpc_morethan1aday_sum.pdf'
style=minimal;

proc print data=rpc_one_a_day4 NOOBS;
 title 'Report#6 Account Summary with Attempts Flagged as Having More Than 1 RPC/Day';
run;

ods pdf close;

options orientation=portrait
		papersize=letter;

ods pdf file='\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\rpc_morethan1aday_detail.pdf'
style=minimal;

proc print data=rpc_one_a_day5 NOOBS;
 title 'Report#6 Account Detail with Attempts Flagged as Having More Than 1 RPC/Day';
run;

ods pdf close;

data rpc_one_a_day6a;
set rpc_one_a_day3;
by account_number;
if first. account_number then n=1;
else n+1;
run;

proc sql;
create table rpc_one_a_day6b as select account_number
,max(n) as max_acct_attempts
from rpc_one_a_day6a
group by account_number;
quit;

proc sql;
create table rpc_one_a_day6c as select a.*
,b.max_acct_attempts
from rpc_one_a_day6a as a
left join rpc_one_a_day6b as b on a.account_number=b.account_number
;
quit;

proc sql;
create table rpc_one_a_day6d as select distinct account_number
,n as rpc_in_attempt_sequence
,max_acct_attempts as total_acct_attempts
from rpc_one_a_day6c
where outcome_description = 'Right Party Contact' and n < max_acct_attempts
;
quit;

options orientation=portrait
		papersize=letter;

ods pdf file='\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\post_rpc_attempts_summary.pdf'
style=minimal;

proc print data=rpc_one_a_day6d NOOBS;
 title 'Summary - Accounts w/ at least 1 RPC/Day with post RPC attempts';
run;

ods pdf close;

proc sql;
create table rpc_one_a_day6e as select account_number
,est_local_time as attempt_time
,outcome_description
,attempt_source
from rpc_one_a_day6c
where account_number in (select account_number from rpc_one_a_day6d)
order by account_number, est_local_time
;
quit;

options orientation=portrait
		papersize=letter;

ods pdf file='\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\dialer\tcpa_compliance_rptg\post_rpc_attempts_detail.pdf'
style=minimal;

proc print data=rpc_one_a_day6e NOOBS;
 title 'Detail - Accounts w/ at least 1 RPC/Day with post RPC attempts';
run;

ods pdf close;
