libname odscacs oracle user="GMALATHU" password="cHanG3m3T0da9" path=prdBIDW schema=ods_cacs;
libname dwprd oracle user="GMALATHU" password="cHanG3m3T0da9" path=prdBIDW schema=dwprd;
libname cxprd oracle user="GMALATHU" password="cHanG3m3T0da9" path=prdBIDW schema=cxprd;
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
where d2_transaction_date >= '07FEB2017:00:00:00'dt and 
	  d2_transaction_date <  '08FEB2017:00:00:00'dt and 
      substr(D2_CALL_TYPE_RAW,4,1) ne 'I'
/* and d2_agency in ('AR3') > Use cacs to validate agency instead of dialer attempts. */
;
quit;

/*Get agency for the accounts dialed yesterday in dialer attempts.*/

proc sql;
create table d2_agency_chk as select c13_acct_num
,c13_a3p_tp_id as agency
,c13_location_code as location_code

from odscacs.C13_CACS_P325PRI_DAILY
where c13_acct_num in (select distinct D2_ACCOUNT_NUMBER from d2_att)
and c13_processing_date = '06FEB2017:00:00:00'dt
and c13_location_code = '010101'
;
quit;

/* Limit attempts set to only those assigned to AR3*/

proc sql;
create table d2_att1 as select a.*
,b.agency

from d2_att as a
inner join d2_agency_chk as b on a.D2_ACCOUNT_NUMBER=b.c13_acct_num
;
quit;

/*Append dialer source description to autodials*/

proc sql;
create table autodial_src1 as select 'autodialer' as attempt_source
,*
from d2_att1
where substr(campaign,4,1) not in ('G','P')
;
quit;

/*Append dialer source description to manual dials*/

proc sql;
create table manual_src1 as select 'manual' as attempt_source
,*
from d2_att1
where substr(campaign,4,1) in ('G','P')
;
quit;

/*Append the autodial and manual dial id'd dials to one another*/

data dial_desc;
set autodial_src1 manual_src1;
run;

/* Get customer state and zip code data in CACS associated w/ accounts dialed via autodialer */

proc sql;
create table max_customer_stzip as select c14_acct_num
,c14_processing_date
,c14_customer_state
,c14_customer_zip_code
from odscacs.C14_CACS_P327EXT_DAILY
where c14_acct_num in (select distinct d2_account_number from dial_desc)
and c14_processing_date = '06FEB2017:00:00:00'dt
;
quit;

/*Unique Zip Codes and State codes*/

proc sql;
create table zip_state1 as select c14_customer_zip_code as customer_zip_code label='customer_zip_code'
,c14_customer_state as customer_state label='customer_state'
from max_customer_stzip
;
quit;

/*deduped list of zip code state to use for state lookups on zip code*/
proc sort data=zip_state1 nodupkey out=zip_state2;
by customer_zip_code;
run;

/* Append area code/state/zip from cacs to end of dialer attempts data */

proc sql;
create table dial_desc2 as select a.*
,substr(a.D2_PHONE_NUMBER,1,3) as area_code
,b.c14_customer_state as acct_state
,b.c14_customer_zip_code as customer_zip_code
from dial_desc as a left join 
max_customer_stzip as b on a.D2_ACCOUNT_NUMBER=b.c14_acct_num
and intnx('dtday',a.d2_transaction_date,-1,'B') = b.c14_processing_date
;
quit;

/* Pull from time zone table to assign time zone to numeric representations of the time difference from CST (based on area code)  */

proc sql;
create table dial_desc3 as select a.*
,b.areacode as areacode_from_score label='areacode_from_score'
,b.exchange as exchange_from_score label='exchange_from_score'
,b.state as area_cd_state label='area_cd_state'
,case when b.adjust_cst = -300 then 'SST'
when b.adjust_cst = -240 then 'HAST'
when b.adjust_cst = -180 then 'AKST'
when b.adjust_cst = -120 then 'PST'
when b.adjust_cst = -60 then 'MST'
when b.adjust_cst = 0 then 'CST'
when b.adjust_cst = 60 then 'EST'
when b.adjust_cst = 120 then 'ATL'
else 'nil' end as Area_Code_TZ
from dial_desc2 as a
left join score.d_areacode_ex_timezone as b
on a.area_code=b.areacode
and substr(a.d2_phone_number,4,3)=b.exchange
;
quit;

/* Pull from time zone table to assign time zone to numeric representations of the time difference from CST (based on zip code)  */
proc sql;
create table dial_desc4 as select a.*
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
from dial_desc3 as a
left join score.d_areacode_zip_timezone as b
on a.area_code=b.areacode
and substr(a.customer_zip_code,1,5)=b.zip_code
left join zip_state2 as c
on a.customer_zip_code=c.customer_zip_code
;
quit;

/*cacs area code/zip and score area code/zip/adjust cmt values are*/
/*aligned*/

/* This step converts the datetime_call_start in CST to accounts' local time based on the zip code and area code derived Time Zones */

proc sql;
create table dial_desc5 as select *
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
else timepart(d2_datetime_call_start) end as dt_strt_by_zip_cd format=time8. /* If timepart(datetime_call_start) not found then use local time */
from dial_desc4
/* from tz_cons_eval_data5 >> used this dataset for testing records only having data for both area code and zip code. */ 
;
quit;

/*Testing Locat Time Calculations*/

data test_cacs_v_score_tz(keep=d2_account_number outcome_cd 
long_desc campaign list d2_datetime_call_start d2_datetime_call_end
d2_phone_number areacode_from_score exchange_from_score Area_Code_TZ
customer_zip_code customer_state zip_cd_tz dt_strt_by_area_cd dt_strt_by_zip_cd
);
set dial_desc5;
where areacode_from_score = '252' and exchange_from_score = '398'
;
run;

data get_time1a(keep=d2_account_number d2_datetime_call_start
d2_phone_number areacode_from_score exchange_from_score 
area_code_tz zip_cd_tz dt_strt_by_area_cd dt_strt_by_zip_cd
get_hour get_minute get_second
);
set dial_desc5;
where areacode_from_score = '252' and exchange_from_score = '398'
;
get_hour = put(hour(timepart(d2_datetime_call_start))+1, z2.);
get_minute = put(minute(timepart(d2_datetime_call_start)),z2.);
get_second = put(second(timepart(d2_datetime_call_start)),z2.);
run;

data get_time1b(keep=d2_account_number d2_datetime_call_start
d2_phone_number areacode_from_score exchange_from_score 
area_code_tz zip_cd_tz dt_strt_by_area_cd dt_strt_by_zip_cd
get_hour get_minute get_second
);
set dial_desc5;
where areacode_from_score = '706' and exchange_from_score = '540'
;
get_hour = put(hour(timepart(d2_datetime_call_start))+1, z2.);
get_minute = put(minute(timepart(d2_datetime_call_start)),z2.);
get_second = put(second(timepart(d2_datetime_call_start)),z2.);
run;

data get_time_add;
set get_time1a get_time1b;
run;

PROC EXPORT DATA= get_time_add                                                                                                                                                                                                                                                                                                                                
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\get_time_add.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

/* Step below is what determines the most conservative time zone between local time derived from area code and zip code. */

proc sql;
create table dial_desc6 as select *
,case when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd < dt_strt_by_zip_cd then 'area_dt'
when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd > dt_strt_by_zip_cd then 'zip_dt' 
when dt_strt_by_area_cd = dt_strt_by_zip_cd then 'same' else 'nill' end as early_time_ind
,case when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd > dt_strt_by_zip_cd then 'area_dt'
when dt_strt_by_area_cd ne dt_strt_by_zip_cd and dt_strt_by_area_cd < dt_strt_by_zip_cd then 'zip_dt' 
when dt_strt_by_area_cd = dt_strt_by_zip_cd then 'same' else 'nill' end as late_time_ind
from dial_desc5
;
quit;

/* Step which assigns label determining whether area code or zip code should be used as the early time or late time indicator */

proc sql;
create table dial_desc7 as select *
,hour(timepart(d2_datetime_call_start)) as cst_attempt_hour
,datepart(d2_datetime_call_start) as datetime_datepart format=mmddyy8.
,case when hour(timepart(d2_datetime_call_start)) < 12 and early_time_ind = 'area_dt' then dt_strt_by_area_cd
when hour(timepart(d2_datetime_call_start)) < 12 and early_time_ind = 'zip_dt' then dt_strt_by_zip_cd
when hour(timepart(d2_datetime_call_start)) > 11 and late_time_ind = 'area_dt' then dt_strt_by_area_cd
when hour(timepart(d2_datetime_call_start)) > 11 and late_time_ind = 'zip_dt' then dt_strt_by_zip_cd 
else dt_strt_by_area_cd end as est_local_time format=time8.
from dial_desc6
;
quit;

/*  Step which applies logic to estimated local time for determining whether there was a time zone miss on dials by assigning 0 and 1 */

proc sql;
create table dial_desc8 as select *
,case when est_local_time < input('07:59:00',time8.) then 1 else 0 end as before_8AM_misses
,case when est_local_time > input('21:01:00',time8.) then 1 else 0 end as after_9PM_misses
,case when est_local_time >= input('07:59:00',time8.)
and est_local_time <= input('21:01:00',time8.) then 1 else 0 end as tz_compliant
from dial_desc7
;
quit;
