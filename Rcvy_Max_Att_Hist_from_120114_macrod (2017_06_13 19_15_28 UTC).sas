libname odscacs oracle user="GMALATHU" password="Ph03n1x246" path=prdBIDW schema=ods_cacs;
libname dwprd oracle user="GMALATHU" password="Ph03n1x246" path=prdBIDW schema=dwprd;
libname cxprd oracle user="GMALATHU" password="Ph03n1x246" path=prdBIDW schema=cxprd;
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

%macro rcvymaxatt(date_dt1,date_dt2,date_d);

/* Get inventory in AR5 */

proc sql;
create table AR5 as select c13_acct_num as acct_num label='acct_num'
,c13_referred_ind as referred_ind label='referred_ind'
,c13_a3p_tp_id as agency label='agency'
,c13_processing_date as processing_date label='processing_date'
from odscacs.C13_CACS_P325PRI_DAILY
where c13_processing_date = /*'24OCT2014:00:00:00'dt*/ &date_dt1
and c13_referred_ind in ('Y')
and compress(c13_a3p_tp_id) in ('AR5')
;
quit;

proc sql;
create table AR5_UNQ as select distinct acct_num 
,datepart(processing_date) as processing_date format=mmddyy10.
from AR5
;
quit;

/* Get dialer attempts made against accounts in AR5 inventory */

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
,input(put(hour(timepart(a.d2_datetime_call_start)),z2.)||':00',time5.) as hour_bucket format=time5.
,a.D2_PHONE_NUMBER
,a.D2_PHONE_TYPE
,a.D2_TIME_ZONE
,a.D2_TRANSACTION_DATE
,case when d2_contact_outcome_code not in ('00','01','02','03','04','05','06','11','20') then 1 else 0 end as cfpb_attempt
,case when d2_contact_outcome_code in ('31','32','33','34','35','36','37','38', '39','40')
            then 1 else 0 end as rpc
from odscacs.d2_dialer_attempts as a
left join cxprd.d_outcome_cd as b on a.D2_ATTEMPT_OUTCOME_CODE=b.outcome_cd
where d2_account_number in (select acct_num from AR5)
and d2_transaction_date = /*'24OCT2014:00:00:00'dt*/ &date_dt2
and substr(D2_CALL_TYPE_RAW,4,1) ne 'I'
order by d2_account_number, d2_datetime_call_start
;
quit;

/* Identify attempt list source as 'autodialer'  */

proc sql;
create table autodial_src as select *
,'autodialer' as attempt_source
from d2_att
;
quit;

/* Get customer state and zip code information */

proc sql;
create table max_customer_stzip1 as select c14_acct_num
,c14_processing_date
,c14_customer_state
,c14_customer_zip_code
from odscacs.C14_CACS_P327EXT_DAILY
where c14_acct_num in (select acct_num from AR5)
and c14_processing_date = /*'24OCT2014:00:00:00'dt */ &date_dt1
;
quit;

proc sql;
create table zip_state1a as select c14_customer_zip_code as customer_zip_code label='customer_zip_code'
,c14_customer_state as customer_state label='customer_state'
from max_customer_stzip1
;
quit;

proc sort data=zip_state1a nodupkey out=zip_state2a;
by customer_zip_code;
run;

/* Main set of auto dials to join up to. */

proc sql;
create table autodial_src1 as select a.*
,b.c14_customer_state as acct_state
,b.c14_customer_zip_code as customer_zip_code
from autodial_src as a left join 
max_customer_stzip1 as b on a.d2_account_number=b.c14_acct_num
and a.d2_transaction_date=b.c14_processing_date
;
quit;

/* Get manual attempts made against accounts in AR5 inventory*/

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
where datepart(call_start_datetime) = /* intnx('day',today(),-3) */ &date_d
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
where entry_date =  /* intnx('day',today(),-3) */ &date_d
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

data pbx_dfsmanual_join_a;
set pbx_dfsmanual_join;
if dfsman_account not in (' ');
run;

proc sql;
create table cust_stzip as select c14_acct_num
,c14_processing_date
,c14_customer_state
,c14_customer_zip_code
from odscacs.C14_CACS_P327EXT_DAILY
where c14_acct_num in (select distinct dfsman_account from pbx_dfsmanual_join_a)
and c14_processing_date = /* intnx('dtday',datetime(),-3) */ &date_dt1
;
quit;

proc sql;
create table dfsman_pbx_stzip as select a.*
,input(put(hour(timepart(a.pbx_dial_dt_start)),z2.)||':00',time5.) as hour_bucket format=time5.
,b.c14_customer_state as customer_state
,b.c14_customer_zip_code as customer_zip_code
from pbx_dfsmanual_join_a as a left join cust_stzip as b
on a.dfsman_account=b.c14_acct_num
;
quit;

/*Limit accounts in manual dial attempt list to accounts in 'AR5' set above*/
proc sql;
create table manual_src1 as select *
from dfsman_pbx_stzip where dfsman_account in (select acct_num from AR5)
;
quit;

data zip_TZ_lookup1(keep=areacode adjust_cst zip_code);
set score.d_areacode_zip_timezone;
run;

proc sort data=zip_TZ_lookup1 nodupkey out=zip_TZ_lookup2;
by areacode zip_code;
run;

proc sort data=zip_TZ_lookup2 nodupkey out=zip_TZ_lookup3;
by zip_code;
run;

proc sql;
create table manual_src2 as select a.*
,case when b.adjust_cst = -300 then 'SST'
when b.adjust_cst = -240 then 'HAST'
when b.adjust_cst = -180 then 'AKST'
when b.adjust_cst = -120 then 'PST'
when b.adjust_cst = -60 then 'MST'
when b.adjust_cst = 0 then 'CST'
when b.adjust_cst = 60 then 'EST'
when b.adjust_cst = 120 then 'ATL'
else 'nil' end as Zip_Code_TZ
from manual_src1 as a
left join zip_TZ_lookup2 as b
on substr(compress(a.dfsman_phone),1,3)=b.areacode
and substr(compress(a.customer_zip_code),1,5)=b.zip_code
;
quit;

proc sql;
create table manual_src3 as select a.*
,case when b.adjust_cst = -300 then 'SST'
when b.adjust_cst = -240 then 'HAST'
when b.adjust_cst = -180 then 'AKST'
when b.adjust_cst = -120 then 'PST'
when b.adjust_cst = -60 then 'MST'
when b.adjust_cst = 0 then 'CST'
when b.adjust_cst = 60 then 'EST'
when b.adjust_cst = 120 then 'ATL'
else 'nil' end as Zip_Code_TZ1
from manual_src2 as a
left join zip_TZ_lookup3 as b
on substr(a.customer_zip_code,1,5)=b.zip_code
;
quit;

proc sql;
create table manual_src4 as select pbx_dial_dt_start
,pbx_channel
,pbx_extension
,agent_name
,pbx_user_name
,pbx_phone_number
,pbx_disposition
,pbx_duration
,pbx_call_time_start
,pbx_call_time_end
,dfsman_entry_date
,dfsman_entry_time
,dfsman_account
,dfsman_phone
,dfsman_agent
,dfsman_state
,dfsman_status
,outcome_description
,dfsman_extension
,dfsman_call_time_start
,customer_state
,customer_zip_code
,hour_bucket
,case when Zip_Code_TZ not in ('nil') then Zip_Code_TZ else Zip_Code_TZ1 end as Zip_Code_TZ2
from manual_src3
;
quit;

/*autodial attempts on 10/24/14 is 6168 rows before filtering on cfpb attempts only */
/*after filtering on cfpb attempts, there are 4191 rows*/

proc sql;
create table autodial_col_arr as select 'autodial' as attempt_source label='attempt_source'
,D2_ACCOUNT_NUMBER as account_number label='account_number'
,D2_PHONE_NUMBER as phone_number label='phone_number'
,LONG_DESC as outcome label='outcome'
,D2_AGENCY as agency label='agency'
,D2_AGENT_ID_CACS as agent label='agent'
,campaign as campaign label='campaign'
,list as list label='list'
,datepart(D2_TRANSACTION_DATE) as attempt_date format=mmddyy10. label='attempt_date'
,D2_DATETIME_CALL_START as datetime_call_start format=datetime20. label='datetime_call_start'
,hour_bucket
,acct_state as customer_state label='customer_state'
,customer_zip_code as customer_zip label='customer_zip'
,D2_TIME_ZONE as Time_Zone label='Time_Zone'
from autodial_src1
where cfpb_attempt = 1
;
quit;

proc sql;
create table manual_col_arr as select 'manual' as attempt_source label='attempt_source'
,dfsman_account as account_number label='account_number'
,dfsman_phone as phone_number label='phone_number'
,outcome_description as outcome label='outcome'
,'AR5' as agency label='agency'
,pbx_extension as agent label='agent'
,'n/a' as campaign label='campaign'
,'n/a' as list label='list'
,datepart(pbx_dial_dt_start) as attempt_date format=mmddyy10. label='attempt_date'
,pbx_dial_dt_start as datetime_call_start label='datetime_call_start'
,hour_bucket
,customer_state as customer_state label='customer_state'
,customer_zip_code as customer_zip label='customer_zip'
,Zip_Code_TZ2 as Time_Zone label='Time_Zone'
from manual_src4
;
quit;

data attempts_join;
set autodial_col_arr manual_col_arr;
run;

proc sql;
create table attempt_seq as select *
from attempts_join
order by account_number, datetime_call_start
;
quit;

/* Create a master list of account numbers and phone numbers for obtaining phone type. */

proc sql; /* Set containing phone number ranking data.  */
create table phone_type as select acct_num||phone_number as acct_phone
,*
                  ,case when phone_type='PHN1CELL' then 1
             when phone_type='PHN3CELL' then 2
             when phone_type='PHN2CELL' then 3
                   when phone_type='PHN4CELL' then 4
                  when phone_type='phone2' then 5
                  when phone_type='phone1' then 6 
             when phone_type='phone3' then 7 
             when phone_type='phone4' then 8 
             when phone_type='phone5' then 9 
                   when phone_type='phone10' then 10 
                 when phone_type='opcell1' then 11
                 when phone_type='opcell2' then 12
             when phone_type='opcell3' then 13
             when phone_type='opcell4' then 14
             when phone_type='scell1' then 15
             when phone_type='scell2' then 16
             when phone_type='scell3'  then 17
                   when phone_type='scell4'  then 18
             when phone_type='scell5'  then 19
             when phone_type='scell6'  then 20
             when phone_type='scell7'  then 21
             when phone_type='scell8'  then 22
             when phone_type='scell9'  then 23
                  when phone_type='order1'  then 24
             when phone_type='order2'  then 25
             when phone_type='order3'  then 26
             when phone_type='order4'  then 27
                 when phone_type='skip1'  then 28
          when phone_type='skip2'  then 29
             when phone_type='skip3'  then 30
             when phone_type='skip4'  then 31
             when phone_type='skip5'  then 32
             when phone_type='skip6'  then 33
             when phone_type='skip7'  then 34
             when phone_type='skip8'  then 35
             when phone_type='skip9'  then 36 end as phone_type_rank
                  , /*today()-1 >> '24OCT2014'd*/ &date_d as join_date format=date9. /* GM060914, Since today is Monday, change today()-1 to today()-4 so it pulls back data.  */
            
from score.max_number_list
/*  where date=today()-1 >> GM060914, Since today is Monday, change today()-1 to today()-4 so it pulls back data.  */
where date= /*today()-1 >> '24OCT2014'd */ &date_d
;
quit;

proc sort data=phone_type;
by acct_num phone_number phone_type_rank;
run;
proc sort data=phone_type nodupkey out=phone_type1;
by acct_num phone_number;
run;

/* Second source of data to use for obtaining phone numbers and phone_types for accounts in AR5 inventory */
proc sql;
create table phone_type2 as select acct_num||phone_number as acct_phone
,*
from score.d_cellphone_id_file
where date = /*'24OCT2014'd*/ &date_d
order by acct_num, phone_number
;
quit;

/* next two steps perform a validation on phone numbers that switched types based on comparing max_number_list to score.d_cellphone_id_file */

proc sql;
create table ph_type_val1 as select a.acct_num
,a.phone_number
,a.phone_type
,a.date
,case when b.consent_code = 'C' and a.phone_type not in ('PHN1CELL','PHN2CELL','PHN3CELL','PHN4CELL','opcell1','opcell2','opcell3','opcell4','scell1','scell2','scell3','scell4',
'scell5','scell6','scell7','scell8','scell9') then 'CELL'
when b.consent_code = 'C' and a.phone_type in ('PHN1CELL','PHN2CELL','PHN3CELL','PHN4CELL','opcell1','opcell2','opcell3','opcell4','scell1','scell2','scell3','scell4',
'scell5','scell6','scell7','scell8','scell9') then a.phone_type
when b.consent_code in (' ') then a.phone_type else a.phone_type end as phone_type1 /*  Assigns 'true' phone type based on scrubbing against d_cellphone_id_file */
from phone_type1 as a
left join phone_type2 as b on a.acct_num=b.acct_num
and a.phone_number=b.phone_number
;
quit;

proc sql;
create table ph_type_val2 as select *
,case when phone_type1 in ('PHN1CELL','PHN2CELL','PHN3CELL','PHN4CELL','opcell1','opcell2','opcell3','opcell4','scell1','scell2','scell3','scell4',
'scell5','scell6','scell7','scell8','scell9','CELL') /* Condition in which phone numbers are 'True' Cell Numbers based on available data */
and phone_type not in ('PHN1CELL','PHN2CELL','PHN3CELL','PHN4CELL','opcell1','opcell2','opcell3','opcell4','scell1','scell2','scell3','scell4',
'scell5','scell6','scell7','scell8','scell9') then 1 else 0 end as switch
from ph_type_val1
;
quit;

/* Need to append and dedupe account numbers, phone_numbers & phone_types between max_number_list and d_cellphone_id_file. 
From phone_type(max_number_list) append acct_num, phone_number & phone_type where switch=0 to set 'A' (accounts that didn't flip phone_types
Then append acct_num, phone_number & phone_type where switch='1' to set 'A' (accounts that flipped phone_types)
Then from phone_type2(d_cellphone_id_file) append acct_num, phone_number and phone_type where acct_num & phone_number not in max_number_list.
*/

/* Insert accounts/phone numbers that are cell numbers based on scrubbing max_number_list phone types against score.d_cellphone_id_file*/

proc sql;
create table phone_type3a as select acct_num
,phone_number
,date
,phone_type1
from ph_type_val2
where switch = 1
;
quit;

/* Append accounts and phone numbers which did not fit 'switch' criteria/condition*/

proc sql;
create table phone_type3b as select acct_num
,phone_number
,date
,phone_type1
from ph_type_val2
where switch = 0
;
quit;

proc sql;
create table phone_type3c as select a.acct_num
,a.phone_number
,a.date
,case when a.consent_code in ('C') then 'CELL'
when a.consent_code in ('L') then 'LANDLINE' else 'nil' end as phone_type1
from phone_type2 as a
left join phone_type1 as b on a.acct_phone=b.acct_phone
where b.acct_phone in (' ')
order by acct_num, phone_number
;
quit;

/* Master table containing phone_types for all account numbers and phone numbers in AR5 inventory.*/

data phone_type_mstr;
set phone_type3a phone_type3b phone_type3c;
run;

proc sql;
create table att_ph_typ as select a.*
,case when b.phone_type1 not in (' ') then b.phone_type1 else 'UNVERIFIEDSKIP' end as phone_type1
from attempt_seq as a
left join phone_type_mstr as b on a.account_number=b.acct_num
and a.phone_number=b.phone_number
;
quit;

proc sql;
create table unver_skip1 as select account_number||phone_number as acct_phone
,*
from att_ph_typ
where phone_type1 in ('UNVERIFIEDSKIP')
;
quit;

/*Detail on any unverified skip numbers dialed more than twice*/

proc sql;
create table unver_skip2 as select acct_phone
,count(datetime_call_start) as attempt_count
from unver_skip1
group by acct_phone
having count(datetime_call_start) > 2
;
quit;

data score.rcvy_max_att_unver_skip_from_120114;
set unver_skip2;
run;

/*
proc append base=score.rcvy_max_att_unver_skip_from_120114 data=unver_skip2;
run;
*/

proc sql;
create table att_ph_typ1 as select *
,case when phone_type1 in ('phone1','phone3','phone4','phone5') then 1 else 0 end as Home
,case when phone_type1 in ('PHN1CELL','PHN2CELL','PHN3CELL','PHN4CELL','opcell1','opcell2','opcell3','opcell4','scell1','scell2','scell3','scell4',
'scell5','scell6','scell7','scell8','scell9','CELL') then 1 else 0 end as Cell
,case when phone_type1 in ('phone2') then 1 else 0 end as Work
,case when phone_type1 in ('UNVERIFIEDSKIP') then 1 else 0 end as Skip_Unverified
,case when phone_type1 not in ('phone1','phone3','phone4','phone5','PHN1CELL','PHN2CELL','PHN3CELL','PHN4CELL','opcell1','opcell2','opcell3','opcell4','scell1','scell2','scell3','scell4',
'scell5','scell6','scell7','scell8','scell9','CELL','phone2','UNVERIFIEDSKIP') then 1 else 0 end as Other
from att_ph_typ
;
quit;

proc sql;
create table agg1 as select account_number
,sum(home) as home_attempts
,sum(cell) as cell_attempts
,sum(work) as work_attempts
,sum(other) as other_attempts
,sum(home)+sum(cell)+sum(work)+sum(other) as acct_attempts
from att_ph_typ1
group by account_number
;
quit;

proc sql;
create table agg1_a as select account_number
,phone_number
,sum(home) as home_attempts
,sum(cell) as cell_attempts
,sum(work) as work_attempts
,sum(other) as other_attempts
from att_ph_typ1
group by account_number, phone_number
;
quit;

proc sql;
create table agg2 as select b.*
,case when a.acct_attempts > 7 then 1 /* OK */
when a.acct_attempts <= 7 and (b.home_attempts > 4 or b.cell_attempts > 2 or b.work_attempts > 1) then 1 /* GM112314, scenario may meet this condition and 2nd when condition in step below at same time. */
else 0 end as over

,case when a.acct_attempts = 7 and (b.home_attempts = 4 and b.cell_attempts = 2 and b.work_attempts = 1) then 1 /* valid scenario  */
when a.acct_attempts = 7 and (b.home_attempts <= 4 and b.cell_attempts <= 2 and b.work_attempts <= 1) then 1 /* unlikely scenario.but accounted for where acct_att=7 (can be excluded) */
when a.acct_attempts < 7 and (b.home_attempts = 4 or b.cell_attempts = 2 or b.work_attempts = 1) then 1
else 0 end as maxed

,case when a.acct_attempts < 7 and (b.home_attempts < 4 and b.cell_attempts < 2 and b.work_attempts < 1) then 1
else 0 end as under

from agg1 as a /* account level  */
left join agg1_a as b on a.account_number=b.account_number /*account and phone number level*/
;
quit;


/*Step which corrects the issue w/ accounts showing both over and maxed. 'Over' trumps 'Maxed' */
/* GM112314 - Revisit this step...  */

proc sql;
create table over_max_accts as select distinct account_number
from agg2
where over > 0
;
quit;

proc sql;
create table over_max_detail as select b.*
from over_max_accts as a
left join att_ph_typ1 as b on a.account_number=b.account_number
order by account_number, datetime_call_start
;
quit;

data over_max_dials1a;
set over_max_detail;
by account_number;
if first. account_number then n=1;
else n+1;
run;

proc sql;
create table over_max_dials1b as select '_'||account_number as account_number
,attempt_source
,phone_number
,outcome
,agency
,agent
,campaign
,list
,attempt_date
,datetime_call_start
,hour_bucket
,customer_state
,customer_zip
,Time_Zone
,phone_type1
,Home
,Cell
,Work
,Skip_Unverified
,Other
,n
from over_max_dials1a
;
quit;

/*
ods listing CLOSE;
ods html file=&outfile2;
proc print data=over_max_dials1b;
run;
ods html close;
ods listing;
*/

data score.rcvy_max_att_overmax_from_120114;
set over_max_dials1b;
run;

/*
proc append base=score.rcvy_max_att_overmax_from_120114 data=over_max_dials1b;
run;
*/

proc sql;
create table agg3 as select account_number
,sum(over)+sum(maxed)+sum(under) as sum_types
from agg2
group by account_number
;
quit;

proc sql;
create table agg2b as select a.acct_num
,a.processing_date
,1 as AR5_Inventory
,case when b.account_number not in (' ') then 1 else 0 end as att_on_AR5_inv
,b.*
from AR5_UNQ as a
left join agg2 as b on a.acct_num=b.account_number
;
quit;

proc sql;
create table agg2c as select processing_date
,sum(AR5_Inventory) as AR5_Inventory
,sum(att_on_AR5_Inv) as AR5_Attempts
,sum(under) as Not_Maxed
,sum(maxed) as Maxed
,sum(over) as Over_Maxed
from agg2b
group by processing_date
;
quit;

/*
options orientation=landscape
		papersize=letter;

ods pdf file=&outfile3
style=minimal;

proc print data=agg2c NOOBS;
 title 'Recovery Max Attempts Summary ';
run;

ods pdf close;
*/

data score.rcvy_max_att_summary_from_120114;
set agg2c;
run;

/*
proc append base=score.rcvy_max_att_summary_from_120114 data=agg2c;
run;
*/

%mend;

%rcvymaxatt('01DEC2014:00:00:00'dt,'01DEC2014:00:00:00'dt,'01DEC2014'd);
%rcvymaxatt('02DEC2014:00:00:00'dt,'02DEC2014:00:00:00'dt,'02DEC2014'd);
%rcvymaxatt('03DEC2014:00:00:00'dt,'03DEC2014:00:00:00'dt,'03DEC2014'd);
%rcvymaxatt('04DEC2014:00:00:00'dt,'04DEC2014:00:00:00'dt,'04DEC2014'd);
%rcvymaxatt('05DEC2014:00:00:00'dt,'05DEC2014:00:00:00'dt,'05DEC2014'd);
%rcvymaxatt('06DEC2014:00:00:00'dt,'06DEC2014:00:00:00'dt,'06DEC2014'd);
%rcvymaxatt('08DEC2014:00:00:00'dt,'08DEC2014:00:00:00'dt,'08DEC2014'd);
%rcvymaxatt('09DEC2014:00:00:00'dt,'09DEC2014:00:00:00'dt,'09DEC2014'd);
%rcvymaxatt('10DEC2014:00:00:00'dt,'10DEC2014:00:00:00'dt,'10DEC2014'd);
%rcvymaxatt('11DEC2014:00:00:00'dt,'11DEC2014:00:00:00'dt,'11DEC2014'd);
%rcvymaxatt('12DEC2014:00:00:00'dt,'12DEC2014:00:00:00'dt,'12DEC2014'd);
%rcvymaxatt('13DEC2014:00:00:00'dt,'13DEC2014:00:00:00'dt,'13DEC2014'd);
%rcvymaxatt('15DEC2014:00:00:00'dt,'15DEC2014:00:00:00'dt,'15DEC2014'd);
%rcvymaxatt('16DEC2014:00:00:00'dt,'16DEC2014:00:00:00'dt,'16DEC2014'd);
%rcvymaxatt('17DEC2014:00:00:00'dt,'17DEC2014:00:00:00'dt,'17DEC2014'd);
%rcvymaxatt('18DEC2014:00:00:00'dt,'18DEC2014:00:00:00'dt,'18DEC2014'd);
%rcvymaxatt('19DEC2014:00:00:00'dt,'19DEC2014:00:00:00'dt,'19DEC2014'd);
%rcvymaxatt('20DEC2014:00:00:00'dt,'20DEC2014:00:00:00'dt,'20DEC2014'd);
