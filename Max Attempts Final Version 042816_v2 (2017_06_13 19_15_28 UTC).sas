libname odscacs oracle user="GMALATHU" password="Ph03n1x223500" path=prdBIDW schema=ods_cacs;
libname dwprd oracle user="GMALATHU" password="Ph03n1x223500" path=prdBIDW schema=dwprd;
libname cxprd oracle user="GMALATHU" password="Ph03n1x223500" path=prdBIDW schema=cxprd;
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

%macro max(date_dt1,date_dt2,date_d);

/*Get Accounts Queued*/

proc sql;
create table accts_queued as select D1_ACCOUNT_NUMBER
            ,D1_CAMPAIGN_ID
            ,D1_PROCESSING_DATE
            ,datepart(D1_PROCESSING_DATE) as processing_date format=date9.
            
            
from odscacs.D1_DIALER_LIST
                              where D1_PROCESSING_DATE = &date_dt1
                              and compress(D1_CAMPAIGN_ID) in ('6199','3199','16199')
                              ;
                              quit;

proc sql;
create table cust_state as select c14_acct_num
			,C14_location_code
            ,C14_CUSTOMER_STATE
            ,C14_CUSTOMER_ZIP_CODE
            ,c14_processing_date
            ,datepart(c14_processing_date) as processing_dt format=date9.

from odscacs.c14_cacs_p327ext_daily
where c14_acct_num in (select distinct D1_ACCOUNT_NUMBER from accts_queued
                                                                        )
and c14_processing_date = &date_dt2
and C14_location_code = '010101'

order by c14_acct_num, c14_processing_date;
quit;

proc sql;
create table queued_agency as select c13_acct_num
,c13_a3p_tp_id

from odscacs.c13_cacs_p325pri_daily
where c13_acct_num in (select distinct D1_ACCOUNT_NUMBER from accts_queued
                                                                        )
and c13_processing_date = &date_dt2
/*and c13_location_code = '010101'*/
;
quit;

proc sql;
create table test1 as select c13_a3p_tp_id
,count(c13_acct_num) as count_accounts

from queued_agency
group by c13_a3p_tp_id
;
quit;

/*Get autodials data*/

proc sql;
create table d2_att as select a.D2_ACCOUNT_NUMBER
,a.D2_AGENT_ID_CACS
,a.D2_ATTEMPT_OUTCOME_CODE
,b.OUTCOME_CD
,b.LONG_DESC
,a.D2_ATTEMPT_OUTCOME_CODE_RAW
,a.D2_CALL_ID /* should be unique */
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
,a.D2_SYS_LOAD_DTM
,case when d2_contact_outcome_code not in ('00','01','02','03','04','06','11','20') then 1 else 0 end as cfpb_attempt
,case when d2_contact_outcome_code in ('31','32','33','34','35','36','37','38', '39','40')
            then 1 else 0 end as rpc
,'autodialer' as attempt_source
from odscacs.d2_dialer_attempts as a
left join cxprd.d_outcome_cd as b on a.D2_ATTEMPT_OUTCOME_CODE=b.outcome_cd
where d2_transaction_date = &date_dt1
and substr(D2_CALL_TYPE_RAW,4,1) ne 'I'
and D2_CONTACT_OUTCOME_CODE not in (' ')
and D2_ATTEMPT_OUTCOME_CODE_RAW not in ('LM','TR')
order by d2_account_number, d2_datetime_call_start
;
quit;

proc sql;
create table autodial_agcy as select c13_acct_num as acct_num label='acct_num'
,c13_a3p_tp_id as agency label='agency'

from odscacs.c13_cacs_p325pri_daily
where c13_acct_num in (select distinct D2_ACCOUNT_NUMBER from d2_att)
and c13_processing_date = &date_dt2
and compress(c13_a3p_tp_id) in ('ARA','AR6')
;
quit;

/*Get manual dials data, and filter both autodials and manual dials data to just the fields */
/*required for the remainder of reporting requirement
Rows 158 - 495 in 092815_v2 code (nearly 340 lines)
*/

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
when b.status = 'PTP' then 'Promise to Pay'
when b.status = 'RCB' then 'Request Callback'
when b.status = 'SPY' then 'Speed Pay'
when b.status = 'MSG' then 'Message'
when b.status = 'SKP' then 'Skip'
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

data pbx_dfsmanual_join_b;
set pbx_dfsmanual_join;
if dfsman_account not in (' ');
run;

proc sql;
create table pbx_agency_a as select c13_acct_num as acct_num label='acct_num'
,c13_a3p_tp_id as agency label='agency'
from odscacs.C13_CACS_P325PRI_DAILY
where c13_acct_num in (select dfsman_account from pbx_dfsmanual_join_b)
and c13_processing_date = &date_dt2
;
quit;

proc sql;
create table pbx_agency_b as select c19_acct_num as acct_num label='acct_num'
,c19_a3p_tp_id as agency label='agency'
from odscacs.C19_CACS_P326PRI_DAILY
where c19_acct_num in (select dfsman_account from pbx_dfsmanual_join_b)
and c19_processing_date = &date_dt2
;
quit;

data pbx_agency;
set pbx_agency_a pbx_agency_b;
run;

proc sql;
create table pbx_dfsmanual_join_a as select a.*
,b.agency
from pbx_dfsmanual_join_b as a
left join pbx_agency as b on a.dfsman_account=b.acct_num
where b.agency in ('ARA','AR6')
;
quit;

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
create table dfsman_pbx_stzip as select a.*
,input(put(hour(timepart(a.pbx_dial_dt_start)),z2.)||':00',time5.) as hour_bucket format=time5.
,b.c14_customer_state as customer_state
,b.c14_customer_zip_code as customer_zip_code
from pbx_dfsmanual_join_a as a left join cust_stzip as b
on a.dfsman_account=b.c14_acct_num
where compress(b.c14_customer_state) not in ('ID','MA','SC','WA','WV','PA')
or substr(b.c14_customer_zip_code,1,3) not in ('100','101','102','103','104','111','112','113','114','116')
or substr(b.c14_customer_zip_code,1,5) not in ('11004','11005')
;
quit;

/*Limit accounts in manual dial attempt list to accounts in ARA/AR6 set above*/
/* GM123014, step below was neutralized due to a proxy step further up the code: 'pbx_dfsmanual_join_a'  */
proc sql;
create table manual_src1 as select *
from dfsman_pbx_stzip
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
create table manual_src4 as select 'manual' as attempt_source
,pbx_dial_dt_start
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
,agency
,customer_state
,customer_zip_code
,hour_bucket
,case when Zip_Code_TZ not in ('nil') then Zip_Code_TZ else Zip_Code_TZ1 end as Zip_Code_TZ2
from manual_src3
;
quit;

/*Next step is to to align the autodialed and manual dialed attempts. The summary in order to be effective for*/
/*max attempts reporting should account for all attempts made to consumer accounts (whether or not they appear */
/*in the list of accounts to be worked that day(d1_dialer_list)*/

/*D1(Accts Queued) ---> d2_att*/
/*D1(Accts Queued) ---> manual_src4*/

proc sql;
create table accts_q_autodial as select b.attempt_source
,a.d1_account_number
/*,c.agency*/
,b.D2_AGENT_ID_CACS
,b.D2_ATTEMPT_OUTCOME_CODE
,b.OUTCOME_CD
,b.LONG_DESC
,b.D2_ATTEMPT_OUTCOME_CODE_RAW
,b.D2_CALL_ID
,b.D2_CALL_LIST_ID
,b.D2_CALL_TYPE
,b.campaign
,b.list
,b.D2_CONNECT_TIME
,b.D2_CONTACT_OUTCOME_CODE
,b.D2_CONTACT_OUTCOME_CODE_RAW
,b.D2_DATETIME_CALL_END
,b.D2_DATETIME_CALL_START
,b.hour_bucket
,b.D2_PHONE_NUMBER
,b.D2_PHONE_TYPE
,b.D2_TIME_ZONE
,b.D2_TRANSACTION_DATE
,b.cfpb_attempt
,b.rpc + case when b.cfpb_attempt = 1 and b.D2_CONTACT_OUTCOME_CODE in ('23') then 1 else 0 end as contact
,case when b.cfpb_attempt = 1 and b.D2_CONTACT_OUTCOME_CODE in ('23') then 1 else 0 end as third_party
,c.C14_CUSTOMER_STATE
,c.C14_CUSTOMER_ZIP_CODE

from accts_queued as a
inner join d2_att as b on a.d1_account_number=b.d2_account_number
inner join cust_state as c on a.d1_account_number=c.c14_acct_num
/*left join accts_q_agcy as c on a.d1_account_number=c.acct_num*/
where (compress(c.C14_CUSTOMER_STATE) not in ('ID','MA','SC','WA','WV','PA') or
substr(C14_CUSTOMER_ZIP_CODE,1,3) not in ('100','101','102','103','104','111','112','113','114','116')or
substr(C14_CUSTOMER_ZIP_CODE,1,5) not in ('11004','11005'))
;
quit;

proc sql;
create table accts_q_manual as select b.attempt_source
,a.d1_account_number
,b.pbx_dial_dt_start
,b.pbx_channel
,b.pbx_extension
,b.agent_name
,b.pbx_user_name
,b.pbx_phone_number
,b.pbx_disposition
,b.pbx_duration
,b.pbx_call_time_start
,b.pbx_call_time_end
,b.dfsman_entry_date
,b.dfsman_entry_time
,b.dfsman_account
,b.dfsman_phone
,b.dfsman_agent
,b.dfsman_state
,b.dfsman_status
,b.outcome_description
,b.dfsman_extension
,b.dfsman_call_time_start
,b.customer_state
,b.customer_zip_code
,b.hour_bucket
,b.Zip_Code_TZ2

from accts_queued as a
inner join manual_src4 as b on a.d1_account_number=b.dfsman_account
;
quit;

proc sql;
create table autodial_col_arr as select attempt_source
,D1_ACCOUNT_NUMBER as account_number label='account_number'
,D2_PHONE_NUMBER as phone_number label='phone_number'
,LONG_DESC as outcome label='outcome'
,D2_AGENT_ID_CACS as agent label='agent'
,campaign
,list
,datepart(D2_TRANSACTION_DATE) as attempt_date format=mmddyy10. label='attempt_date'
,D2_DATETIME_CALL_START as datetime_call_start format=datetime20. label='datetime_call_start'
,hour_bucket
,D2_TIME_ZONE as Time_Zone label='Time_Zone'
/*,rpc as rpc_ind*/
,cfpb_attempt
,contact
/*,third_party*/

from accts_q_autodial
where cfpb_attempt = 1
;
quit;

proc sql;
create table manual_col_arr as select attempt_source
,dfsman_account as account_number label='account_number'
,dfsman_phone as phone_number label='phone_number'
,outcome_description as outcome label='outcome'
,pbx_extension as agent label='agent'
,'n/a' as campaign label='campaign'
,'n/a' as list label='list'
,datepart(pbx_dial_dt_start) as attempt_date format=mmddyy10. label='attempt_date'
,pbx_dial_dt_start as datetime_call_start label='datetime_call_start'
,hour_bucket
,Zip_Code_TZ2 as Time_Zone label='Time_Zone'
,1 as cfpb_attempt
,case when outcome_description = 'Right Party Contact' then 1
when outcome_description = 'Third Party Contact' then 1
when outcome_description = 'No Connect' then 1
when outcome_description = 'Other Party Contact' then 1
when outcome_description = 'Wrong Party Contact' then 1
when outcome_description = 'Promise to Pay' then 1
when outcome_description = 'Request Callback' then 1
when outcome_description = 'Speed Pay' then 1
when outcome_description = 'Message' then 1
when outcome_description = 'Skip' then 0 else 0 end as contact

from accts_q_manual
;
quit;

data attempts_join;
set autodial_col_arr manual_col_arr;
run;

proc sort data=attempts_join nodupkey;
by ACCOUNT_NUMBER PHONE_NUMBER DATETIME_CALL_START;
run;

/*Next step is to get the distinct list of accounts dialed and join it to the phone type master to find out how many numbers are*/
/*associated with the accounts that were dialed.*/

proc sql;
create table accts_dialed as select distinct account_number from attempts_join
;
quit;

proc sql;
create table cons_accts_dialed as select distinct c13_acct_num as account_number
,c13_location_code
,c13_a3p_tp_id

from odscacs.C13_CACS_P325PRI_DAILY
where c13_acct_num in (select distinct account_number from attempts_join)
and c13_processing_date = &date_dt2
and c13_location_code = '010101'
;
quit;

/* Phone type master code */

proc sql;
create table dem as select ACCT_NUM
,DEMOG_STATUS
,NAME_RELATIONSHIP
,ADDR_RELATIONSHIP
,DEM_TELEPHONE_1
,substr(compress(DEM_TELEPHONE_1),1,12) as phone1
,DEM_TELEPHONE_2
,substr(compress(DEM_TELEPHONE_2),1,12) as phone2
,DEM_TELEPHONE_3
,substr(compress(DEM_TELEPHONE_3),1,12) as phone3
,case when DEMOG_CHG_DATE>0 then input(put(DEMOG_CHG_DATE,z6.),yymmdd6.) end as
            DEMOG_CHG_DATE format=date9.
,DATE_LAST_UPDATED as DATE_LAST_UPDATED1
,case when DATE_LAST_UPDATED>0 then input(put(DATE_LAST_UPDATED,z6.),yymmdd6.) end as
            DATE_LAST_UPDATED2 format=date9.
,DATE_UPDATED

from odscacs.p325dem
where acct_num in (select account_number from cons_accts_dialed)
and DEMOG_STATUS='C'
and ADDR_RELATIONSHIP=1
order by ACCT_NUM;
quit;

data dema; set dem;
by acct_num;
if first.acct_num then c=1;
else c+1;
run;
data dem1 dem2 dem3;set dema;
if c=1 then output dem1;
else if c=2 then output dem2;
else if c=3 then output dem3;
run;

proc transpose data=dem1 out=dem1a (rename=(col1=phone_number )) ;
  by ACCT_NUM;
  var phone1-phone3;

run;
proc transpose data=dem2 out=dem2a (rename=(col1=phone_number )) ;
  by ACCT_NUM;
  var phone1-phone3;

run;
proc transpose data=dem3 out=dem3a (rename=(col1=phone_number )) ;
  by ACCT_NUM;
  var phone1-phone3;

run;
data all_dem ; set dem1a dem2a dem3a;
if phone_number=' ' then delete;
phone_type=substr(phone_number,1,1);
phone_consent=substr(phone_number,2,1);
phone=compress(substr(phone_number,3,10));
/*if phone_consent not in ('Y') then delete;*/
run;
proc sort data=all_dem nodupkey;
by acct_num phone;
run;
data all_dem1; set all_dem;
where length(phone)=10 
and substr(phone,1,3) not in ('000','111','222','333','444','555','666','777','999');
run;

proc sql;
create table all_dem_final as select acct_num
,phone
,_name_ as phone_field label='phone_field'
,phone_type
,case when phone_type = 'B' then 'work'
when phone_type = 'C' then 'home'
when phone_type = 'D' then 'cell'
when phone_type = 'E' then 'home'
when phone_type = 'F' then 'cell'
when phone_type = 'G' then 'work'
when phone_type = 'H' then 'home'
when phone_type = 'I' then 'cell'
when phone_type = 'J' then 'home'
when phone_type = 'P' then 'home' else 'skip' end as desc

from all_dem1
;
quit;

proc sql;
create table phone_type_maxnum as select *
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
             when phone_type='skip9'  then 36 else 37 end as phone_type_rank
                  ,today()-1 as join_date format=date9.
            
from score.max_number_list
where date= &date_d
;
quit;

proc sort data=phone_type_maxnum;
by acct_num phone_number phone_type_rank;
run;

/* Deduped list of accounts and phone numbers based on rank.. */
proc sort data=phone_type_maxnum nodupkey out=phone_type_maxnum1;
by acct_num phone_number;
run;

proc sql;
create table neustar as select *
from odscacs.N1_NEUSTAR_TCPA_OUTPUT
;
quit;

data neustar1a(keep=acct_phone n1_account_number N1_PHONE_NUMBER N1_RESULT_CODE N1_PHONE_VALIDATION N1_PHONE_TYPE N1_PHONE_IN_SERVICE N1_PREPAID_PHONE N1_DO_NOT_CALL N1_PROCESSING_DATE);
set neustar;
acct_phone=n1_account_number||N1_PHONE_NUMBER;
if n1_phone_number not in (' ');
if N1_result_code not in ('0');
run;

data neustar_join;
set neustar1a score.neustar_gap_1012;
run;

proc sort data=neustar_join out=neustar2;
by acct_phone descending N1_PROCESSING_DATE;
run;

proc sort data=neustar2 out=neustar3 nodupkey;
by acct_phone;
run;

proc sql;
create table cellphone_id as select acct_num||phone_number as acct_phone
,*
from score.d_cellphone_id_file
where date = '22OCT2015'd /*intnx('day',today(),-1) */
order by acct_num, phone_number
;
quit;

proc sql;
create table phone_type_mstr as select a.acct_num
,a.phone_number
,a.date
,b.n1_result_code
,b.n1_phone_type
,b.n1_phone_in_service
,b.n1_prepaid_phone
,b.n1_do_not_call
,c.consent_code as Innovis_consent_cd

,case when b.N1_PHONE_NUMBER is null and upcase(a.phone_type) in ('PHN1CELL','PHN2CELL','PHN3CELL','PHN4CELL','OPCELL1','OPCELL2','OPCELL3','OPCELL4') then 'cell'
when b.N1_PHONE_NUMBER is null and upcase(phone_type) in ('PHONE1','PHONE3','PHONE4','PHONE5') then 'home'
when b.N1_PHONE_NUMBER is null and (upcase(phone_type) = 'PHONE2' or upcase(phone_type) in ('ORDER1','ORDER2','ORDER3','ORDER4','SKIP1','SKIP2','SKIP3','SKIP4','SKIP5','SKIP6','SKIP7','SKIP8','SKIP9')) then 'work/skip'
when b.N1_PHONE_NUMBER is not null and b.N1_RESULT_CODE = 'L' and upcase(a.phone_type) = 'PHONE2' then 'work/skip'
when b.N1_PHONE_NUMBER is not null and b.N1_RESULT_CODE = 'L' and upcase(a.phone_type) ne 'PHONE2' then 'home'
when b.N1_PHONE_NUMBER is not null and b.N1_RESULT_CODE in('1','2','3','5','7','10') then 'cell' else 'work/skip' end as phone_type

from phone_type_maxnum1 as a
left join neustar3 as b on a.acct_num=b.N1_ACCOUNT_NUMBER and
a.phone_number=b.N1_PHONE_NUMBER
left join cellphone_id as c on a.acct_num=c.acct_num and a.phone_number=c.phone_number
where substr(a.phone_number,1,3) not in ('000','111','222','333','444','555','666','777','999','100') and
length(a.phone_number) = 10
order by a.acct_num, a.phone_number
;
quit;

/*This set will be used to determine the max number of dials available on each account based on the phone numbers available to*/
/*dial on each account. If the max number of dials based on available numbers > 7, then set the max dials on the account to 7*/
/*otherwise set the max dials to the sum of max dials on the account.*/

proc sql;
create table dialed_acct_phnum1a as select b.account_number as acct_num
,b.phone_number
,b.phone_type
,case when b.phone_type = 'home' then 4
when b.phone_type = 'cell' then 2
when b.phone_type in ('work','skip') then 1 else 1 end as max_dials

from cons_accts_dialed as a
left join phone_type_mstr_alt as b on a.account_number=b.account_number
order by acct_num
;
quit;

proc sql;
create table acct_max_dials as select acct_num
,case when sum(max_dials) > 7 then 7 else sum(max_dials) end as max_dials_acct

from dialed_acct_phnum1a
group by acct_num
;
quit;

/*This set appends phone type to the attempts join set. Next step is to sum the cfpb attempts by phone type which will segway into*/
/*the final steps which will determine whether the account was maxed, not maxed or over the max. remember the other criteria for */
/*maxed out including an attempt having an rpc or 3rd party contact.*/

proc sql;
create table acct_phtyp_att1a as select a.*
,c.phone_type
,c.n1_result_code
,c.n1_phone_type
,c.n1_phone_in_service
,c.n1_prepaid_phone
,c.n1_do_not_call
,c.Innovis_consent_cd

from attempts_join as a
inner join cons_accts_dialed as b on a.account_number=b.account_number
inner join phone_type_mstr_alt as c on a.account_number=c.account_number
and a.phone_number=c.phone_number
;
quit;

proc sql;
create table acct_phtyp_att1b as select account_number
,phone_number
,phone_type
,sum(cfpb_attempt) as sum_attempts
,sum(contact) as sum_contacts

from acct_phtyp_att1a
group by account_number, phone_number, phone_type
order by account_number, phone_number
;
quit;

proc transpose data=acct_phtyp_att1b out=acct_phtyp_att1c prefix=phtyp;
by account_number phone_number;
id phone_type;
var sum_attempts;
run;

data acct_phtyp_att2c;
set acct_phtyp_att1c;

array c(*) _numeric_;

do i=1 to dim(c);
if c(i) = . then c(i) = 0;
end;
drop i; 
run;

proc sql;
create table acct_phtyp_att1d as select account_number
,phone_number
,sum(sum_contacts) as sum_contacts

from acct_phtyp_att1b
group by account_number, phone_number
order by account_number, phone_number
;
quit;

/*This is the final  step!!!*/
/*(if phtyphome <= 4 and phtypcell <=2 and phtypwork <=1 and phtypskip <= 1 (all phone types have dials up to the phone type max)*/
/*and phtyphome + phtypcell + phtypwork + phtypskip = max_dials_acct) or*/
/*(if phtyphome <= 4 and phtypcell <=2 and phtypwork <=1 and phtypskip <= 1 (all phone types have dials up to the phone type max)*/
/*and phtyphome + phtypcell + phtypwork + phtypskip < max_dials_acct) and*/
/*(sum_rpc > 0 or sum_3p > 0) then acct_maxed=1 else acct_maxed=0*/
/**/
/*(if phtyphome > 4 or phtypcell > 2 or phtypwork > 1 or phtypskip > 1) or*/
/*(if phtyphome + phtypcell + phtypwork + phtypskip > max_dials_acct) or */
/*(if phtyphome <= 4 and phtypcell <=2 and phtypwork <=1 and phtypskip <= 1 (all phone types have dials up to the phone type max)*/
/*and phtyphome + phtypcell + phtypwork + phtypskip < max_dials_acct) and*/
/*(sum_rpc > 1 or sum_3p > 1) then acct_over_max=1 else acct_over_max=0*/
/**/
/*(if phtyphome <= 4 and phtypcell <=2 and phtypwork <=1 and phtypskip <= 1 (all phone types have dials up to the phone type max)*/
/*and phtyphome + phtypcell + phtypwork + phtypskip < max_dials_acct) and */
/*(sum_rpc = 0 and sum_3p = 0) then acct_not_maxed=1 else acct_not_maxed=0*/

proc sql;
create table maxed as select b.*
,a.max_dials_acct
,c.sum_contacts
,d.attempt_date
,1 as dialed
,case when ((b.phtyphome <=4 and b.phtypcell <=2 and b.phtypwork <=1 and b.phtypskip <=1) and
(b.phtyphome + b.phtypcell + b.phtypwork + b.phtypskip = a.max_dials_acct)) or
((b.phtyphome <=4 and b.phtypcell <=2 and b.phtypwork <=1 and b.phtypskip <=1) and
(b.phtyphome + b.phtypcell + b.phtypwork + b.phtypskip < a.max_dials_acct) and
(c.sum_contacts = 1)) then 1 else 0 end as phnum_maxed

,case when (b.phtyphome > 4 or b.phtypcell > 2 or b.phtypwork > 1 or b.phtypskip > 1) or
(b.phtyphome + b.phtypcell + b.phtypwork + b.phtypskip > a.max_dials_acct) or
((b.phtyphome <= 4 and b.phtypcell <=2 and b.phtypwork <=1 and b.phtypskip <=1) and
(b.phtyphome + b.phtypcell + b.phtypwork + b.phtypskip < a.max_dials_acct) and
(c.sum_contacts > 1)) then 1 else 0 end as phnum_over_max

,case when (b.phtyphome <=4 and b.phtypcell <=2 and b.phtypwork <=1 and b.phtypskip <=1) and
(b.phtyphome + b.phtypcell + b.phtypwork + b.phtypskip < a.max_dials_acct) and
(c.sum_contacts = 0) then 1 else 0 end as phnum_not_maxed

from acct_max_dials as a
inner join acct_phtyp_att2c as b on a.acct_num=b.account_number
inner join acct_phtyp_att1d as c on b.account_number=c.account_number and b.phone_number=c.phone_number
inner join (select distinct account_number, attempt_date from attempts_join) as d on a.acct_num=d.account_number
;
quit;

proc sql;
create table dialer_group  as
select 
	acct_num
	,group
	,date
from score.d_dialer_acct_class
where date = &date_d
order by acct_num
;
quit;

proc sql;
create table accts_q_agcy_a as select c13_acct_num as acct_num label='acct_num'
,c13_a3p_tp_id as agency label='agency'

from odscacs.C13_CACS_P325PRI_DAILY
where c13_acct_num in (select d1_account_number from accts_queued)
and c13_processing_date = &date_dt2
;
quit;

proc sql;
create table accts_q_agcy_b as select c19_acct_num as acct_num label='acct_num'
,c19_a3p_tp_id as agency label='agency'

from odscacs.C19_CACS_P326PRI_DAILY
where c19_acct_num in (select d1_account_number from accts_queued)
and c19_processing_date = &date_dt2
;
quit;

data accts_q_agcy;
set accts_q_agcy_a accts_q_agcy_b;
run;

proc sql;
create table max_data as select c.c13_a3p_tp_id
,b.group
,a.attempt_date
,sum(maxed)/sum(dialed) as pct_maxed_dialed format=percent10.2
,sum(under)/sum(dialed) as pct_not_maxed_dialed format=percent10.2
,sum(over)/sum(dialed) as pct_over_maxed_dialed format=percent10.2
,sum(maxed) as accounts_maxed
,sum(under) as accounts_not_maxed
,sum(over) as accounts_over_max

from acct_maxed_final as a /* replaced 'axed' */
inner join dialer_group as b on a.account_number=b.acct_num
inner join cons_accts_dialed as c on a.account_number=c.account_number
group by c.c13_a3p_tp_id, b.group, a.attempt_date
;
quit;

proc sql;
create table not_maxed_detail as select b.*
from (select account_number from acct_maxed_final where under = 1) as a
inner join acct_phtyp_att1a as b on a.account_number=b.account_number
;
quit;

proc sql;
create table not_maxed_detail_GrpA as select c.*

from (select account_number from acct_maxed_final where under = 1) as a
inner join dialer_group as b on a.account_number=b.acct_num
inner join acct_phtyp_att1a as c on a.account_number=c.account_number
where group = 'A'
;
quit;

proc sql;
create table not_maxed_rawdials_GrpA as select b.*

from (select distinct account_number from not_maxed_detail_GrpA) as a
inner join d2_att as b on a.account_number=b.d2_account_number
order by b.d2_account_number, b.d2_phone_number
;
quit;

proc sql;
create table accts_not_dialed as select a.d1_account_number as account_number

from accts_queued as a
left join (select distinct account_number from attempts_join) as b on a.d1_account_number=b.account_number
where b.account_number is null
;
quit;

proc sql;
create table agg_not_dialed as select b.agency
,c.group
,c.date
,count(a.account_number) as count_accounts
from accts_not_dialed as a
inner join accts_q_agcy as b on a.account_number=b.acct_num
inner join dialer_group as c on a.account_number=c.acct_num
group by b.agency, c.group, c.date
;
quit;

proc sql;
create table group_inv1 as select a.d1_account_number
,b.group
,c.agency

from accts_queued as a
inner join dialer_group as b on a.d1_account_number=b.acct_num
inner join accts_q_agcy as c on a.d1_account_number=c.acct_num
;
quit;

proc sql;
create table group_inv2 as select agency
,group
,count(d1_account_number) as count_accounts

from group_inv1
group by agency, group
;
quit;

proc sql;
create table Inventory as 
select 
	 c13_acct_num 			as acct_num 		label='acct_num'
	,c13_referred_ind 		as referred_ind 	label='referred_ind'
	,c13_a3p_tp_id 			as agency 			label='agency'
	,c13_processing_date 	as processing_date 	label='processing_date'
from odscacs.c13_cacs_p325pri_daily
where c13_processing_date = &date_dt2 and
	  c13_referred_ind in ('Y') and 
      compress(c13_a3p_tp_id) in ('ARA','AR6')
;
quit;

proc sql;
create table Inventory_group as select a.*
,b.group

from Inventory as a
left join dialer_group as b on a.acct_num=b.acct_num
;
quit;

proc sql;
create table inv_agg as select agency
,group
,datepart(processing_date) as inventory_date format=date7. label='inventory_date'
,count(acct_num) as count_accounts

from Inventory_group
group by agency, group, inventory_date
;
quit;

proc sql;
create table max_att_daily_summary as select a.agency
,a.group
,a.inventory_date
,intnx('day',a.inventory_date,1,'B') as dial_date format=date7.
,a.count_accounts as inventory
,case when a.group in (' ') then a.count_accounts else b.count_accounts end as accounts_not_dialed
,c.accounts_maxed
,c.accounts_not_maxed
,c.accounts_over_max

from inv_agg as a
left join agg_not_dialed as b on a.agency=b.agency and a.group=b.group
left join max_data as c on a.agency=c.c13_a3p_tp_id and a.group=c.group
;
quit;

%mend;

%max('26APR2016:00:00:00'dt,'25APR2016:00:00:00'dt,'26APR2016'd);
data maxed_all;
set maxed;
run;
data max_data_all;
set max_data;
run;
/*data not_maxed_detail_all;*/
/*set not_maxed_detail;*/
/*run;*/
/*data not_maxed_detail_GrpA_all;*/
/*set not_maxed_detail_GrpA;*/
/*run;*/
/*data not_maxed_rawdials_GrpA_all;*/
/*set not_maxed_rawdials_GrpA;*/
/*run;*/
data agg_not_dialed_all;
set agg_not_dialed;
run;
data max_att_daily_summary_all;
set max_att_daily_summary;
run;

%max('03FEB2016:00:00:00'dt,'02FEB2016:00:00:00'dt,'03FEB2016'd);
proc append base=maxed_all data=maxed;run;
proc append base=max_data_all data=max_data;run;
proc append base=agg_not_dialed_all data=agg_not_dialed;run;
proc append base=max_att_daily_summary_all data=max_att_daily_summary;run;

%max('04FEB2016:00:00:00'dt,'03FEB2016:00:00:00'dt,'04FEB2016'd);
proc append base=maxed_all data=maxed;run;
proc append base=maxed_data_all data=maxed_data;run;
proc append base=agg_not_dialed_all data=agg_not_dialed;run;
proc append base=max_att_daily_summary_all data=max_att_daily_summary;run;

proc sort data=max_data_all;
by attempt_date c13_a3p_tp_id group;
run;

/*Detail set with assignment of max, overmax, under max flags by account*/
PROC EXPORT DATA= maxed_all                                                                                                                                                                                                                                                                                                                                  
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\maxed_all.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

/*Summary set of accounts maxed, under, over by agency and dialer group*/
PROC EXPORT DATA= max_data_all                                                                                                                                                                                                                                                                                                                                  
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\max_data_all.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

/*Detail of not maxed accounts*/
PROC EXPORT DATA= not_maxed_detail_all                                                                                                                                                                                                                                                                                                                                 
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\not_maxed_detail_all.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

/*Detail of the not maxed group A accounts*/
PROC EXPORT DATA= not_maxed_detail_GrpA_all                                                                                                                                                                                                                                                                                                                                  
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\not_maxed_detail_GrpA_all.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

/*Detail of the not maxed dials from the dialer attempts set*/
PROC EXPORT DATA= not_maxed_rawdials_GrpA_all                                                                                                                                                                                                                                                                                                                                  
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\not_maxed_rawdials_GrpA_all.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

/*Summary by agency and dialer group of the accounts not dialed*/
PROC EXPORT DATA= agg_not_dialed_all                                                                                                                                                                                                                                                                                                                                  
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\agg_not_dialed_all.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

/*Summary set based on the format used for general distribution to Steve Snyder and others*/
PROC EXPORT DATA= max_att_daily_summary_all                                                                                                                                                                                                                                                                                                                                  
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\max_att_daily_summary_all.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;
