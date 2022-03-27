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

/**************************************************************************************
     NAME: Max Attempts Not Maxed for Weekly Metrics
	 AUTHOR: Gogie Malathu
	 FUNCTIONAL AREA: Special Project
	 DISTRIBUTION LIST: gogie_malathu@dellteam.com
	 SOURCE SYSTEM: CACS
	 OUTPUT FORMAT: Excel
	 DESC: Brief description of the report purpose, applied filters and specific measures
			applied in the report.
     
	 REVIEWED BY:
	 date and initials of the person who performed the peer review. Examples below.
	 2015/01/06 MFT: Reviewed and passed standard.
	 UPDATE LOG:
	 date and initials of the person who created and or scheduled the report. Examples 
	 below.
	 2015/01/05 TK: Initial creation
	 2015/03/19 MFT: removed join requirement for sets pbx and dfsmanual for extension,
					added proc sodrt nodupkey by phone_number and call time to remove
					potential dups.
		                   		
/*************************************************************************************/

/* GM021715 - Use following dates for day of the week:

1)Monday - CACS (Monday), Dialer(Monday)
2)Tuesday - CACS (Tuesday), Dialer(Tuesday)
3)Wednesday - CACS (Wednesday), Dialer(Wednesday)
4)Thursday - CACS (Thursday), Dialer(Thursday)
5)Friday - CACS (Friday), Dialer(Friday)
6)Saturday - CACS (Friday or Sunday), Dialer(Saturday)

date_dt1 - is datetime() format of dialer file date
date_dt2 - is datetime() format of cacs file date
date_d - date7 format of dialer file date

*/

%macro preco_maxatt(date_dt1,date_dt2,date_d,dow,week);

proc sql;
create table accts_queued as select D1_ACCOUNT_NUMBER
,D1_APPLICATION_ID
,D1_CALL_LIST_ID
,D1_CAMPAIGN_ID
,D1_CAMPAIGN_TYPE
,D1_PROCESSING_DATE

from odscacs.d1_dialer_list
where d1_processing_date = &date_dt1 /*'12FEB2015:00:00:00'dt*/
and D1_CAMPAIGN_ID in ('6199','3199','16199')
;
quit;

proc sql;
create table accts_q_agcy_a as select c13_acct_num as acct_num label='acct_num'
,c13_a3p_tp_id as agency label='agency'

from odscacs.C13_CACS_P325PRI_DAILY
where c13_acct_num in (select d1_account_number from accts_queued)
and c13_processing_date = &date_dt2 /*'12FEB2015:00:00:00'dt*/
;
quit;

proc sql;
create table accts_q_agcy_b as select c19_acct_num as acct_num label='acct_num'
,c19_a3p_tp_id as agency label='agency'

from odscacs.C19_CACS_P326PRI_DAILY
where c19_acct_num in (select d1_account_number from accts_queued)
and c19_processing_date = &date_dt2 /*'12FEB2015:00:00:00'dt*/
;
quit;

data accts_q_agcy;
set accts_q_agcy_a accts_q_agcy_b;
run;

/*This set should match up on an account number level to accounts queued up for work same day (d1_dialer_list)*/
/*There are 65921 accounts which have been assigned a dial group based on 1/29/15 data*/

proc sql;
create table dialer_group as
select 
	 a.acct_num
	,a.group
	,a.date
from score.d_dialer_acct_class a
join (select acct_num, max(date) as maxdate
      from score.d_dialer_acct_class
	  group by acct_num
	  ) b on b.acct_num=a.acct_num and b.maxdate=a.date
order by a.acct_num
;
quit;

/*data dialer_group;*/
/*set score.d_dialer_acct_class;*/
/*if date = &date_d >> '12FEB2015'd;*/
/*run;*/

/* Get raw dialer attempts for day being measured = dial group = d1 dialer list date */

proc sql;
create table d2_att as select a.D2_ACCOUNT_NUMBER
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
,'autodialer' as attempt_source
from odscacs.d2_dialer_attempts as a
left join cxprd.d_outcome_cd as b on a.D2_ATTEMPT_OUTCOME_CODE=b.outcome_cd
where d2_transaction_date = &date_dt1 /*12FEB2015:00:00:00'dt  */
and substr(D2_CALL_TYPE_RAW,4,1) ne 'I'
and D2_CONTACT_OUTCOME_CODE not in (' ')
order by d2_account_number, d2_datetime_call_start
;
quit;

/*Get accounts_queued and all data from 'd2_att' set (after joining autodial attempts to manual attempts)*/

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
where datepart(call_start_datetime) = &date_d /*'12FEB2015'd */
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
and c13_processing_date = &date_dt2 /*'12FEB2015:00:00:00'dt*/
;
quit;

proc sql;
create table pbx_agency_b as select c19_acct_num as acct_num label='acct_num'
,c19_a3p_tp_id as agency label='agency'
from odscacs.C19_CACS_P326PRI_DAILY
where c19_acct_num in (select dfsman_account from pbx_dfsmanual_join_b)
and c19_processing_date = &date_dt2 /*'12FEB2015:00:00:00'dt*/
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
and c14_processing_date = &date_dt2 /*'12FEB2015:00:00:00'dt*/
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
,c.agency
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
,b.rpc

from accts_queued as a
inner join d2_att as b on a.d1_account_number=b.d2_account_number
left join accts_q_agcy as c on a.d1_account_number=c.acct_num
;
quit;

proc sql;
create table accts_q_manual as select b.attempt_source
,a.d1_account_number
,c.agency
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
left join accts_q_agcy as c on a.d1_account_number=c.acct_num
;
quit;

proc sql;
create table autodial_col_arr as select attempt_source
,D1_ACCOUNT_NUMBER as account_number label='account_number'
,D2_PHONE_NUMBER as phone_number label='phone_number'
,LONG_DESC as outcome label='outcome'
,agency
,D2_AGENT_ID_CACS as agent label='agent'
,campaign
,list
,datepart(D2_TRANSACTION_DATE) as attempt_date format=mmddyy10. label='attempt_date'
,D2_DATETIME_CALL_START as datetime_call_start format=datetime20. label='datetime_call_start'
,hour_bucket
,D2_TIME_ZONE as Time_Zone label='Time_Zone'
,rpc as rpc_ind
from accts_q_autodial
where cfpb_attempt = 1
;
quit;

proc sql;
create table manual_col_arr as select attempt_source
,dfsman_account as account_number label='account_number'
,dfsman_phone as phone_number label='phone_number'
,outcome_description as outcome label='outcome'
,agency
,pbx_extension as agent label='agent'
,'n/a' as campaign label='campaign'
,'n/a' as list label='list'
,datepart(pbx_dial_dt_start) as attempt_date format=mmddyy10. label='attempt_date'
,pbx_dial_dt_start as datetime_call_start label='datetime_call_start'
,hour_bucket
,Zip_Code_TZ2 as Time_Zone label='Time_Zone'
,case when outcome_description = 'Right Party Contact' then 1 else 0 end as rpc_ind
from accts_q_manual
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

/*New Phone Number Master Scrubbed File*/

/* Start w/ the score.d_cellphone_id_file*/

proc sql;
create table phone_type1 as select acct_num||phone_number as acct_phone
,*
from score.d_cellphone_id_file
where date = /*'24OCT2014'd*/ &date_d
order by acct_num, phone_number
;
quit;

/*
Then left join this set to the all_dem set from Mike's code.
Implement case when as  follows:
case when consent code = 'L' and _name_ = 'phone2' then 'work'
when consent code = 'L' and _name_ ne 'phone2' then 'home'
when consent code = 'C' then 'cell'
*/

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

from odscacs.p325dem
where acct_num in (select distinct acct_num from phone_type1)
/*and DEMOG_STATUS='C'*/
order by ACCT_NUM;
quit;

data demc demp; set dem;
if DEMOG_STATUS='C' then output demc;
else output demp;
run;


/*current processing*/
data dema; set demc;
by acct_num;
if first.acct_num them c=1;
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

data all_dem; set dem1a dem2a dem3a;
if phone_number=' ' then delete;
phone_type=substr(phone_number,1,1);
phone_consent=substr(phone_number,2,1);
phone=compress(substr(phone_number,3,10));
if phone_consent='Y' then out=1;
else out=2;
run;

proc sort data=all_dem ;
by acct_num phone out;
run;

proc sort data=all_dem nodupkey;
by acct_num phone;
run;

proc sql;
create table phone_type_mstr1 as select a.acct_num
,a.phone_number
,a.date format=mmddyy10.
,case when a.consent_code = 'L' and b._name_ = 'phone2' then 'work'
when a.consent_code = 'L' and b._name_ ne 'phone2' then 'home'
when a.consent_code = 'C' then 'cell' else 'none' end as phone_type1

from phone_type1 as a
left join all_dem as b on a.acct_num=b.acct_num and a.phone_number=b.phone
;
quit;

/*Add phone type to autodialer/manual attempt detail*/
proc sql;
create table att_ph_typ as select a.*
,case when b.phone_type1 not in (' ') then b.phone_type1 else 'UNVERIFIEDSKIP' end as phone_type1
from attempt_seq as a
left join phone_type_mstr1 as b on a.account_number=b.acct_num
and a.phone_number=b.phone_number
;
quit;

proc sort data=att_ph_typ nodupkey out=att_ph_typ_a;
by account_number phone_number datetime_call_start;
run;

proc sql;
create table unver_skip1 as select account_number||phone_number as acct_phone
,*
from att_ph_typ_a
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

/*
PROC EXPORT DATA= unver_skip2                                                                                                                                                                                                                                                                                                                                  
            OUTFILE=&outfile1 >> "\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\unverified_skip_021215.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;
*/

proc sql;
create table att_ph_typ1 as select a.*
,b.group as dialer_group label='dialer_group'
,case when a.phone_type1 = 'home' then 1 else 0 end as Home
,case when a.phone_type1 = 'cell' then 1 else 0 end as Cell
,case when a.phone_type1 ='work' then 1 else 0 end as Work
,case when a.phone_type1 in ('UNVERIFIEDSKIP') then 1 else 0 end as Skip_Unverified
,case when a.phone_type1 not in ('home','cell','work') and a.phone_type1 = 'UNVERIFIEDSKIP' then 1 else 0 end as Other
from att_ph_typ_a as a
left join dialer_group as b on a.account_number=b.acct_num
;
quit;

proc sql;
create table agg1 as select account_number
,dialer_group
,attempt_date
,agency
,sum(home) as home_attempts
,sum(cell) as cell_attempts
,sum(work) as work_attempts
,sum(other) as other_attempts
,sum(home)+sum(cell)+sum(work)+sum(other) as acct_attempts
from att_ph_typ1
group by account_number, dialer_group, attempt_date, agency
;
quit;

proc sql;
create table agg1_a as select account_number
,phone_number
,case when sum(home) > 0 then 4
when sum(cell) > 0 then 2
when sum(work) > 0 then 1
else 1 end as max_dials
,sum(home) as home_attempts
,sum(cell) as cell_attempts
,sum(work) as work_attempts
,sum(other) as other_attempts
,sum(rpc_ind) as rpc_ind
from att_ph_typ1
group by account_number, phone_number
;
quit;

/*New Game Plan
>Create dataset agg2_alt
*/

proc sql;
create table agg2_alt as select a.account_number
,b.phone_number
,a.dialer_group
,a.attempt_date
,a.agency
,b.home_attempts
,b.cell_attempts
,b.work_attempts
,b.other_attempts
,case when b.home_attempts > 0 then 1
when b.cell_attempts > 0 then 1
when b.work_attempts > 0 then 1
when b.other_attempts > 0 then 1
else 0 end as phone_types

/* New Alt Max Attempts Logic - note: 'over' value will be duped across phone/number types, so dupe values by account
will be re-assigned a value of '1' for over, maxed or not maxed*/
,case when a.acct_attempts > 7 then 1
when a.acct_attempts <= 7 and b.rpc_ind = 0 and (b.home_attempts > b.max_dials or b.cell_attempts > b.max_dials or b.work_attempts > b.max_dials or b.other_attempts > b.max_dials) then 1
when  a.acct_attempts <= 7 and  b.rpc_ind > 1 then 1
else 0 end as over2

/*Case statement below checks for maxed at the phone number level.*/
,case when a.acct_attempts < 7 and b.rpc_ind = 1 then 1
when a.acct_attempts = 7 then 1
when a.acct_attempts < 7 and b.home_attempts=b.max_dials and b.cell_attempts = 0 and b.work_attempts = 0 and b.other_attempts = 0 then 1
when a.acct_attempts < 7 and b.home_attempts=0 and b.cell_attempts = b.max_dials and b.work_attempts = 0 and b.other_attempts = 0 then 1
when a.acct_attempts < 7 and b.home_attempts=0 and b.cell_attempts = 0 and b.work_attempts = b.max_dials and b.other_attempts = 0 then 1
when a.acct_attempts < 7 and b.home_attempts=0 and b.cell_attempts = 0 and b.work_attempts = 0 and b.other_attempts = b.max_dials then 1
/* when a.acct_attempts < 7 and b.home_attempts=0 and b.cell_attempts = 0 and b.work_attempts = 4 and b.other_attempts = b.max_dials then 1 > special case for work=home */
else 0 end as maxed2

,case when a.acct_attempts < 7 and b.rpc_ind = 0 	and b.home_attempts < b.max_dials and b.cell_attempts = 0 and b.work_attempts = 0 and b.other_attempts = 0 then 1 /* 4+2 */
when a.acct_attempts < 7 and b.rpc_ind = 0 			and b.home_attempts = 0 and b.cell_attempts < b.max_dials and b.work_attempts = 0 and b.other_attempts = 0 then 1 /* 4+1 */
when a.acct_attempts < 7 and b.rpc_ind = 0 			and b.home_attempts = 0 and b.cell_attempts = 0 and b.work_attempts < b.max_dials and b.other_attempts = 0 then 1 /* 4+1 */
when a.acct_attempts < 7 and b.rpc_ind = 0 			and b.home_attempts = 0 and b.cell_attempts = 0 and b.work_attempts = 0 and b.other_attempts < b.max_dials then 1 /* 4 */
else 0 end as not_maxed2

from agg1 as a /* account level  */
left join agg1_a as b on a.account_number=b.account_number /*account and phone number level*/
order by a.account_number
;
quit;

/*Next step is to group by account number, dialer_group and agency and */
/*sum(home_attempts+cell_attempts+work_attempts+other_attempts)*/
/*sum(phone_types)*/
/*sum(maxed2)*/
/*then if sum(maxed2) = sum(phone_types) then account is maxed, otherwise not_maxed*/

proc sql;
create table agg2dot1_alt as select account_number
,dialer_group
,attempt_date
,agency
,sum(phone_types) as phone_types
,sum(over2) as over
,sum(maxed2) as maxed
,sum(not_maxed2) as not_maxed

from agg2_alt
group by account_number
,dialer_group
,attempt_date
,agency
;
quit;

proc sql;
create table agg2dot2_alt as select account_number
,dialer_group
,attempt_date
,agency

/* Over > 0 trumps maxed > 0 or not_maxed > 0 at phone number level */
,case when (over > 0 and maxed > 0) or (over > 0 and not_maxed > 0) or (over > 0 and maxed = 0 and not_maxed = 0) then over else 0 end as over

/* If  #'s maxed = the # of phone_types dialed on the account, then all numbers dialed have been maxed, therefore account is maxed. */
,case when maxed = phone_types then 1 else 0 end as maxed

/* If an account is over and not_maxed at the same time, over trumps not_maxed. If not over, and #'s maxed < #'s dialed(phone types) and not_maxed > 0 then not_maxed
otherwise if over and maxed are both 0 and not_maxed > 0 then not_maxed
 */
,case when (over > 0 and not_maxed) > 0 then 0
when (over = 0 and maxed > 0 and maxed < phone_types and not_maxed > 0) then 1
when (over = 0 and maxed = 0 and not_maxed > 0) then 1 else not_maxed end as not_maxed

from agg2dot1_alt
;
quit;

/* Dataset below represents the final account level max attempts detail by dialer group, attempt date and agency */
proc sql;
create table agg2dot3_alt as select account_number
,dialer_group
,attempt_date
,agency
,over
,maxed
,not_maxed

from agg2dot2_alt;
quit;

/*  This is a check_sum step that verifies that the sum of  numeric values populated in over, under, maxed = # of rows in dataset above.*/
proc sql;
create table agg2dot4_alt as select sum(over+maxed+not_maxed) as total_accounts_chksum
from agg2dot3_alt;
quit;

/* Get over maxed accounts and detail. */
proc sql;
create table over_max_accts as select distinct account_number
from agg2dot3_alt
where over > 0
;
quit;

proc sql;
create table over_max_detail as select b.*
from over_max_accts as a
left join att_ph_typ1 as b on a.account_number=b.account_number
order by phone_number, account_number, datetime_call_start
;
quit;

data over_max_dials1a;
set over_max_detail;
by phone_number;
if first. phone_number then n=1;
else n+1;
run;

/* Get not maxed accounts and detail.  */
proc sql;
create table not_maxed_accts as select distinct account_number
from agg2dot3_alt
where not_maxed > 0
;
quit;

proc sql;
create table not_maxed_detail as select b.attempt_source
,b.account_number
,b.phone_number
,b.agency
,b.campaign
,b.list
,b.datetime_call_start
,b.phone_type1
,b.dialer_group

from not_maxed_accts as a
left join att_ph_typ1 as b on a.account_number=b.account_number
order by phone_number, account_number, datetime_call_start
;
quit;

data not_maxed_dials1a;
set not_maxed_detail;
by phone_number;
if first. phone_number then n=1;
else n+1;
run;

/* Get maxed accounts and detail. */
proc sql;
create table maxed_accts as select distinct account_number
from agg2dot3_alt
where maxed > 0
;
quit;

proc sql;
create table maxed_detail as select b.attempt_source
,b.account_number
,b.phone_number
,b.agency
,b.campaign
,b.list
,b.datetime_call_start
,b.phone_type1
,b.dialer_group

from maxed_accts as a
left join att_ph_typ1 as b on a.account_number=b.account_number
order by phone_number, account_number, datetime_call_start
;
quit;

data maxed_dials1a;
set maxed_detail;
by phone_number;
if first. phone_number then n=1;
else n+1;
run;

/* Build dataset representing accounts queued and related details. */
proc sql;
create table agg4_a as select a.d1_account_number as account_number label='account_number'
,datepart(a.d1_processing_date) as processing_date format=mmddyy10.
,b.agency
,c.group as dial_group label='dial_group'
,1 as accts_queued

from accts_queued as a /* d1_dialer_list represents accounts queued  */
left join accts_q_agcy as b on a.d1_account_number=b.acct_num
left join dialer_group as c on a.d1_account_number=c.acct_num
;
quit;

/* Build dataset representing accounts dialed and related details. */
proc sql;
create table agg5_a as select account_number
,attempt_date as processing_date label='processing_date'
,agency
,dialer_group as dial_group label='dial_group'
,1 as accts_dialed

from agg1
;
quit;


proc sql;
create table agg6_a as select a.account_number
,b.attempt_date as processing_date label='processing_date'
,b.agency
,b.dialer_group as dial_group label='dial_group'
,a.not_maxed
,a.maxed
,a.over

from agg2dot3_alt as a /* Main dataset w/ deduped over, max, under flags by account */
left join agg1 as b on a.account_number=b.account_number
;
quit;

proc sql;
create table agg7_a as select a.agency
,a.processing_date
,a.dial_group
,a.accts_queued
,case when b.account_number in (' ') then 0 else 1 end as accts_dialed
,c.not_maxed
,c.maxed
,c.over

from agg4_a as a /* represents accounts queued from d1 */
left join agg5_a as b on a.account_number=b.account_number /* represents accounts dialed */
left join agg2dot3_alt as c on a.account_number=c.account_number /* represents de-duped list of what's over/under/maxed */
;
quit;

proc sql;
create table agg7_b as select agency
,processing_date
,dial_group
,accts_queued
,accts_dialed
,case when not_maxed in (.) then 0 else not_maxed end as not_maxed
,case when maxed in (.) then 0 else maxed end as maxed
,case when over in (.) then 0 else over end as over

from agg7_a
;
quit;

proc sql;
create table agg7_c as select agency
,&dow as DOW
,&week as Week
,processing_date
,dial_group
/*,sum(accts_queued) as queued_accounts*/
/*,sum(accts_dialed) as dialed_accounts*/
/*,sum(under) as Not_Maxed*/
/*,sum(maxed) as Maxed*/
/*,sum(over) as Over_Maxed*/
,sum(not_maxed)/sum(accts_dialed) as pct_not_maxed_dialed format=percent10.2
/*,sum(maxed)/sum(accts_dialed) as pct_maxed_dialed format=percent10.2*/
/*,sum(over)/sum(accts_dialed) as pct_overmax_dialed format=percent10.2*/
,sum(not_maxed)/sum(accts_queued) as pct_not_maxed_queued format=percent10.2
/*,sum(maxed)/sum(accts_queued) as pct_maxed_queued format=percent10.2*/
/*,sum(over)/sum(accts_queued) as pct_overmax_queued format=percent10.2*/

from agg7_b
group by agency, /*&dow, &week,*/ processing_date, dial_group
having agency in ('ARA','AR6')
;
quit;

/*data week_summary;*/
/*set agg7_c;*/
/*run;*/

/*proc append base=week_summary data=agg7_c;*/
/*run;*/

/*
options orientation=landscape
		papersize=letter;

ods pdf file=&outfile4 >> '\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\collections_max_attempts_summary_021215.pdf'
style=minimal;

proc print data=agg7_c NOOBS;
 title 'Collections Max Attempts Summary ';
run;

ods pdf close;
*/

/*data week_summary;*/
/*set agg7_c;*/
/*run;*/
/**/
/*proc append base=week_summary data=agg7_c;*/
/*run;*/

/*%preco_maxatt('27Jul2015:00:00:00'dt,'26Jul2015:00:00:00'dt,'27Jul2015'd,'Mon','Last4Avg');*/
/*data week_summary;*/
/*set agg7_c;*/
/*run;*/

%mend;

%preco_maxatt('18Aug2015:00:00:00'dt,'17Aug2015:00:00:00'dt,'18Aug2015'd,'Tue','_Pre-Fix');
data week_summary;
set agg7_c;
run;
data gogie.over_max_081815;
set over_max_dials1a;
run;
data gogie.maxed_dials_081815;
set maxed_dials1a;
run;
data gogie.not_maxed_dials_081815;
set not_maxed_dials1a;
run;
%preco_maxatt('19Aug2015:00:00:00'dt,'18Aug2015:00:00:00'dt,'19Aug2015'd,'Wed','_Pre-Fix');
proc append base=week_summary data=agg7_c;
run;
data gogie.over_max_081915;
set over_max_dials1a;
run;
data gogie.maxed_dials_081915;
set maxed_dials1a;
run;
data gogie.not_maxed_dials_081915;
set not_maxed_dials1a;
run;
%preco_maxatt('20Aug2015:00:00:00'dt,'19Aug2015:00:00:00'dt,'20Aug2015'd,'Thu','_Pre-Fix');
proc append base=week_summary data=agg7_c;
run;
data gogie.over_max_082015;
set over_max_dials1a;
run;
data gogie.maxed_dials_082015;
set maxed_dials1a;
run;
data gogie.not_maxed_dials_082015;
set not_maxed_dials1a;
run;


%preco_maxatt('08Sep2015:00:00:00'dt,'07Sep2015:00:00:00'dt,'08Sep2015'd,'Tue','Post-Fix');
proc append base=week_summary data=agg7_c;
run;
data gogie.over_max_090815;
set over_max_dials1a;
run;
data gogie.maxed_dials_090815;
set maxed_dials1a;
run;
data gogie.not_maxed_dials_090815;
set not_maxed_dials1a;
run;
%preco_maxatt('09Sep2015:00:00:00'dt,'08Sep2015:00:00:00'dt,'09Sep2015'd,'Wed','Post-Fix');
proc append base=week_summary data=agg7_c;
run;
data gogie.over_max_090915;
set over_max_dials1a;
run;
data gogie.maxed_dials_090915;
set maxed_dials1a;
run;
data gogie.not_maxed_dials_090915;
set not_maxed_dials1a;
run;
%preco_maxatt('10Sep2015:00:00:00'dt,'09Sep2015:00:00:00'dt,'10Sep2015'd,'Thu','Post-Fix');
proc append base=week_summary data=agg7_c;
run;
data gogie.over_max_091015;
set over_max_dials1a;
run;
data gogie.maxed_dials_091015;
set maxed_dials1a;
run;
data gogie.not_maxed_dials_091015;
set not_maxed_dials1a;
run;

PROC EXPORT DATA= week_summary                                                                                                                                                                                                                                                                                                                                  
			OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\Tue_Thu_Summary.txt"
			DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
	 PUTNAMES=YES;                                                        
																																																																																			   
RUN;

PROC EXPORT DATA= gogie.not_maxed_dials_090815                                                                                                                                                                                                                                                                                                                                  
			OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\not_maxed_dials_090815.txt"
			DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
	 PUTNAMES=YES;                                                        
																																																																																			   
RUN;

PROC EXPORT DATA= gogie.not_maxed_dials_090915                                                                                                                                                                                                                                                                                                                                  
			OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\not_maxed_dials_090915.txt"
			DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
	 PUTNAMES=YES;                                                        
																																																																																			   
RUN;

PROC EXPORT DATA= gogie.not_maxed_dials_091015                                                                                                                                                                                                                                                                                                                                  
			OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\not_maxed_dials_091015.txt"
			DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
	 PUTNAMES=YES;                                                        
																																																																																			   
RUN;

PROC EXPORT DATA= gogie.maxed_dials_090815                                                                                                                                                                                                                                                                                                                                  
			OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\maxed_dials_090815.txt"
			DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
	 PUTNAMES=YES;                                                        
																																																																																			   
RUN;

PROC EXPORT DATA= gogie.maxed_dials_090915                                                                                                                                                                                                                                                                                                                                  
			OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\maxed_dials_090915.txt"
			DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
	 PUTNAMES=YES;                                                        
																																																																																			   
RUN;

PROC EXPORT DATA= gogie.maxed_dials_091015                                                                                                                                                                                                                                                                                                                                  
			OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\maxed_dials_091015.txt"
			DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
	 PUTNAMES=YES;                                                        
																																																																																			   
RUN;
