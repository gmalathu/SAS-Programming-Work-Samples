libname odscacs oracle user="GMALATHU" password="Ph03n1xABC!" path=prdBIDW schema=ods_cacs;
libname dwprd oracle user="GMALATHU" password="Ph03n1xABC!" path=prdBIDW schema=dwprd;
libname cxprd oracle user="GMALATHU" password="Ph03n1xABC!" path=prdBIDW schema=cxprd;
options compress=yes;
option sastrace = ',b.,b.,b.d' sastraceloc=saslog nostsuffix;
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
libname bttc 'E:\BTTC';

/*Constraint in new environment is 6 dials a week on an account.*/

/*option obs=10000;*/
option obs=MAX;

/*data test1;*/
/*set odscacs.p325dem;*/
/*set odscacs.p327ext;*/
/*run;*/

proc sql;
create table pri as select acct_num
,b.a3p_tp_id as agency label='agency'
,b.A3P_DATE_ASSIGN
,b.CACS_STATE_CODE
,b.CYCLE_ID
,b.LOCATION_CODE
,b.PLACEMENT_NUM
,b.REFERRED_IND
,b.STATE_ENTRY_DATE

from odscacs.P325pri
where referred_ind = 'Y'
and a3p_tp_id in ('ARA','AR6')
;
quit;

proc sql;
create table ext as select ACCT_NUM
,b.ACCT_OPEN_DATE
,b.BALANCE_AMT
/*,b.BEHAVIOR_INDEX*/
/*,b.BUS_ADDR_STREET_1*/
/*,b.BUS_ADDR_STREET_2*/
/*,b.BUS_DOB_DATE*/
/*,b.BUS_FIRST_NAME*/
/*,b.BUS_LAST_NAME*/
/*,b.BUS_MIDDLE_NAME*/
/*,b.BUSINESS_CITY*/
/*,b.BUSINESS_STATE*/
/*,b.BUSINESS_ZIP_CODE*/
,b.CHARGE_OFF_AMT
,b.CHARGE_OFF_DATE
/*,b.CREDIT_SCORE*/
,b.CUST_ADDR_STREET_1
,b.CUST_DOB_DATE
,b.CUST_FIRST_NAME
,b.CUST_LAST_NAME
,b.CUST_MIDDLE_NAME
,b.CUSTOMER_CITY
,b.CUSTOMER_NAME
,b.CUSTOMER_STATE
,b.CUSTOMER_ZIP_CODE
,b.CYCLE_ID
/*,b.CYCLES_DELINQ_CNT*/
/*,b.DAYS_DELINQ_CNT*/
,b.LAST_PAYMENT_AMT
,b.LAST_PAYMENT_DATE
,b.LGL_STATUS
,b.PRINCIPAL_AMT
/*,b.RISK_CODE*/
/*,b.TIME_ZONE*/
,b.TOTAL_DUE_AMT

from odscacs.p327ext
where acct_num in (select acct_num from pri)
;
quit;

proc sql;
create table join as select a.*
,b.ACCT_OPEN_DATE
,b.BALANCE_AMT
,b.CHARGE_OFF_AMT
,b.CHARGE_OFF_DATE
,b.CUST_ADDR_STREET_1
,b.CUST_DOB_DATE
,b.CUST_FIRST_NAME
,b.CUST_LAST_NAME
,b.CUST_MIDDLE_NAME
,b.CUSTOMER_CITY
,b.CUSTOMER_NAME
,b.CUSTOMER_STATE
,b.CUSTOMER_ZIP_CODE
,b.CYCLE_ID
,b.LAST_PAYMENT_AMT
,b.LAST_PAYMENT_DATE
,b.LGL_STATUS
,b.PRINCIPAL_AMT
,b.TOTAL_DUE_AMT

from pri as a
inner join ext as b on a.acct_num=b.acct_num
;
quit;

/*proc sql;*/
/*create table dem as select ACCT_NUM*/
/*,b.ADDR_RELATIONSHIP*/
/*,b.ADDRESS_1*/
/*,b.ADDRESS_2*/
/*,b.BLOCK_CODE*/
/*,b.CAS_ADDRESS_IND*/
/*,b.CITY*/
/*,b.DATE_LAST_UPDATED*/
/*,b.DATE_UPDATED*/
/*,b.DEM_ISO_LANG_CD*/
/*,b.DEM_TELEPHONE_1*/
/*,b.DEM_TELEPHONE_2*/
/*,b.DEM_TELEPHONE_3*/
/*,b.DEMOG_CHG_DATE*/
/*,b.DEMOG_CHG_IND*/
/*,b.DEMOG_NAME*/
/*,b.DEMOG_STATUS*/
/*,b.FIRST_NAME*/
/*,b.ISO_COUNTRY_CD*/
/*,b.LAST_NAME*/
/*,b.LOCATION_CODE*/
/*,b.MIDDLE_NAME*/
/*,b.NAME_RELATIONSHIP*/
/*,b.STATE*/
/*,b.ZIP*/
/**/
/*from odscacs.p325dem*/
/*where ACCT_NUM in (select acct_num from join)*/
/*;*/
/*quit;*/

/*Create segmentation column based on advertising exec standard age based segments:*/
/*18 – 24*/
/*25 – 34*/
/*35 – 44*/
/*45 – 54*/
/*55 – 64*/
/*65+*/

proc sql;
create table get_age1 as select acct_num
,BALANCE_AMT
,CHARGE_OFF_DATE

,CUST_DOB_DATE
,input(put(CUST_DOB_DATE,z8.),yymmdd8.) as DOB format=mmddyy10.
,round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) as age
,case when 0 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 17 then '00-17'
when 17 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 24 then '18-24'
when 24 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 34 then '25-34'
when 34 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 44 then '35-44'
when 44 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 54 then '45-54'
when 54 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 64 then '55-64'
when round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) > 64 then '65+  ' else 'null' end as age_segment

,CUSTOMER_STATE
,CUSTOMER_ZIP_CODE

,BUS_DOB_DATE
,BUSINESS_STATE
,BUSINESS_ZIP_CODE

,CYCLE_ID
,LAST_PAYMENT_AMT
,LAST_PAYMENT_DATE
,PRINCIPAL_AMT

from join;
quit;

data get_null;
set get_age1;
if age_segment = 'null';
run;

proc sql;
create table test1 as select distinct acct_num from get_age1;
quit;

proc sql;
create table age_seg_agg1 as select age_segment
,count(acct_num) as count_accounts

from get_age1
group by age_segment;
quit;

/*Get attempts from last week*/

%macro loop_attempts;
%do I=1 %to 5;
	%if &I < 6 %then %do;

proc sql;
create table dials&I as select a.D2_ACCOUNT_NUMBER
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
where d2_transaction_date = intnx('dtday',datetime(),&I-9) /* '22AUG2016:00:00:00'dt */
and substr(D2_CALL_TYPE_RAW,4,1) ne 'I'
and D2_CONTACT_OUTCOME_CODE not in (' ')
and D2_ATTEMPT_OUTCOME_CODE_RAW not in ('LM','TR')
order by d2_account_number, d2_datetime_call_start
;
quit;

%end;
%end;
%mend loop_attempts;

%loop_attempts;

data weekly_dials;
set dials1 dials2 dials3 dials4 dials5;
run;

data keep_rpc(keep=D2_ACCOUNT_NUMBER D2_ATTEMPT_OUTCOME_CODE LONG_DESC 
D2_DATETIME_CALL_END D2_DATETIME_CALL_START hour_bucket D2_PHONE_NUMBER 
cfpb_attempt rpc);
set weekly_dials;
if rpc > 0;
if D2_ACCOUNT_NUMBER not in (' ');
if substr(D2_ACCOUNT_NUMBER,4,1) not in ('-');
run;

proc sql;
create table ext as select ACCT_NUM
,CUST_DOB_DATE
,input(put(CUST_DOB_DATE,z8.),yymmdd8.) as DOB format=mmddyy10.
,round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) as age
,case when 0 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 17 then '00-17'
when 17 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 24 then '18-24'
when 24 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 34 then '25-34'
when 34 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 44 then '35-44'
when 44 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 54 then '45-54'
when 54 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 64 then '55-64'
when round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) > 64 then '65+  ' else 'null' end as age_segment

from odscacs.p327ext
where acct_num in (select distinct D2_ACCOUNT_NUMBER from keep_rpc)
;
quit;

proc sql;
create table dem as select ACCT_NUM
,FIRST_NAME
,LAST_NAME
,DEM_DOB_DATE

from odscacs.p325dem
where ACCT_NUM in (select distinct D2_ACCOUNT_NUMBER from keep_rpc)
;
quit;

proc sql;
create table others as select a.*
,b.age_segment

from keep_rpc as a
inner join (select acct_num, age_segment from ext) as b on a.D2_ACCOUNT_NUMBER=b.acct_num
where D2_ACCOUNT_NUMBER not in (' ')
and substr(D2_ACCOUNT_NUMBER,4,1) not in ('-')
and length(compress(D2_ACCOUNT_NUMBER)) = 18
and b.age_segment not in ('null')
order by D2_ACCOUNT_NUMBER, hour_bucket, D2_DATETIME_CALL_START
;
quit;

PROC EXPORT DATA= others                                                                                                                                                                                                                                                                                                                                  
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\others.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

/*Coding starting on 9/8/16*/

data inventory_set;
set bttc.inventory_set;
run;

data dialer_list;
set bttc.dialer_list;
run;

data attempts;
set bttc.attempts;
run;

data dialer_groups;
set bttc.dialer_groups;
run;

/*Get average scores by cycle id and pull in the DOB/age segment for */
/*resulting dataset*/

proc sql;
create table blah1 as select acct_num
,charge_off_date
,BALANCE_AMT
,CYCLE_ID 
,CUST_DOB_DATE
,location_code
,input(put(CUST_DOB_DATE,z8.),yymmdd8.) as DOB format=mmddyy10.
,round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) as age
,case when 0 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 17 then '00-17'
when 17 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 24 then '18-24'
when 24 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 34 then '25-34'
when 34 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 44 then '35-44'
when 44 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 54 then '45-54'
when 54 < round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) <= 64 then '55-64'
when round((today() - input(put(CUST_DOB_DATE,z8.),yymmdd8.))/364,1) > 64 then '65+  ' else 'null' end as age_segment

from odscacs.p327ext
where charge_off_date = 0
and location_code in ('010101')
;
quit;

proc sql;
create table blah2 as select acct_num
,referred_ind
,REL_RISK_SCORE /* number pool is based off of. */
,case
when rel_risk_score <= 347 then 'LOW'
when rel_risk_score >= 382 then 'HIGH'
else 'MEDIUM'
end  as risk_tier label='risk_tier'
,a3p_tp_id as agency label='agency'
,location_code

from odscacs.P325PRI
where acct_num in (select acct_num from blah1)
and location_code in ('010101')
and referred_ind = 'Y'
and a3p_tp_id in ('ARA','AR6')
;
quit;

proc sql;
create table blah as select a.*
,b.REL_RISK_SCORE /* number pool is based off of. */
,b.risk_tier
,b.agency

from blah1 as a
inner join blah2 as b on a.acct_num=b.acct_num
;
quit;
