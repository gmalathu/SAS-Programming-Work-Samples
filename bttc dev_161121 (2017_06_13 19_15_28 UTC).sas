
%macro dateloop;
%do I= 0 %to 49;
%if &I<49 %then %do;
 
proc sql;
create table inventory as select c13_acct_num
			,c13_a3p_tp_id
			,c13_referred_ind
			,c13_PROMISE_ARNG_IND_1
			,c13_INSTRUCTION_CODE
			,input(put(c13_HOLD_DATE,z6.),yymmdd6.) as c13_HOLD_DATE format=date9.
			,c13_PROMISE_ARNG_IND_2
			,input(put(c13_PROMISE_TAKEN_DATE,z6.),yymmdd6.) as c13_PROMISE_TAKEN_DATE format=date9.
			,c13_PROMISE_TAKEN_STE
			,input(put(c13_SETL_SETUP_DATE,z6.),yymmdd6.) as c13_SETL_SETUP_DATE format=date9.
			,input(put(c13_SETL_SATISFY_DATE,z6.),yymmdd6.) as c13_SETL_SATISFY_DATE format=date9.
			,input(put(c13_SETL_EXPIRE_DATE,z6.),yymmdd6.) as c13_SETL_EXPIRE_DATE format=date9.
			,c13_processing_date
			,c13_cacs_state_code
			,input(put(C13_STATE_ENTRY_DATE,z6.),yymmdd6.) as C13_STATE_ENTRY_DATE format=date9.
			,C13_CYCLE_ID
,C13_REL_RISK_SCORE


from odscacs.c13_cacs_p325pri_daily

where compress(c13_a3p_tp_id) in ('ARA','AR6')
and c13_referred_ind='Y'
and c13_processing_date=intnx('dtday','17Jul2016:00:00:00'dt,0+&I)
/*and c13_processing_date=intnx('dtday',datetime(),-2,'B')*/
;
quit;
proc sql;
create table ext as select c14_acct_num
			,c14_balance_amt
			,c14_processing_date
			,case when  c14_coll_asgn_ud_2 = 'S' then  'SILVER    '
	when  c14_coll_asgn_ud_2 = 'P' then 'PLATINUM'
	when c14_coll_asgn_ud_2 = 'E' then  'ELITE    '
	else  c14_coll_asgn_ud_2 end as service_tier 
	,C14_CUSTOMER_STATE
,substr(C14_CUSTOMER_ZIP_CODE,1,5) as C14_CUSTOMER_ZIP_CODE
,case when C14_CYCLES_DELINQ_CNT in (.,0.1) then 0
else C14_CYCLES_DELINQ_CNT+1 end as dq_bucket



from odscacs.c14_cacs_p327ext_daily
where c14_acct_num in (select distinct c13_acct_num from inventory)
and c14_processing_date=intnx('dtday','17Jul2016:00:00:00'dt,0+&I)
and C14_CYCLES_DELINQ_CNT not in (.)
;
quit;
proc sql;
create table inventory&I as select a.*
		,b.*

from inventory as a
inner join ext as b on a.c13_acct_num=b.c14_acct_num;
quit;
proc sql;
create table list as select *

from odscacs.d1_dialer_list
where D1_ACCOUNT_NUMBER in (select distinct c13_acct_num from inventory&I)
and D1_PROCESSING_DATE=intnx('dtday','18Jul2016:00:00:00'dt,0+&I)
;
quit;
proc sql;
create table dialer_attempts as select D2_ACCOUNT_NUMBER as acct
,D2_CALL_TYPE_RAW as campaign_id label='campaign_id'
,D2_DATETIME_CALL_START as DATETIME_CALL_START 
,D2_PHONE_NUMBER as phone_number
,datepart(D2_TRANSACTION_DATE) as attempt_date format=date9.
,case when substr(D2_CALL_TYPE_RAW,4,1) ne 'I' and d2_contact_outcome_code in ('20','21','22','23','24','25','30','31','32','33','34','35','36','37','38','39','40','50') 
		then 1 else 0 end as contact
,case when substr(D2_CALL_TYPE_RAW,4,1) ne 'I' and D2_ATTEMPT_OUTCOME_CODE_RAW in ('NP','PR')
		then 1 else 0 end as rpc
,case when substr(D2_CALL_TYPE_RAW,4,1) ne 'I' and D2_ATTEMPT_OUTCOME_CODE_RAW in ('PR')
		then 1 else 0 end as ptp
,case when d2_contact_outcome_code not in ('00','01','02','03','04','06','11','20')
			and substr(D2_CALL_TYPE_RAW,4,1) ne 'I' then 1 else 0 end as attempt 
,case when d2_contact_outcome_code not in ('00','01','02','03','04','06','11','20')
			and substr(D2_CALL_TYPE_RAW,4,1) in ('G','P') then 1 else 0 end as manual_attempt 
,case when d2_contact_outcome_code not in ('00','01','02','03','04','06','11','20')
			and substr(D2_CALL_TYPE_RAW,4,1) not in ('G','P','I') then 1 else 0 end as dialer_attempt 
,case when substr(D2_CALL_TYPE_RAW,4,1) = 'I' then 1 else 0 end as inbound_call 
,case when substr(D2_CALL_TYPE_RAW,4,1) not in ( 'I') then 1 else 0 end as total_attempt 

from odscacs.d2_dialer_attempts
where D2_ACCOUNT_NUMBER in (select distinct c13_acct_num from inventory&I)
and d2_transaction_date = intnx('dtday','18Jul2016:00:00:00'dt,0+&I)
and d2_agency in ('ARA','AR3','AR6')
;
quit;

%end;
 
data inv; set inv inventory&I;
run;
data dlist; set dlist list;
run;
data datt; set datt dialer_attempts;
run;

%end;
%mend dateloop;
 
%dateloop;

data BTTC.inventory_set; set inv;
inventory_date=intnx('day',datepart(c13_processing_date),1);
format inventory_date date9.;
run;
data BTTC.dialer_list; set dlist;
run;
data BTTC.attempts; set datt;
run;
proc sql;
create table BTTC.dialer_groups as
select * from score.d_dialer_acct_class
where acct_num in (select distinct c13_acct_num from BTTC.inventory_set)
									and date>='18Jul2016'd;
									quit;




proc sql;
create table listj as select a.c13_acct_num
,a.c13_a3p_tp_id
,a.c13_referred_ind
,a.c13_promise_arng_ind_1
,a.c13_instruction_code
,a.c13_hold_date
,a.c13_cacs_state_code
,a.c14_balance_amt
,a.c14_customer_state
,a.c14_customer_zip_code
,a.dq_bucket1
		,b.D1_APPLICATION_ID
,b.D1_CAMPAIGN_ID
,b.D1_CAMPAIGN_TYPE
,b.D1_CALL_LIST_ID
,b.D1_PROCESSING_DATE
,d.group
,case when e.desc=' ' then 'Not Queued' else e.desc end as Dialer_Grouping
,case when c.attempt>0 then 1 else 0 end as unique_attempt
,case when compress(a.C14_CUSTOMER_STATE) in ('ID','MA','WA') then 1
	when substr(a.C14_CUSTOMER_ZIP_CODE,1,3) in ('100','101','102','103','104','111','112','113','114','116') then 1
	when substr(a.C14_CUSTOMER_ZIP_CODE,1,5) in ('11004','11005') then 1
	else 0 end as restricted_state
,intnx('day',datepart(a.c14_processing_date),1) as inv_date format=date9.
,weekday(intnx('day',datepart(a.c14_processing_date),1)) as test
,case when a.C13_HOLD_DATE>=intnx('day',datepart(a.c14_processing_date),1) then 1
			else 0 end as hold_acct
,case when b.D1_CAMPAIGN_ID not in (' ') then 1 else 0 end as in_list
,case when b.D1_CAMPAIGN_ID not in (' ','9801') then 1 else 0 end as report_queued

,c.total_attempt
,c.attempt
,c.dialer_attempt
,c.manual_attempt
,c.contact
,c.rpc
,c.ptp
,c.inbound_call
,1 as acct
,case when substr(c13_cacs_state_code,1,1)='S' then 1 else 0 end as skip_acct

from (select *, dq_bucket-2 as dq_bucket1 from inv where compress(c13_a3p_tp_id) in ('ARA','AR6')
						and weekday(intnx('day',datepart(c14_processing_date),1)) not in (1,7)
						) as a
left join dlist as b on a.c13_acct_num=b.D1_ACCOUNT_NUMBER
					and a.c14_processing_date=intnx('dtday',b.D1_PROCESSING_DATE,-1)
left join (select acct
				,attempt_date
,sum(contact) as contact
,sum(rpc) as rpc
,sum(ptp) as ptp
,sum(attempt) as attempt
,sum(total_attempt) as total_attempt
,sum(manual_attempt) as manual_attempt
,sum(dialer_attempt) as dialer_attempt
,sum(inbound_call ) as inbound_call

from datt
group by acct ,attempt_date) as c on a.c13_acct_num=c.acct
					and datepart(a.c14_processing_date)=intnx('day',c.attempt_date,-1)
left join (select * from score.d_dialer_acct_class ) as d on a.c13_acct_num=d.acct_num
													and intnx('day',datepart(a.c14_processing_date),1)=d.date
left join score.d_dialer_number_class as e on d.group=e.group
order by a.c13_acct_num, a.c13_processing_date
		;
					quit;

proc means data=listj (keep=c13_a3p_tp_id inv_date  acct hold_acct skip_acct restricted_state in_list report_queued unique_attempt total_attempt attempt dialer_attempt manual_attempt
						  contact rpc ptp inbound_call) noprint missing nway;
class c13_a3p_tp_id inv_date ;
var acct hold_acct skip_acct restricted_state in_list report_queued unique_attempt total_attempt attempt dialer_attempt manual_attempt
						  contact rpc ptp inbound_call;
output out=agency_sum (drop=_type_ _freq_)
sum=acct hold_acct skip_acct restricted_state in_list report_queued unique_attempt total_attempt attempt dialer_attempt manual_attempt
						  contact rpc ptp inbound_call;
						run;

PROC EXPORT DATA= agency_sum                                                                                                                                                                                                                                                                                                                                        
            OUTFILE="\\tsclient\C\Users\mike_tratnik\My Documents\SAS_Data\agency_sum.txt"                                                                                                                                                                                                                                                                                                                                               
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN; 
/*proc delete data=inv;*/
/*run;*/
/**/
/*proc delete data=dlist;*/
/*run;*/
/**/
/*proc delete data=datt;*/
/*run;*/

data ashaset; set listj;
where in_list=1 and unique_attempt=1;
run;


PROC EXPORT DATA= ashaset                                                                                                                                                                                                                                                                                                                                        
            OUTFILE="E:\\Asha\july 2016 attempts.txt"                                                                                                                                                                                                                                                                                                                                               
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN; 

proc sql;
create table asha2 as select 
a.c13_acct_num
,a.c13_a3p_tp_id
,a.c13_referred_ind
,a.c13_promise_arng_ind_1
,a.c13_instruction_code
,a.c13_hold_date
,a.c13_cacs_state_code
,a.c14_balance_amt
,a.c14_customer_state
,a.c14_customer_zip_code
,a.dq_bucket1
		,b.D1_APPLICATION_ID
,b.D1_CAMPAIGN_ID
,b.D1_CAMPAIGN_TYPE
,b.D1_CALL_LIST_ID
,b.D1_PROCESSING_DATE
,d.group
,case when e.desc=' ' then 'Not Queued' else e.desc end as Dialer_Grouping
,intnx('day',datepart(a.c14_processing_date),1) as inv_date format=date9.


from (select *, dq_bucket-2 as dq_bucket1 from inv where compress(c13_a3p_tp_id) in ('ARA','AR6')
						and weekday(intnx('day',datepart(c14_processing_date),1)) not in (1,7)
						and intnx('day',datepart(c14_processing_date),1)<'01aug2016'd) as a
inner join dlist as b on a.c13_acct_num=b.D1_ACCOUNT_NUMBER
					and a.c14_processing_date=intnx('dtday',b.D1_PROCESSING_DATE,-1)
					and '20jul2016'd<=intnx('day',datepart(c14_processing_date),1)<'01aug2016'd
left join (select * from score.d_dialer_acct_class ) as d on a.c13_acct_num=d.acct_num
													and intnx('day',datepart(a.c14_processing_date),1)=d.date
left join score.d_dialer_number_class as e on d.group=e.group
order by a.c13_acct_num, a.c13_processing_date
		;
					quit;
proc sql;
create table datt2 as select *

from datt
where acct in (select distinct c13_acct_num from asha2)
and '20jul2016'd<=attempt_date<'01aug2016'd;
quit;


proc sql;
create table dem as select ACCT_NUM
,DEMOG_STATUS
,NAME_RELATIONSHIP
,ADDR_RELATIONSHIP
,substr(compress(DEM_TELEPHONE_1),1,12) as phone1
,substr(compress(DEM_TELEPHONE_2),1,12) as phone2
,substr(compress(DEM_TELEPHONE_3),1,12) as phone3
,DEMOG_CHG_DATE
from odscacs.p325dem
where ACCT_NUM in (select distinct c13_acct_num from asha2)
and DEMOG_STATUS='C'
and NAME_RELATIONSHIP='A'
and location_code='010101'
/*and ADDR_RELATIONSHIP=*/
order by ACCT_NUM,DEMOG_CHG_DATE;

quit;

data dema; set dem;
where DEMOG_STATUS='C';
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
  by ACCT_NUM ;
  var phone1-phone3;

run;
proc transpose data=dem2 out=dem2a (rename=(col1=phone_number )) ;
  by ACCT_NUM ;
  var phone1-phone3;

run;
proc transpose data=dem3 out=dem3a (rename=(col1=phone_number )) ;
  by ACCT_NUM ;
  var phone1-phone3;

run;
data all_demc; set dem1a dem2a dem3a;
if phone_number=' ' then delete;
phone_type=substr(compress(phone_number),1,1);
phone_consent=substr(compress(phone_number),2,1);
phone=substr(compress(phone_number),3,10);
run;
proc sort data=all_demc nodupkey out=all_demc1;
by acct_num phone;
run;

proc sql;
create table asha3 as select a.*
		,b.phone_type

from datt2 as a
left join all_demc1 as b on a.acct=b.acct_num
						and a.phone_number=b.phone;
						quit;

proc sql;
create table asha4 as select a.*
		,b.*
		,case when b.phone_type ='H' then 'Home/Land' 
		when b.phone_type ='I' then 'Home/Cell' 
		when b.phone_type ='J' then 'Home/Unknown' 
		when b.phone_type ='B' then 'Bus/Land' 
		when b.phone_type ='F' then 'Bus/Cell' 
		when b.phone_type ='G' then 'Bus/Unknown' 
		when b.phone_type ='C' then 'Alt/Land' 
		when b.phone_type ='D' then 'Alt/Cell' 
		when b.phone_type ='E' then 'Alt/Unknown' 
		else 'Curr. Blank/Unknown' end as type_tranlation

from asha2 as a
inner join asha3 as b on a.c13_acct_num=b.acct
					and a.inv_date=b.attempt_date
					and b.phone_type not in (' ')
					order by a.c13_acct_num,a.inv_date;
					quit;

PROC EXPORT DATA= asha4                                                                                                                                                                                                                                                                                                                                        
            OUTFILE="E:\\Asha\july 2016 sample.txt"                                                                                                                                                                                                                                                                                                                                               
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN; 
