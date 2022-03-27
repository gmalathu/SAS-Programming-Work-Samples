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

/*Get accounts queued*/
%macro max(dtime1,dtime2,day1,detail1,notdialed1,summary1);

proc sql;
create table accts_queued as select D1_ACCOUNT_NUMBER
            ,D1_CAMPAIGN_ID
            ,D1_PROCESSING_DATE
            ,datepart(D1_PROCESSING_DATE) as processing_date format=date9.
            
            
from odscacs.D1_DIALER_LIST
                              where D1_PROCESSING_DATE = &dtime1
                              and compress(D1_CAMPAIGN_ID) in ('6199','3199','16199')
                              ;
                              quit;
							  
/*Get state for accounts queued*/

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
and c14_processing_date = &dtime2
and C14_location_code = '010101'

order by c14_acct_num, c14_processing_date;
quit;

/*Get agency for queued accounts*/

proc sql;
create table queued_agency as select c13_acct_num
,c13_a3p_tp_id

from odscacs.c13_cacs_p325pri_daily
where c13_acct_num in (select distinct D1_ACCOUNT_NUMBER from accts_queued)
and c13_processing_date = &dtime2
/*and c13_location_code = '010101'*/
;
quit;

/*Get dial detail from the dialer attempts table*/

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
,case when ((substr(D2_CALL_TYPE_RAW,4,1) ne 'I' and d2_contact_outcome_code in ('20','21','22','23','24','25','30','31','32','33','34','35','36','37','38','39','40','50')) or
(substr(D2_CALL_TYPE_RAW,4,1) ne 'I' and D2_ATTEMPT_OUTCOME_CODE_RAW in ('NP','PR'))) then 1 else 0 end as contact_rpc
,case when d2_contact_outcome_code not in ('00','01','02','03','04','06','11','20')
and substr(D2_CALL_TYPE_RAW,4,1) in ('G','P') then 'manual'
when d2_contact_outcome_code not in ('00','01','02','03','04','06','11','20')
and substr(D2_CALL_TYPE_RAW,4,1) not in ('G','P','I') then 'dialer' else 'other' end as attempt_source

from odscacs.d2_dialer_attempts as a
left join cxprd.d_outcome_cd as b on a.D2_ATTEMPT_OUTCOME_CODE=b.outcome_cd
where d2_transaction_date between intnx('dtday',&dtime1,0,'B') and intnx('dtday',&dtime1,0,'E')
and substr(D2_CALL_TYPE_RAW,4,1) ne 'I'
and D2_CONTACT_OUTCOME_CODE not in (' ')
and D2_ATTEMPT_OUTCOME_CODE_RAW not in ('LM','TR')
and compress(a.D2_CALL_TYPE_RAW) not in ('WGAP')
order by d2_account_number, d2_datetime_call_start
;
quit;

/*Get agency for dialer attempts were agency id the day prior is ARA or AR6*/

proc sql;
create table dials_agency as select c13_acct_num as acct_num label='acct_num'
,c13_a3p_tp_id as agency label='agency'

from odscacs.c13_cacs_p325pri_daily
where c13_acct_num in (select distinct D2_ACCOUNT_NUMBER from d2_att)
and c13_processing_date = &dtime2
and compress(c13_a3p_tp_id) in ('ARA','AR6')
;
quit;

/*Create dataset containing the accounts queued joined to attempts
made on attempt date. Use same name in the meantime so as not to
break the object reference structure*/

proc sql;
create table accts_q_all as select b.attempt_source
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
,b.contact_rpc
,case when b.cfpb_attempt = 1 and b.D2_CONTACT_OUTCOME_CODE in ('23') then 1 else 0 end as third_party
,c.C14_CUSTOMER_STATE
,c.C14_CUSTOMER_ZIP_CODE

from accts_queued as a
inner join d2_att as b on a.d1_account_number=b.d2_account_number
inner join cust_state as c on a.d1_account_number=c.c14_acct_num
/*inner join dials_agency as d on a.d1_account_number=d.acct_num*/
where ((compress(c.C14_CUSTOMER_STATE) not in ('ID','MA','SC','WA','WV','PA')) or
(substr(C14_CUSTOMER_ZIP_CODE,1,3) not in ('100','101','102','103','104','111','112','113','114','116')) or
(substr(C14_CUSTOMER_ZIP_CODE,1,5) not in ('11004','11005')))
;
quit;

proc sql;
create table attempts_join as select attempt_source
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
,contact_rpc
/*,third_party*/

from accts_q_all
where cfpb_attempt = 1
;
quit;

proc sort data=attempts_join nodupkey;
by ACCOUNT_NUMBER PHONE_NUMBER DATETIME_CALL_START;
run;

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
and c13_processing_date = &dtime2
and c13_location_code = '010101'
;
quit;

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
when phone_type = 'F' then 'work'
when phone_type = 'G' then 'work'
when phone_type = 'H' then 'home'
when phone_type = 'I' then 'cell'
when phone_type = 'J' then 'home' else 'skip' end as desc

from all_dem1
;
quit;

data get_phone5;
set all_dem_final;
if phone_field = 'phone5';
run;

data acct_phnum_dialed(keep=account_number phone_number);
set attempts_join;
run;

proc sort data=acct_phnum_dialed nodupkey out=att_join_unq;
by account_number phone_number;
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
create table phone_type_mstr_alt as select a.*
,b.phone_field
,b.phone_type as phone_type_value label='phone_type_value'
,b.desc as phone_type_desc label='phone_type_desc'

,c.n1_result_code
,c.n1_phone_type
,c.n1_phone_in_service
,c.n1_prepaid_phone
,c.n1_do_not_call
,d.consent_code as Innovis_consent_cd

/*,case when c.N1_PHONE_NUMBER is null and b.desc = 'cell' then 'cell'*/
/*when c.N1_PHONE_NUMBER is null and b.desc = 'home' then 'home'*/
/*when c.N1_PHONE_NUMBER is null and b.desc = 'work' then 'work'*/
/*when c.N1_PHONE_NUMBER is not null and c.N1_RESULT_CODE = 'L' and upcase(b.phone_field) = 'PHONE2' then 'work'*/
/*when c.N1_PHONE_NUMBER is not null and c.N1_RESULT_CODE = 'L' and upcase(b.phone_field) ne 'PHONE2' then 'home'*/
/*when c.N1_PHONE_NUMBER is not null and c.N1_RESULT_CODE in ('1','2','3','5','7','10') then 'cell' */
/*when c.N1_PHONE_NUMBER is not null and c.N1_RESULT_CODE = ' ' and b.desc not in (' ') then b.desc else 'skip' end as phone_type*/

/*Updated Logic to prioritize cacs over neustar*/
/*,case when b.desc = 'cell' then 'cell'*/
/*when b.desc = 'home' then 'home'*/
/*when b.desc = 'work' then 'work'*/
/*when b.desc in (' ') and c.N1_PHONE_NUMBER is not null and c.N1_RESULT_CODE = 'L' then 'home'*/
/*when b.desc in (' ') and c.N1_PHONE_NUMBER is not null and c.N1_RESULT_CODE in ('5','7','10') then 'cell'*/
/*when b.desc not in (' ') and c.N1_PHONE_NUMBER is not null then b.desc else 'skip' end as phone_type*/

/*Updated logic to only look at cacs and disregard neustar.*/
,case when b.desc not in (' ') then b.desc
when b.desc in (' ') then 'skip' else 'skip' end as phone_type

from att_join_unq as a
left join all_dem_final as b on a.account_number=b.acct_num and a.phone_number=b.phone
left join neustar3 as c on a.account_number=c.N1_ACCOUNT_NUMBER and
a.phone_number=c.N1_PHONE_NUMBER
left join cellphone_id as d on a.account_number=d.acct_num and a.phone_number=d.phone_number
order by account_number, phone_number
;
quit;

data get_nulls(keep=account_number phone_number);
set phone_type_mstr_alt;
if phone_type = 'skip';
run;

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
create table unverified_skip1 as select account_number
,phone_number
,datetime_call_start

from acct_phtyp_att1a
where phone_type = 'skip'
;
quit;

proc sql;
create table unverified_skip as select account_number
, phone_number
,count(datetime_call_start) as attempt_count

from unverified_skip1
group by account_number, phone_number
;
quit;

proc sql;
create table acct_phtyp_att1b as select account_number
,phone_number
,phone_type
,sum(cfpb_attempt) as sum_attempts
,sum(contact_rpc) as sum_contact_rpc

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
,sum(sum_contact_rpc) as sum_contact_rpc

from acct_phtyp_att1b
group by account_number, phone_number
order by account_number, phone_number
;
quit;

proc sql;
create table maxed as select b.*
,a.max_dials_acct
,c.sum_contact_rpc
,d.attempt_date
,1 as dialed
,case when ((b.phtyphome <=4 and b.phtypcell <=2 and b.phtypwork <=1 and b.phtypskip <=1) and
(b.phtyphome + b.phtypcell + b.phtypwork + b.phtypskip = a.max_dials_acct)) or
((b.phtyphome <=4 and b.phtypcell <=2 and b.phtypwork <=1 and b.phtypskip <=1) and
(b.phtyphome + b.phtypcell + b.phtypwork + b.phtypskip < a.max_dials_acct) and
(c.sum_contact_rpc = 1)) then 1 else 0 end as phnum_maxed

,case when (b.phtyphome > 4 or b.phtypcell > 2 or b.phtypwork > 1 or b.phtypskip > 1) or
(b.phtyphome + b.phtypcell + b.phtypwork + b.phtypskip > a.max_dials_acct) or
((b.phtyphome <= 4 and b.phtypcell <=2 and b.phtypwork <=1 and b.phtypskip <=1) and
(b.phtyphome + b.phtypcell + b.phtypwork + b.phtypskip < a.max_dials_acct) and
(c.sum_contact_rpc > 1)) then 1 else 0 end as phnum_over_max

,case when (b.phtyphome <=4 and b.phtypcell <=2 and b.phtypwork <=1 and b.phtypskip <=1) and
(b.phtyphome + b.phtypcell + b.phtypwork + b.phtypskip < a.max_dials_acct) and
(c.sum_contact_rpc = 0) then 1 else 0 end as phnum_not_maxed

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
where date = &day1
order by acct_num
;
quit;

proc sql;
create table accts_q_agcy_a as select c13_acct_num as acct_num label='acct_num'
,c13_a3p_tp_id as agency label='agency'

from odscacs.C13_CACS_P325PRI_DAILY
where c13_acct_num in (select d1_account_number from accts_queued)
and c13_processing_date = &dtime2
;
quit;

proc sql;
create table accts_q_agcy_b as select c19_acct_num as acct_num label='acct_num'
,c19_a3p_tp_id as agency label='agency'

from odscacs.C19_CACS_P326PRI_DAILY
where c19_acct_num in (select d1_account_number from accts_queued)
and c19_processing_date = &dtime2
;
quit;

data accts_q_agcy;
set accts_q_agcy_a accts_q_agcy_b;
run;

proc sql;
create table maxed_acct_agg as select account_number
,attempt_date
,sum(phnum_maxed) as maxed
,sum(phnum_over_max) as over_maxed
,sum(phnum_not_maxed) as under_maxed
,sum(phtypwork)+sum(phtyphome)+sum(phtypcell)+sum(phtypskip) as sum_all_attempts
,max_dials_acct
,dialed

from maxed
group by account_number, attempt_date,max_dials_acct,dialed
;
quit;

/*proc sql;*/
/*create table acct_maxed_final as select account_number*/
/*,attempt_date*/
/*,case when (over_maxed+under_maxed = 0 and maxed >=1) and sum_all_attempts <= max_dials_acct then 1 else 0 end as maxed*/
/*,case when over_maxed >=1 then 1 else 0 end as over*/
/*,case when under_maxed >= 1 and sum_all_attempts <= max_dials_acct then 1 else 0 end as under*/
/*,dialed*/
/**/
/*from maxed_acct_agg*/
/*;*/
/*quit;*/

/*Key step in determining if there are dupes in attempt_phone or */
/*accounts dialed in the max attempts daily summary*/

proc sql;
create table acct_maxed_finala as select account_number
,attempt_date
,case when (over_maxed > 0 and under_maxed = 0 and maxed = 0) then 1
when (over_maxed > 0 and (maxed > 0 or under_maxed > 0)) then 1
else 0 end as over
,case when (maxed > 0 and under_maxed = 0 and over_maxed = 0) then 1
when (maxed > 0 and under_maxed > 0 and over_maxed = 0) then 1
when (maxed > 0 and over_maxed > 0) then 0
else 0 end as maxed
,case when (under_maxed > 0 and maxed = 0 and over_maxed = 0) then 1
when (under_maxed > 0 and (maxed > 0 or over_maxed > 0)) then 0
else 0 end as under
,dialed

from maxed_acct_agg
;
quit;

proc sql;
create table pre_agg_max as select a.account_number
,b.c13_a3p_tp_id
,c.group
,a.attempt_date
,a.over
,a.maxed
,a.under
,a.dialed

from acct_maxed_finala as a
left join cons_accts_dialed as b on a.account_number=b.account_number
left join dialer_group as c on a.account_number = c.acct_num
;
quit;

proc sql;
create table max_data as select 
c13_a3p_tp_id
,group
,attempt_date

,sum(maxed)/sum(dialed) as pct_maxed_dialed format=percent10.2
,sum(under)/sum(dialed) as pct_not_maxed_dialed format=percent10.2
,sum(over)/sum(dialed) as pct_over_maxed_dialed format=percent10.2
,sum(maxed) as accounts_maxed
,sum(under) as accounts_not_maxed
,sum(over) as accounts_over_max
,sum(dialed) as sum_dialed

from pre_agg_max
group by c13_a3p_tp_id, group, attempt_date
;
quit;

proc sql;
create table &detail1 as select b.*
,c.phone_field||' - '||c.phone_type as phone_field_type
from (select account_number from acct_maxed_finala where over = 1) as a
inner join acct_phtyp_att1a as b on a.account_number=b.account_number
left join all_dem_final as c on b.account_number=c.acct_num and b.phone_number=c.phone
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
create table &notdialed1 as select b.agency
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
where c13_processing_date = &dtime2 and
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
create table attempt_phone1 as select a.agency
,a.group
,count(b.phone_number) as count_phone_numbers

from (select a.agency
,a.group
,a.acct_num
,b.phone_number

from Inventory_Group as a
left join maxed as b on a.acct_num=b.account_number
where b.phone_number not in (' '))
group by a.agency
,a.group
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
create table &summary1 as select a.agency
,a.group
,a.inventory_date
,d.count_phone_numbers as attempt_phone
,intnx('day',a.inventory_date,1,'B') as dial_date format=date7.
,a.count_accounts as inventory
,case when a.group in (' ') then a.count_accounts else b.count_accounts end as accounts_not_dialed
,c.accounts_maxed
,c.accounts_not_maxed
,c.accounts_over_max

from inv_agg as a
left join &notdialed1 as b on a.agency=b.agency and a.group=b.group
left join max_data as c on a.agency=c.c13_a3p_tp_id and a.group=c.group
left join attempt_phone1 as d on a.agency=d.agency and a.group=d.group
;
quit;

proc append base=detail_test1 data=&detail1;
run;
proc append base=notdialed_test1 data=&notdialed1;
run;
proc append base=summary_test1 data=&summary1;
run;

%mend;

%max('01Nov2016:00:00:00'dt,'31Oct2016:00:00:00'dt,'01Nov2016'd,detail1_1101,not_dialed1_1101,summary1_1101);
data detail_test1;
set detail1_1101;
run;

data notdialed_test1;
set not_dialed1_1101;
run;

data summary_test1;
set summary1_1101;
run;
%max('02Nov2016:00:00:00'dt,'01Nov2016:00:00:00'dt,'02Nov2016'd,detail1_1102,not_dialed1_1102,summary1_1102);
%max('03Nov2016:00:00:00'dt,'02Nov2016:00:00:00'dt,'03Nov2016'd,detail1_1103,not_dialed1_1103,summary1_1103);
%max('04Nov2016:00:00:00'dt,'03Nov2016:00:00:00'dt,'04Nov2016'd,detail1_1104,not_dialed1_1104,summary1_1104);
%max('07Nov2016:00:00:00'dt,'06Nov2016:00:00:00'dt,'07Nov2016'd,detail1_1107,not_dialed1_1107,summary1_1107);
%max('08Nov2016:00:00:00'dt,'07Nov2016:00:00:00'dt,'08Nov2016'd,detail1_1108,not_dialed1_1108,summary1_1108);
%max('09Nov2016:00:00:00'dt,'08Nov2016:00:00:00'dt,'09Nov2016'd,detail1_1109,not_dialed1_1109,summary1_1109);
%max('10Nov2016:00:00:00'dt,'09Nov2016:00:00:00'dt,'10Nov2016'd,detail1_1110,not_dialed1_1110,summary1_1110);
%max('11Nov2016:00:00:00'dt,'10Nov2016:00:00:00'dt,'11Nov2016'd,detail1_1111,not_dialed1_1111,summary1_1111);
%max('14Nov2016:00:00:00'dt,'13Nov2016:00:00:00'dt,'14Nov2016'd,detail1_1114,not_dialed1_1114,summary1_1114);
%max('15Nov2016:00:00:00'dt,'14Nov2016:00:00:00'dt,'15Nov2016'd,detail1_1115,not_dialed1_1115,summary1_1115);
%max('16Nov2016:00:00:00'dt,'15Nov2016:00:00:00'dt,'16Nov2016'd,detail1_1116,not_dialed1_1116,summary1_1116);
%max('17Nov2016:00:00:00'dt,'16Nov2016:00:00:00'dt,'17Nov2016'd,detail1_1117,not_dialed1_1117,summary1_1117);
%max('18Nov2016:00:00:00'dt,'17Nov2016:00:00:00'dt,'18Nov2016'd,detail1_1118,not_dialed1_1118,summary1_1118);
%max('21Nov2016:00:00:00'dt,'20Nov2016:00:00:00'dt,'21Nov2016'd,detail1_1121,not_dialed1_1121,summary1_1121);
%max('22Nov2016:00:00:00'dt,'21Nov2016:00:00:00'dt,'22Nov2016'd,detail1_1122,not_dialed1_1122,summary1_1122);
%max('23Nov2016:00:00:00'dt,'22Nov2016:00:00:00'dt,'23Nov2016'd,detail1_1123,not_dialed1_1123,summary1_1123);
%max('24Nov2016:00:00:00'dt,'23Nov2016:00:00:00'dt,'24Nov2016'd,detail1_1124,not_dialed1_1124,summary1_1124);
%max('25Nov2016:00:00:00'dt,'24Nov2016:00:00:00'dt,'25Nov2016'd,detail1_1125,not_dialed1_1125,summary1_1125);
%max('28Nov2016:00:00:00'dt,'27Nov2016:00:00:00'dt,'28Nov2016'd,detail1_1128,not_dialed1_1128,summary1_1128);
%max('29Nov2016:00:00:00'dt,'28Nov2016:00:00:00'dt,'29Nov2016'd,detail1_1129,not_dialed1_1129,summary1_1129);
%max('30Nov2016:00:00:00'dt,'29Nov2016:00:00:00'dt,'30Nov2016'd,detail1_1130,not_dialed1_1130,summary1_1130);

PROC EXPORT DATA= summary_test1                                                                                                                                                                                                                                                                                                                                 
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\summary_test1.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

PROC EXPORT DATA= detail_test1                                                                                                                                                                                                                                                                                                                                
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\detail_test1.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

PROC EXPORT DATA= notdialed_test1                                                                                                                                                                                                                                                                                                                                
            OUTFILE="\\tsclient\C\Users\Gogie_Malathu\Documents\SAS Data\notdialed_test1.txt"
            DBMS=tab replace;                                                                                                                                                                                                                                                                                                                                          
     PUTNAMES=YES;                                                        
                                                                                                                                                                                                                                                                                                                                               
RUN;

proc sql;
create table pri_test as select acct_num
,cacs_state_code

from odscacs.p325pri
where referred_ind = 'Y'
and compress(a3p_tp_id) in ('ARA','AR6')
;
quit;
