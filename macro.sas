/*******************************************************/
%macro rename(dsetin=, vars=, varn=,);

     
%let rename=;

%let xn=1;

%do %until ( %scan(&vars,&xn)= );
    %let token1 = %scan(&vars,&xn);
	%let token2 = %scan(&varn,&xn);
	%let rename=&rename &token1 = &token2;
	%let xn=%EVAL(&xn + 1);
%end;

	proc datasets library=work noprint;
	modify &dsetin;
	rename &rename;
	run;quit;

%mend rename;
/*******************************************************/
%macro ttm(dsetin=, dsetout=, vars=, code=, year=, month=,);

%if &dsetout = %then %let dsetout = &dsetin;
     
%let mer=;
%let varm=;
%let xn=1;
  
%do %until ( %scan(&vars,&xn)= );
    %let token = %scan(&vars,&xn);
	%let mer=&mer , a.&token +b.&token -c.&token as ttm_&token;
	%let varm=&varm ttm_&token;
	%let xn=%EVAL(&xn + 1);
%end;

%let xn=%eval(&xn-1);

	proc delete data=temp; run;
	
	proc sql;
	create table temp as select a.* &mer from &dsetin as a 
	left join &dsetin as b on a.&year - 1=b.&year and b.&month =12 and a.&code =b.&code
	left join &dsetin as c on a.&year - 1=c.&year and a.&month =c.&month and a.&code =c.&code ; quit;

	data &dsetout;
    set temp;
  		array trimvars{&xn} &vars;
		array trimvarm{&xn} &varm;
  			do xi = 1 to dim(trimvars);
				if &month=12 then trimvarm{xi}=trimvars{xi};
			end;
	drop xi;
	run;  

%mend ttm;
/*******************************************************/
%macro ioa(dsetin=, dsetout=, vars=, code=, year=, month=,);

%if &dsetout = %then %let dsetout = &dsetin;
     
%let mer=;
%let xn=1;
  
%do %until ( %scan(&vars,&xn)= );
    %let token = %scan(&vars,&xn);
	%let mer=&mer , a.&token -b.&token as yd_&token , b.&token as lag_&token;
	%let xn=%EVAL(&xn + 1);
%end;


	proc delete data=&dsetout; run;
	
	proc sql;
	create table &dsetout as select a.* &mer from &dsetin as a 
	left join &dsetin as b on a.&year - 1=b.&year and a.&month =b.&month and a.&code =b.&code; quit;


%mend ioa;
/*******************************************************/
%macro sq(dsetin=, dsetout=, vars=, code=, year=, month=,);

%if &dsetout = %then %let dsetout = &dsetin;
     
%let mer=;
%let varm=;
%let xn=1;
  
%do %until ( %scan(&vars,&xn)= );
    %let token = %scan(&vars,&xn);
	%let mer=&mer , a.&token -b.&token as sq_&token;
	%let varm=&varm sq_&token;
	%let xn=%EVAL(&xn + 1);
%end;

%let xn=%eval(&xn-1);

	proc delete data=temp; run;
	
	proc sql;
	create table temp as select a.* &mer from &dsetin as a 
	left join &dsetin as b on a.&year =b.&year and a.&month-3=b.&month  and a.&code =b.&code; quit;

	data &dsetout;
    set temp;
  		array trimvars{&xn} &vars;
		array trimvarm{&xn} &varm;
  			do xi = 1 to dim(trimvars);
				if &month=3 then trimvarm{xi}=trimvars{xi};
			end;
	drop xi;
	run;  

%mend sq;
/*******************************************************/
%macro growth(dsetin=, dsetout=,vars=, code=, month_id=,);

%let mer1=;
%let mer2=;
%let xn=1;
  
%do %until ( %scan(&vars,&xn)= );
    %let token = %scan(&vars,&xn);
	%let mer1=&mer1 ,((a.&token-b.&token)/(abs(b.&token )+1))  as g_&token;
	%let xn=%EVAL(&xn + 1);
%end;

	proc sql;
	create table &dsetout as select a.* &mer1 from &dsetin as a left join 
	&dsetin as b on a.&code =b.&code and a.&month_id -12=b.&month_id; quit;


%mend growth;
/*******************************************************/
%macro industry_adjustment(dsetin=, dsetout=, vars=, week=,class=,);

%if &dsetout = %then %let dsetout = &dsetin;
     
%let mer1=;
%let mer2=;
%let mer3=;
%let mid=;
%let se=;
%let xn=1;
  
%do %until ( %scan(&vars,&xn)= );
    %let token = %scan(&vars,&xn);
    %let mid=&mid mid_&token;
    %let se=&se se_&token;
	%let mer1=&mer1 , a.&token -b.mid_&token as i1_&token;
	%let mer2=&mer2 , (a.&token -b.mid_&token)/b.se_&token as i2_&token;
	%let mer3=&mer3 , a.&token/b.mid_&token as i3_&token;
	%let xn=%EVAL(&xn + 1);
%end;

%let xn=%eval(&xn-1);
%let mer=&mer1 &mer2 &mer3;

proc sort data=&dsetin;by &week &class;quit;
proc means data=&dsetin noprint;
	var &vars;
	by &week &class;
	output out=stat
	median(&vars)=&mid
	std(&vars)=&se;
run;


	proc delete data=temp; run;	
	proc sql;
	create table temp as select a.* &mer from &dsetin as a 
	left join stat as b on a.&week=b.&week and a.&class=b.&class; quit;
  data &dsetout;
    set temp;
   run;

%mend industry_adjustment;
/*******************************************************/
%macro liquidity(datain=,dataout=);
	proc expand
	data=&datain out=mov_avr method=none;
		convert turnover=turnover_1 / transformout=(movave 20  ); 
		convert trading_volume=trading_volume_1 / transformout=( movave 20 ); 
		convert amihud=amihud_1 / transformout=( movave 20 ); 
		convert num=obs_1 / transformout=(movsum 20 ); 
	
		convert turnover=turnover_2 / transformout=(movave 40  ); 
		convert trading_volume=trading_volume_2 / transformout=( movave 40 ); 
		convert amihud=amihud_2 / transformout=( movave 40 ); 
		convert num=obs_2 / transformout=(movsum 40 ); 
		
		convert turnover=turnover_3 / transformout=(movave 60  ); 
		convert trading_volume=trading_volume_3 / transformout=( movave 60 ); 
		convert amihud=amihud_3 / transformout=( movave 60 ); 
		convert num=obs_3 / transformout=(movsum 60 ); 
		
		convert turnover=turnover_4 / transformout=(movave 80  ); 
		convert trading_volume=trading_volume_4 / transformout=( movave 80 ); 
		convert amihud=amihud_4 / transformout=( movave 80 ); 
		convert num=obs_4 / transformout=(movsum 80 ); 
	by stkcd;
	run;
	
	proc delete data=&datain;quit; 
	
	data mov_avr;set mov_avr;
	%do i=1 %to 4;
		array trimvars_&i{3} turnover_&i trading_volume_&i amihud_&i;
		do xi = 1 to dim(trimvars_&i);
			if obs_&i<%eval(10*&i) then trimvars_&i{xi}=.;
			if num=. then  trimvars_&i{xi}=.;
		end;
	drop xi;
	%end;
	drop  adj_price adj_ret amihud liq_size price size trad_share trading_volume turnover;
	run;
	
	proc expand data=mov_avr out=mov_avr120 method=none;

		convert turnover_1=turnover_mean_1 / transformout=(movave 120); 
		convert trading_volume_1=trading_volume_mean_1 / transformout=(movave 120); 
		convert amihud_1=amihud_mean_1 / transformout=(movave 120); 
		convert turnover_1=turnover_std_1 / transformout=(movstd 120); 
		convert trading_volume_1=trading_volume_std_1 / transformout=(movstd 120); 
		convert amihud_1=amihud_std_1 / transformout=(movstd 120); 

		convert turnover_2=turnover_mean_2 / transformout=(movave 120); 
		convert trading_volume_2=trading_volume_mean_2 / transformout=(movave 120); 
		convert amihud_2=amihud_mean_2 / transformout=(movave 120); 
		convert turnover_2=turnover_std_2 / transformout=(movstd 120); 
		convert trading_volume_2=trading_volume_std_2 / transformout=(movstd 120); 
		convert amihud_2=amihud_std_2 / transformout=(movstd 120); 
		
		convert turnover_3=turnover_mean_3 / transformout=(movave 120); 
		convert trading_volume_3=trading_volume_mean_3 / transformout=(movave 120); 
		convert amihud_3=amihud_mean_3 / transformout=(movave 120); 
		convert turnover_3=turnover_std_3 / transformout=(movstd 120); 
		convert trading_volume_3=trading_volume_std_3 / transformout=(movstd 120); 
		convert amihud_3=amihud_std_3 / transformout=(movstd 120); 
		
		convert turnover_4=turnover_mean_4 / transformout=(movave 120); 
		convert trading_volume_4=trading_volume_mean_4 / transformout=(movave 120); 
		convert amihud_4=amihud_mean_4 / transformout=(movave 120); 
		convert turnover_4=turnover_std_4 / transformout=(movstd 120); 
		convert trading_volume_4=trading_volume_std_4 / transformout=(movstd 120); 
		convert amihud_4=amihud_std_4 / transformout=(movstd 120); 
	convert num=obs_120 / transformout=(movsum 120); 
	by stkcd;
	run;
	
	proc delete data=mov_avr;quit; 
	
	data &dataout;set mov_avr120;
	%do i=1 %to 4;
		array trimvars_&i{6} turnover_mean_&i trading_volume_mean_&i amihud_mean_&i
		turnover_std_&i trading_volume_std_&i amihud_std_&i;
		do xi = 1 to dim(trimvars_&i);
			if obs_120<60 then trimvars_&i{xi}=.;
			if num=. then  trimvars_&i{xi}=.;
		end;
	drop xi;
	%end;
	run;

	%do i=1 %to 4;
	data &dataout;set &dataout;
		ln_turnover_&i=log(turnover_&i);
		c1_turnover_&i=turnover_&i-turnover_mean_&i;
		c2_turnover_&i=turnover_&i/turnover_mean_&i;
		c3_turnover_&i=(turnover_&i-turnover_mean_&i)/(turnover_std_&i);
		drop turnover_mean_&i turnover_std_&i;
		
		ln_trading_volume_&i=log(trading_volume_&i);
		c1_trading_volume_&i=trading_volume_&i-trading_volume_mean_&i;
		c2_trading_volume_&i=trading_volume_&i/trading_volume_mean_&i;
		c3_trading_volume_&i=(trading_volume_&i-trading_volume_mean_&i)/(trading_volume_std_&i);
		drop trading_volume_mean_&i trading_volume_std_&i;
		
		ln_amihud_&i=log(amihud_&i);
		c1_amihud_&i=amihud_&i-amihud_mean_&i;
		c2_amihud_&i=amihud_&i/amihud_mean_&i;
		c3_amihud_&i=(amihud_&i-amihud_mean_&i)/(amihud_std_&i);
		drop amihud_mean_&i amihud_std_&i;
	run;
	%end;
	data &dataout;set &dataout;
		if ((turnover_1=.)&(turnover_2=.)&(turnover_3=.)&(turnover_4=.)
		&(ln_turnover_1=.)&(ln_turnover_2=.)&(ln_turnover_3=.)&(ln_turnover_4=.))
		or((c1_turnover_1=.)&(c1_turnover_2=.)&(c1_turnover_3=.)&(c1_turnover_4=.))
		then delete;
	run;
%mend liquidity;
/****************************************************************************************************/
%macro beta(dsetin=,dsetin_dayid=,dsetout=,);

%do i= 60 %to 180 %by 60;

%if &i ^=180 %then %do;
	proc delete data=temp1 temp2 temp3 temp4 temp5 temp6;run;
	%do a= 1000 %to 5000 %by 1000;
			proc sql;
			create table temp1 as select a.date, b.stkcd,
			b.ret,b.ret_m,b.ret_i,b.num from &dsetin_dayid a, &dsetin b 
			where &a-1000 <a.day_id<=&a and a.day_id>=b.day_id>=a.day_id+1-&i;quit;

			proc sort data=temp1; by stkcd date;run;

			proc reg data=temp1 outest=reg_temp1 noprint edf;
			model ret=ret_m ret_i;
			by stkcd date;
			run;quit;

			proc reg data=temp1 outest=reg_temp2 noprint edf;
			model ret=ret_m ret_i;
			by stkcd date;
			where ret_m>0;
			run;quit;

			proc reg data=temp1 outest=reg_temp3 noprint edf;
			model ret=ret_m ret_i;
			by stkcd date;
			where ret_m<0;
			run;quit;

			proc append base=temp2 data=reg_temp1 force;run;
			proc append base=temp3 data=reg_temp2 force;run;
			proc append base=temp4 data=reg_temp3 force;run;
	%end;

		proc sql;
		create table temp5 as select a.date,a.stkcd, b._rmse_ as mse&i, b.ret_m as beta&i, b._edf_ + 3 as num1,
		c.ret_m as beta_p&i, c._edf_ + 3 as num2,a.ret_m as beta_n&i, a._edf_ + 3 as num3
		from temp2 b, temp3 c, temp4 a where 
		a.stkcd=b.stkcd=c.stkcd and a.date=b.date=c.date;quit;

		proc sql;
		create table temp6 as select date, 
		max(num2) as max2,max(num3) as max3 
		from temp5 group by date;quit;

		proc sort data=temp5; by date stkcd;run;

		data result&i;
		merge temp5 temp6;
		by date; 
		if num1<&i/2 then do; beta&i=.;mse&i=.;end;
		if num2<max2/2 then beta_p&i=.;
		if num3<max3/2 then beta_n&i=.;
		drop  max2 max3 num1 num2 num3;
		run;

%end;
%end;

data &dsetout;merge result60 result120 result240;
by date stkcd;run;

%mend beta;
/**********************************************************************/
%macro return_data(dsetin=,dsetin_dayid=,dsetout=,);

	proc delete data=temp1 temp2 temp3 temp_num mov_num temp_mret mov_ret temp_reg temp_res mov_res &dsetout;run;

			data mov_num;set &dsetin;keep stkcd day_id num; run;
			proc sort data=mov_num; by stkcd day_id;run;
		
			%do j= 0 %to 6;
				%do i =1 %to 6;
					%let a1=%eval(20* &j );
					%let a2=%eval(20* &i );
					%if &a1^=0 %then %do; 
					proc expand data=mov_num out=mov_num method=none;
						convert num = num&i.&j / transform=(movsum &a2 lag &a1 );
					by stkcd;
					run;
					%end;
					%else %do;
				 	proc expand data=mov_num out=mov_num method=none;
						convert num = num&i.&j / transform=(movsum &a2);
					by stkcd;
					run;
					%end;
				%end;
			%end;

			data mov_ret;set &dsetin;keep stkcd day_id ret; run;
			proc sort data=mov_ret; by stkcd day_id;run;

	
			%do j= 0 %to 6;
				%do i =1 %to 6;
					%let a1=%eval(20* &j );
					%let a2=%eval(20* &i );
					%if &a1^=0 %then %do; 
					proc expand data=mov_ret out=mov_ret method=none;
						convert ret = ret&i.&j / transform=(movsum &a2 lag &a1);
					by stkcd;
					run;
					%end;
					%else %do;
					proc expand data=mov_ret out=mov_ret method=none;
						convert ret = ret&i.&j / transform=(movsum &a2);
					by stkcd;
					run;
					%end;
				%end;
			%end;


	%do a= 500 %to 5000 %by 500;
			proc sql;
			create table temp1 as select a.day_id, b.stkcd, b.day_id as day_id1, 
			b.ret,b.ret_m,b.ret_i,b.num from &dsetin_dayid a, &dsetin b 
			where &a-500 <a.day_id<=&a and a.day_id>=b.day_id>=a.day_id-119;quit;

			proc sort data=temp1; by stkcd day_id day_id1;run;

			proc reg data=temp1 noprint;
			model ret = ret_m ret_i;
			by stkcd day_id;
			output out=temp_reg r=resid;
			quit;run;

			data temp_reg;set temp_reg; keep stkcd day_id day_id1 resid;
			proc sort; by stkcd day_id day_id1;run;

			proc expand data=temp_reg out=temp_res method=none;
			%do j= 0 %to 6;
				%do i =1 %to 6;
					%let a1=%eval(20* &j );
					%let a2=%eval(20* &i );
					%if &a1^=0 %then %do;
					convert resid = res&i.&j / transform=(movsum &a2 lag &a1 );
					%end;
					%else %do;
					convert resid = res&i.&j / transform=(movsum &a2);
					%end;
				%end;
			%end;
			by stkcd;
			run;

			proc sort data=temp_res;by stkcd day_id day_id1;run;
			data temp_res; set temp_res; by stkcd day_id; if last.day_id;run;

			proc append base=mov_res data= temp_res ;run;

	%end;

	data mov_num;
	set mov_num;
	drop time num;
	run;

	data mov_ret;set mov_ret;
	drop time ret;run;

	data mov_res;set mov_res;
	drop time resid day_id1;run;

	proc sql; create table &dsetout as select a.*, b.*, c.* from mov_res as a
	left join mov_num as b on a.stkcd=b.stkcd and a.day_id=b.day_id
	left join mov_ret as c on a.stkcd=c.stkcd and a.day_id=c.day_id;quit;

	data &dsetout; set &dsetout;
		%do j= 0 %to 6;
			%do i =1 %to 6;
			%let a3=%eval(10* &i );
			if num&i.&j <&a3 then do;
				ret&i.&j = .; res&i.&j = .;end;
			drop num&i.&j ;
			%end;
		%end;
	run;

	
%mend return_data;

/***************************************************************************/
%macro missing(dsetin=, dsetout=,);

proc delete data=tables;run;

proc format; 
      value num_f  . = "0"
                  other = "1" ;     
      value $char_f " " = "0" 
                  other = "1" ;   
run;

ods output onewayfreqs=tables; 
proc freq data= &dsetin;     
tables _all_ / missing;     
format _numeric_ num_f. _character_ $char_f.;
run;
ods output close; 

data &dsetout;
      length variable $50;
      set tables;
      variable = scan(Table,2,"“");
      value = max(of F_:);
      if value = 0;
      keep variable frequency percent;
run;

%mend missing;
/***************************************************************************/
%macro prior(freq=,rev=);
/*滞后变量为前L个月价格(rev=Y时为避免月内反转影响，空出一个月)*/
proc expand data=&freq.ly_all out=&freq.ly_lag method=none;
	%if &rev=Y %then %do;
		convert  adj_price = l_m_p / transform=(lag 1);
		convert  num = num_one / transform=(movsum 1);
		convert  num = num_oy / transform=(movsum 12);
		%do L=1 %to 12;
			convert  adj_price = l_p_&L / transform=(lag %eval(&L+1));
			convert  num = nump&L / transform=(movsum %eval(&L+1));
		%end;
		by stkcd;
	%end;
	%if &rev=N %then %do;
		%do L=1 %to 12;
			convert  adj_price = l_p_&L / transform=(lag %eval(&L));
			convert  num = num&L / transform=(movsum %eval(&L));
			convert  num = num_oy / transform=(movsum 12);
		%end;
		by stkcd;
	%end;
run;
/*前L个月收益(数据缺失超过一半的不选用)*/
data &freq.ly_lag_ret;set &freq.ly_lag;
	%do L=1 %to 12;
		%if &rev=Y %then %do;
			num&L=nump&L-num_one;
			if num&L<%eval(&L)/2 then l_p_&L=.;
			l_r_&L=l_m_p/l_p_&L-1;
			drop l_p_&L l_m_p num&L nump&L num_one;
		%end;
		%if &rev=N %then %do;
			if num&L<%eval(&L)/2 then l_p_&L=.;
			l_r_&L=adj_price/l_p_&L-1;
			drop l_p_&L num&L;
		%end;
	%end;
	if date ne .;
run;
proc sort data=&freq.ly_lag_ret;by &freq._id;quit;
%do L=1 %to 12;
	/*排序*/
	proc rank data=&freq.ly_lag_ret groups=10 out=rank&L;
		by &freq._id;
		var l_r_&L;
		ranks r_&L;
	run;
	data rank&L;set rank&L;
		keep  Stkcd &freq._id r_&L;
	run;
	/*生成winner和loser的三列表*/
	proc sql;
	create table ret_rank&L as
	select a.stkcd,a.&freq._id,a.l_r_&L,b.r_&L
	from &freq.ly_lag_ret as a
	left join rank&L as b on a.stkcd=b.stkcd and a.&freq._id=b.&freq._id;
	quit;
	data ret_wl&L;set ret_rank&L;
		if (r_&L=0)or(r_&L=9);
		drop l_r_&L;
		proc sort;by &freq._id r_&L;
	run;
%end;
proc delete data=	ret_rank1 ret_rank2 ret_rank3 ret_rank4 
					ret_rank5 ret_rank6 ret_rank7 ret_rank8 
					ret_rank9 ret_rank10 ret_rank11 ret_rank12
					rank1 rank2 rank3 rank4 rank5 rank6 
					rank7 rank8 rank9 rank10 rank11 rank12;quit;
%mend;
/***************************************************************************/
%macro holding(freq=);
	/*提出后H个月价格*/
	proc expand data=&freq.ly_all out=&freq.ly_lead method=none;
		%do H=1 %to 12;
			convert  adj_price = le_p_&H / transform=(lead &H);
		%end;
		by stkcd;
	run;
	/*后H个月收益*/
	data &freq.ly_lead_ret;set &freq.ly_lead;
		%do H=1 %to 12;
			le_r_&H=le_p_&H/adj_price-1;
			drop le_p_&H;
		%end;
		if date ne .;
	run;
	/*前L月排序后H个月收益*/
	proc sort data=&freq.ly_lead_ret;by &freq._id stkcd;quit;
	%do L=1 %to 12;
		proc sort data=WORK.RET_WL&L;by &freq._id stkcd;quit;
		data mom_ret&L;
			merge  WORK.RET_WL&L(in=a)  WORK.&freq.LY_LEAD_RET;
			by &freq._id stkcd;
			if a;
		run;
		proc sort data=mom_ret&L;by &freq._id r_&L;quit;
		proc means data=mom_ret&L noprint;
			by &freq._id r_&L;
			var le_r_1 le_r_2 le_r_3 le_r_4
				le_r_5 le_r_6 le_r_7 le_r_8
				le_r_9 le_r_10 le_r_11 le_r_12;
			output out=h_ret&L
			mean(le_r_1)=m_ret&L.1 mean(le_r_2)=m_ret&L.2 mean(le_r_3)=m_ret&L.3 mean(le_r_4)=m_ret&L.4
			mean(le_r_5)=m_ret&L.5 mean(le_r_6)=m_ret&L.6 mean(le_r_7)=m_ret&L.7 mean(le_r_8)=m_ret&L.8
			mean(le_r_9)=m_ret&L.9 mean(le_r_10)=m_ret&L.10 mean(le_r_11)=m_ret&L.11 mean(le_r_12)=m_ret&L.12;
		run;
		data ret&L;set h_ret&L;
			%do H=1 %to 12;
				l_ret&L.&H=lag(m_ret&L.&H);
				ret&L.&H=m_ret&L.&H-l_ret&L.&H;
				drop l_ret&L.&H m_ret&L.&H;
				%if H>1 %then %do;/*持有期H大于1时使用同期多个组合的均值*/
				s&L.&H=0;
					%do j=1 %to %eval(&H);
						s&L.&H=sum(s&L.&H,ret&L.&j);
						%if j=%eval(&H) %then ret&L.&j=s&L.&H/%eval(&H);
					%end;
				drop s&L.&H;
				%end;
			%end;
			if r_&L=9;
			drop  _TYPE_ r_&L;
		run;
	%end;
%mend;
/***************************************************************************/
%macro sort_month(name=,vars=);
data stock_&name;set stock_monthly;
	keep stkcd date year0 month0 month_ret &vars;
run;
/*生成该因子的分位点*/
proc univariate data = stock_&name noprint;
    by year0 month0;
    var &vars;
    output out = breakpoint_&name PCTLPTS = 30 70 PCTLPRE = &vars PCTLNAME = L M H;
run;
data char_&name;
	merge stock_&name(in=a) breakpoint_&name;
	by year0 month0;
	if a;
run;
/*用分位点为股票分组*/
data &name._group;set char_&name;
%let n=1;
%do %until ( %scan(&vars,&n)= );
	%let pvar = %scan(&vars,&n);
	if &pvar>&pvar.M then &pvar._group='H';
	if &pvar.M>&pvar> &pvar.L then &pvar._group='M';
	if &pvar<&pvar.L then &pvar._group='L';
	drop &pvar &pvar.M &pvar.L;
	%let n=%EVAL(&n + 1);
%end;
proc sort;by stkcd year0 month0;
run;
data size_&name;
	merge &name._group(in=a) size_group;
	by stkcd year0;
	if a;
	%let n=1;
	%do %until ( %scan(&vars,&n)= );
		%let pvar = %scan(&vars,&n);
		&pvar.size=cats(&pvar._group,size_group);
		drop &pvar._group;
		if length(&pvar.size)=2;
		%let n=%EVAL(&n + 1);
	%end;
run;
/*生成因子*/
proc sort data=stock_&name;by stkcd year0 month0;run;
%let k=1;
%let HML=;
%do %until ( %scan(&vars,&k)= );
	%let pvar = %scan(&vars,&k);
	data &pvar._&name;set stock_&name;
		keep stkcd date year0 month0 month_ret &pvar;
	run;
	data &pvar._group;set size_&name;
		keep stkcd date year0 month0 &pvar.size;
	run;
	data &pvar;
		merge &pvar._&name &pvar._group(in=a);
		by stkcd year0 month0;
		if a;
		if &pvar ne .;
	run;
	proc sort data=&pvar;by year0 month0 &pvar.size;quit;
	proc means data=&pvar noprint;
		var month_ret;
		by year0 month0 &pvar.size;
		output out=&pvar.stat
		mean(month_ret)=&pvar.factor;
	run;
	
	proc transpose data=&pvar.stat out=HML&pvar;
		id &pvar.size;
		var &pvar.factor;
		by year0 month0;
	run;

	data HML&pvar;set HML&pvar;
		&pvar = 0.5*sum(HB,HS)-0.5*sum(LB,LS);
		drop HB HS MB MS LB LS _NAME_;
	run;
	
	/*
	data HML&pvar;set &pvar.stat;
		if (&pvar.size = 'MB')or(&pvar.size = 'MS') then delete;
		l_f=lag(&pvar.factor);
		&pvar.0=0.5*(l_f+&pvar.factor);
		if (&pvar.size = 'HB')or(&pvar.size = 'LB') then delete;
		l_f0=lag(&pvar.0);
		&pvar=l_f0-&pvar.0;
		if (&pvar.size = 'HS') then delete;
		drop l_f l_f0 &pvar.0 &pvar.size &pvar.factor _FREQ_ _TYPE_;
	run;*/
	%let HML=&HML HML&pvar;
	/*用来保证每个月有六个组合的测试*/
	/*
	proc sort data=&pvar.stat;by year0 month0;run;
	proc means data=&pvar.stat noprint;
		var _FREQ_;
		by year0 month0;
		output out=test
		sum(_FREQ_)=s;
	run;
	data test_&pvar;set test;
		if _FREQ_ ne 6;
	run;
	%let dsid=%sysfunc(open(test_&pvar,i));  
	%let n=%sysfunc(attrn(&dsid,nobs));
	%let rc=%sysfunc(close(&dsid));
	%if %eval(&n)>0 %then %do;
		%put obsnum&pvar=&n;
	%end;*/
	proc delete data=&pvar._&name &pvar._group &pvar &pvar.stat;quit;
	%let k=%EVAL(&k + 1);
%end;
data factor_&name;
	merge &HML;
	by year0 month0;
run;
proc delete data=stock_&name &name._group char_&name breakpoint_&name size_&name &HML;quit;
%mend;
/***************************************************************************/
