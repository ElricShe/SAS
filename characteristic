data ret_based;set ret;
	turnover=trad_share/(liq_size*1000/price);
	amihud=abs(adj_ret*100)/(trading_volume/1000000);
run;

proc sort data=ret;by stkcd year month trddt;quit;
data monthly;set ret;
	by stkcd year month;
	if last.month;
	drop year month;
	rename trddt=date;
run;
data match;set monthly;
	
	size=size*1000;
 
	year0=year(date);
	month0=month(date);
	if year<2003 then do; 	
	 	if month0<5 then do;
	 		year=year0-1;
	 		month=6;
	 	end;
	 
	 	if 4<month0<9 then do;
	 		year=year0-1;
	 		month=12;
	 	end;
	 
	 	if month0>8 then do;
	 		year=year0;
	 		month=6;
	 	end;
 	end;
	if year>2002 then do; 	
	 	if month0<5 then do;
	 		year=year0-1;
	 		month=9;
	 	end;
	 
	 	if 4<month0<9 then do;
	 		year=year0;
	 		month=3;
	 	end;
	 
	 	if 8<month0<11 then do;
	 		year=year0;
	 		month=6;
	 	end;
	 
	 	if month0>10 then do;
	 		year=year0;
	 		month=9;
	 	end;
 	end;
 	drop  adj_price adj_ret liq_size price trad_share;
run;

FILENAME REFFILE '/home/u38414700/factor momentum/data/IAR_Forecdt.txt';

PROC IMPORT DATAFILE=REFFILE 
	DBMS=tab
	OUT=WORK.ear_date
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;
data ear_date;set ear_date;
	year=year(Accper);
	month=month(Accper);
run;
proc expand data=ear_date out=ed  method=none;
	by stkcd;
	convert Accper=l_accper/ transformout=(lead 1);
	convert Actudt=l_actudt/ transformout=(lead 1);
	convert Accper=ll_accper/ transformout=(lead 2);
	convert Actudt=ll_actudt/ transformout=(lead 2);
run;

proc sort data=match;by stkcd year month;quit;
data ret_match;
	merge match(in=a) ed;
	by stkcd year month;
	if a;
run;

data ret_fin;set ret_match;
	if (date>l_actudt)&(date<ll_actudt) then do;
		year=year(l_accper);
		month=month(l_accper);
	end;
	if (date>l_actudt)&(date>=ll_actudt) then do;
		year=year(ll_accper);
		month=month(ll_accper);
	end;
	drop  l_accper l_actudt ll_accper ll_actudt 
	TIME Accper Actudt num month0 year0;
run;
proc delete data=ret_match ear_date ed match;


FILENAME REFFILE '/home/u38414700/factor momentum/data/申万分类.xlsx';

PROC IMPORT DATAFILE=REFFILE
	DBMS=XLSX
	OUT=WORK.sw_class
	REPLACE;
RUN;
data class;set sw_class;
	stkcd=substr(code,1,6)*1;
	if stkcd=. then delete;
run;
/*****************************************************************************************/
/*Finance statement*/
/*****************************************************************************************/

PROC IMPORT OUT= WORK.balance_sheet 
            DATAFILE= "/home/u38414700/factor momentum/data/FS_Combas.txt" 
            DBMS=tab REPLACE;
     GETNAMES=YES; 
     GUESSINGROWS=2000;
RUN;
data balance_sheet;set balance_sheet;if Typrep="A";run;
PROC IMPORT OUT= WORK.profit_statement 
            DATAFILE= "/home/u38414700/factor momentum/data/FS_Comins.txt" 
            DBMS=tab REPLACE;
     GETNAMES=YES; 
     GUESSINGROWS=2000;
RUN;
data profit_statement;set profit_statement;if Typrep="A";run;
PROC IMPORT OUT= WORK.cash_flow_statement
            DATAFILE= "/home/u38414700/factor momentum/data/FS_Comscfd.txt" 
            DBMS=tab REPLACE;
     GETNAMES=YES; 
     GUESSINGROWS=2000;
RUN;
data cash_flow_statement;set cash_flow_statement;if Typrep="A";run;

%rename(dsetin=balance_sheet,vars=A001123000 A001100000 A001212000 A001000000 
A002108000 A002100000 A002206000 A002000000 A003101000 A003103000 
 A003105000 A003000000,varn=inventory current_asset fixed_asset 
 total_asset A_P current_liability long_term_liability 
 total_liability capital_stock surplus_public_accumulation 
 undistributed_profit book_value);
%rename(dsetin=profit_statement,vars=B001100000 B001101000 B001200000 
B001201000 B0f1208000 B001209000 B001210000 B001211000 
B001300000 B001000000 B002100000 B002000000,
varn=income income_1 COGS COGS_1 operation_administrative_expense 
selling_expenses administration_expense  financial_expense  
operating_profit  total_profit tax net_profit);
%rename(dsetin=cash_flow_statement,vars=C001000000 C002006000 C002000000 
C003001000 C003003000 C003002000 C003005000 C003006000
C003007000 C003000000 C007000000 C005000000,
varn=operating_cash_flow capital_expenditure invest_net 
equity_cash_flow bond_cash_flow loan_cash_flow debt_cash_flow coupon_cash_flow
other_payment_cf  borrow_cash_flow other_impact_cash_flow total_cash_flow );

proc sort data=balance_sheet;by stkcd accper;
proc sort data=cash_flow_statement;by stkcd accper;
proc sort data=profit_statement;by stkcd accper;
data financial_statement;
	merge balance_sheet profit_statement cash_flow_statement;
	by stkcd accper;
	
	format Accper date9.;
	year=year(Accper);
	month=month(Accper);
	month_id=(year-1999)*12+month;
	
	
	sales=income_1;
	if income_1=. then sales=income;
	gross_profit=income_1-COGS_1;
	if (gross_profit=.)or((income_1=0)&(COGS_1=0)) then gross_profit=income-COGS;
	expanse=selling_expenses+administration_expense+financial_expense;
	if expanse=. then expanse=operation_administrative_expense ;
	if expanse=. then expanse=gross_profit-operating_profit;
	if tax=. then tax=total_profit-net_profit;
	
	retained_earning=surplus_public_accumulation +undistributed_profit;
	
	financing_debt=bond_cash_flow+loan_cash_flow-debt_cash_flow;
	if financing_debt=. then financing_debt=loan_cash_flow-debt_cash_flow;
	financing_equity=equity_cash_flow-coupon_cash_flow;
	if financing_equity=. then financing_equity=-coupon_cash_flow;

run;

proc delete data=balance_sheet profit_statement cash_flow_statement ;quit;

/*****************************************************************************************/
/*Profitability Factor*/
/*****************************************************************************************/
data profit_raw;set financial_statement;
	keep  Stkcd Accper year month 
	gross_profit operating_profit total_profit net_profit
	book_value total_asset
	sales expanse tax
	operating_cash_flow;
run;
%ttm(dsetin=profit_raw, dsetout=profit_ttm, 
vars=gross_profit operating_profit total_profit net_profit
	sales expanse tax
	operating_cash_flow, code=stkcd, year=year, month=month);
data profit;set profit_ttm;
	g_roe=ttm_gross_profit/book_value;g_roa=ttm_gross_profit/total_asset;
	o_roe=ttm_operating_profit/book_value;o_roa=ttm_operating_profit/total_asset;
	t_roe=ttm_total_profit/book_value;t_roa=ttm_total_profit/total_asset;
	n_roe=ttm_net_profit/book_value;n_roa=ttm_net_profit/total_asset;
	
	g_p_m=gross_profit/ttm_sales;o_p_m=ttm_operating_profit/ttm_sales;
	e_t_a=ttm_expanse/total_asset;e_t_s=ttm_expanse/ttm_sales;
	t_t_a=ttm_tax/total_asset;t_t_s=ttm_tax/ttm_sales;
	
	e_q=(ttm_operating_cash_flow-ttm_operating_profit)/total_asset;
	
	keep  Stkcd Accper year month  
	 t_roa t_roe g_roa g_roe 
	 n_roa n_roe o_roa o_roe
	 o_p_m g_p_m e_t_a e_t_s
	 t_t_a  t_t_s e_q;
run;
/*data "/home/u38414700/factor momentum/data/Profit";set WORK.PROFIT ;run; */
proc delete data=profit_raw profit_ttm;quit;
/*****************************************************************************************/
/*Growth Factor*/
/*****************************************************************************************/
data growth_raw;set financial_statement;
	keep  Stkcd Accper year month month_id
	current_asset fixed_asset total_asset inventory
	current_liability long_term_liability A_P total_liability
	capital_stock retained_earning  book_value
	
	sales gross_profit expanse operating_profit total_profit tax net_profit
	
	operating_cash_flow  invest_net capital_expenditure
	financing_debt financing_equity;
run;
%ttm(dsetin=growth_raw, dsetout=growth_ttm, 
vars=sales gross_profit expanse operating_profit total_profit tax net_profit
	operating_cash_flow  invest_net capital_expenditure
	financing_debt financing_equity, 
	code=stkcd, year=year, month=month);
%sq(dsetin=growth_ttm, dsetout=growth_ttm_sq, 
vars=sales gross_profit expanse operating_profit total_profit tax net_profit
	operating_cash_flow  invest_net capital_expenditure
	financing_debt financing_equity, 
	code=stkcd, year=year, month=month);
%growth(dsetin=growth_ttm_sq, dsetout=growth_ttm_sq_g,
vars=
 inventory current_asset fixed_asset total_asset 
 A_P current_liability long_term_liability total_liability 
 capital_stock book_value retained_earning
 
 ttm_sales ttm_gross_profit ttm_expanse ttm_operating_profit ttm_total_profit ttm_tax ttm_net_profit 
 ttm_operating_cash_flow ttm_invest_net ttm_capital_expenditure ttm_financing_debt ttm_financing_equity 
 
 sq_sales sq_gross_profit sq_expanse sq_operating_profit sq_total_profit sq_tax sq_net_profit 
 sq_operating_cash_flow sq_invest_net sq_capital_expenditure sq_financing_debt sq_financing_equity
, 
code=stkcd, month_id=month_id); 


data growth;set growth_ttm_sq_g;
	sginvg=g_ttm_sales-g_inventory;
	keep Stkcd Accper year month month_id 
 		g_inventory g_current_asset g_fixed_asset g_total_asset 
 		g_A_P g_current_liability g_long_term_liability g_total_liability 
 		g_capital_stock g_book_value g_retained_earning 
 
 		g_ttm_sales g_ttm_gross_profit g_ttm_expanse g_ttm_operating_profit g_ttm_total_profit g_ttm_tax g_ttm_net_profit 
 		g_ttm_operating_cash_flow g_ttm_invest_net g_ttm_ g_ttm_financing_debt g_ttm_financing_equity 
 
 		g_sq_sales g_sq_gross_profit g_sq_expanse g_sq_operating_profit g_sq_total_profit g_sq_tax g_sq_net_profit g_sq_operating_cash_flow 
 		g_sq_invest_net g_sq_capital_expenditure g_sq_financing_debt g_sq_financing_equity
 		
 		sginvg;
 run;
/* data "/home/u38414700/factor momentum/data/Growth";set  WORK.GROWTH ;run; */
 proc delete data= WORK.GROWTH_TTM_SQ WORK.GROWTH_TTM_SQ_G WORK.GROWTH_TTM WORK.GROWTH_RAW;quit;

/*****************************************************************************************/
/*Valuation Factor*/
/*****************************************************************************************/
data valuation_pre_raw;set financial_statement;
	keep Stkcd Accper year month month_id
	book_value 
	gross_profit operating_profit total_profit
	operating_cash_flow total_cash_flow 
	current_asset current_liability;
run;
%ttm(dsetin=valuation_pre_raw, dsetout=valuation_pre_ttm, 
vars=gross_profit operating_profit total_profit
	operating_cash_flow total_cash_flow 
	current_asset current_liability, 
	code=stkcd, year=year, month=month);
%sq(dsetin=valuation_pre_ttm, dsetout=valuation_pre_ttm_sq, 
vars=operating_profit,
	code=stkcd, year=year, month=month);
%growth(dsetin=valuation_pre_ttm_sq, dsetout=valuation_pre_ttm_sq_g,
vars=ttm_operating_profit sq_operating_profit, 
code=stkcd, month_id=month_id); 



proc sort data=valuation_pre_ttm_sq_g;by stkcd year month; 

data size_t;set ret_fin;
	month_id=month_id+1;
	keep stkcd month_id size;
run;
data size;
	merge ret_fin(in=a drop=size) size_t;
	by stkcd month_id;
	if a;
run;

proc sort data=size;by stkcd year month; 
data valuation_pre;
	merge size(in=a)  valuation_pre_ttm_sq_g(drop=month_id);
	by stkcd year month;
	if a;
run;

data valuation_raw;set valuation_pre;
	pb=book_value/size;
	
	g_pe= ttm_gross_profit/size;
	o_pe= ttm_operating_profit/size;
	t_pe= ttm_total_profit/size;
	
	o_pcf= ttm_operating_cash_flow/size;
	t_pcf= ttm_total_cash_flow/size;
	
	tg_peg=(g_ttm_operating_profit*ttm_gross_profit)/size;
	to_peg=(g_ttm_operating_profit*ttm_operating_profit)/size;
	tt_peg=(g_ttm_operating_profit*ttm_total_profit)/size;
	sg_peg=(g_sq_operating_profit*ttm_gross_profit)/size;
	so_peg=(g_sq_operating_profit*ttm_operating_profit)/size;
	st_peg=(g_sq_operating_profit*ttm_total_profit)/size;
	
	pwc=(current_asset-current_liability)/size;
run; 

proc sort data=class;by stkcd;
proc sort data=valuation_raw;by stkcd;
data valuation_raw;
	merge valuation_raw(in=a) class;
	by stkcd;
	if a;
run;

%industry_adjustment(dsetin=valuation_raw, dsetout=valuation, 
vars=pb g_pe o_pe t_pe o_pcf t_pcf tg_peg to_peg tt_peg sg_peg so_peg st_peg pwc,
 week=month_id,class=class);
  
data valuation;set valuation;
	keep Stkcd date size pb month_id code class
 g_pe o_pe t_pe o_pcf t_pcf tg_peg to_peg tt_peg sg_peg so_peg st_peg pwc 
 i1_pb i1_g_pe i1_o_pe i1_t_pe i1_o_pcf i1_t_pcf 
 i1_tg_peg i1_to_peg i1_tt_peg i1_sg_peg i1_so_peg i1_st_peg i1_pwc 
 i2_pb i2_g_pe i2_o_pe i2_t_pe i2_o_pcf i2_t_pcf 
 i2_tg_peg i2_to_peg i2_tt_peg i2_sg_peg i2_so_peg i2_st_peg i2_pwc 
 i3_pb i3_g_pe i3_o_pe i3_t_pe i3_o_pcf i3_t_pcf 
 i3_tg_peg i3_to_peg i3_tt_peg i3_sg_peg i3_so_peg i3_st_peg i3_pwc;
run;
/*data "/home/u38414700/factor momentum/data/Valuation";set WORK.VALUATION ;run; */
proc delete data= WORK.VALUATION_PRE WORK.VALUATION_PRE_RAW WORK.VALUATION_PRE_TTM
 WORK.VALUATION_PRE_TTM_SQ  WORK.VALUATION_PRE_TTM_SQ_G WORK.VALUATION_RAW stat;quit;

/*****************************************************************************************/
/*Asset Factor*/
/*****************************************************************************************/

data asset_raw;set financial_statement;
	keep  Stkcd Accper year month 
	current_asset total_asset fixed_asset inventory
	A_P current_liability long_term_liability total_liability
	capital_stock retained_earning book_value;
run;
data asset;set asset_raw;
	lev_inv=inventory/total_asset;
	lev_cur_a=current_asset/total_asset;
	lev_fix_a=fixed_asset/total_asset;
	lev_ap=A_P/total_asset;
	lev_cur_l=current_liability/total_asset;
	lev_longd=long_term_liability/total_asset;
	lev_tot_l=total_liability/total_asset;
	lev_stock=capital_stock/total_asset;
	lev_re=retained_earning/total_asset;
	lev_tot_e=book_value/total_asset;
run;
data asset;set asset;
	drop current_asset total_asset fixed_asset inventory
	A_P current_liability long_term_liability total_liability
	capital_stock retained_earning book_value ;
run;
/*data "/home/u38414700/factor momentum/data/asset";set WORK.ASSET;run;*/
proc delete data=asset_raw;quit;
/*****************************************************************************************/
/*Liquidity Factor*/
/*****************************************************************************************/
data date;set ret_based;
	keep trddt;
run;
proc sort data=date nodupkey;by trddt;quit;
data code;set ret_based;
	keep stkcd;
run;
proc sort data=code nodupkey;by stkcd;quit;
proc sql;
 create table dc as
 select *
 from code,date;
quit; 
proc sort data= ret_based;by stkcd trddt;quit;
proc sort data= dc;by stkcd trddt;quit;
data trade_data;
	merge ret_based dc(in=a);
	by stkcd trddt;
	if a;
run;

data trade_data1;set trade_data;
	if stkcd<=650;
run;
data trade_data2;set trade_data;
	if 650<stkcd<=1500;
run;
data trade_data3;set trade_data;
	if 1500<stkcd<=2250;
run;
data trade_data4;set trade_data;
	if 2250<stkcd<=2500;
run;
data trade_data5;set trade_data;
	if 2500<stkcd<=2750;
run;
data trade_data6;set trade_data;
	if 2750<stkcd<=600050;
run;
data trade_data7;set trade_data;
	if 600050<stkcd<=600350;
run;
data trade_data8;set trade_data;
	if 600350<stkcd<=600650;
run;
data trade_data9;set trade_data;
	if 600650<stkcd<=600900;
run;
data trade_data10;set trade_data;
	if 600900<stkcd<=602000;
run;
data trade_data11;set trade_data;
	if 602000<stkcd<=603600;
run;
data trade_data12;set trade_data;
	if 603600<stkcd;
run;
%liquidity(datain=trade_data1,dataout=liquidity1);
%liquidity(datain=trade_data2,dataout=liquidity2);
%liquidity(datain=trade_data3,dataout=liquidity3);
%liquidity(datain=trade_data4,dataout=liquidity4);
%liquidity(datain=trade_data5,dataout=liquidity5);
%liquidity(datain=trade_data6,dataout=liquidity6);
%liquidity(datain=trade_data7,dataout=liquidity7);
%liquidity(datain=trade_data8,dataout=liquidity8);
%liquidity(datain=trade_data9,dataout=liquidity9);
%liquidity(datain=trade_data10,dataout=liquidity10);
%liquidity(datain=trade_data11,dataout=liquidity11);
%liquidity(datain=trade_data12,dataout=liquidity12);


proc sort data=ret;by stkcd year month trddt;quit;
data monthly;set ret;
	by stkcd year month;
	if last.month;
	keep Stkcd year month Trddt;
run;
%macro sort_liq;
	%do i=1 %to 12;
		proc sort data=liquidity&i;by stkcd year month trddt;quit;
	%end;
%mend;
%sort_liq;
data liquidity_1;
	merge monthly(in=a) liquidity1 liquidity2 liquidity3 liquidity4 liquidity5;
 	 by stkcd year month trddt;
 	 if a;
run;
data monthly_2;set liquidity_1;
	if TIME=.;
	keep Stkcd year month Trddt;
run;
data liquidity_1;set liquidity_1;
	if TIME ne .;
run;
data liquidity_2;
	merge monthly_2(in=a) liquidity6 liquidity7 liquidity8  liquidity9 liquidity10 liquidity11 liquidity12;
 	 by stkcd year month trddt;
 	 if a;
run;

data liquidity;set liquidity_1 liquidity_2;	 
	keep stkcd trddt 
turnover_1 trading_volume_1 amihud_1 
turnover_2 trading_volume_2 amihud_2  
turnover_3 trading_volume_3 amihud_3 
turnover_4 trading_volume_4 amihud_4   

turnover_std_1 trading_volume_std_1 amihud_std_1 
turnover_std_2 trading_volume_std_2 amihud_std_2 
turnover_std_3 trading_volume_std_3 amihud_std_3 
turnover_std_4 trading_volume_std_4 amihud_std_4

 c1_turnover_1 c2_turnover_1 c3_turnover_1 
 c1_trading_volume_1 c2_trading_volume_1 c3_trading_volume_1 
 c1_amihud_1 c2_amihud_1 c3_amihud_1 

 c1_turnover_2 c2_turnover_2 c3_turnover_2 
 c1_trading_volume_2 c2_trading_volume_2 c3_trading_volume_2 
 c1_amihud_2 c2_amihud_2 c3_amihud_2 

 c1_turnover_3 c2_turnover_3 c3_turnover_3 
 c1_trading_volume_3 c2_trading_volume_3 c3_trading_volume_3 
 c1_amihud_3 c2_amihud_3 c3_amihud_3 

 c1_turnover_4 c2_turnover_4 c3_turnover_4 
 c1_trading_volume_4 c2_trading_volume_4 c3_trading_volume_4 
 c1_amihud_4 c2_amihud_4 c3_amihud_4;
run;
data "/home/u38414700/factor momentum/data/liquidity";set  WORK.liquidity;run;
proc delete data=liquidity1 liquidity2 liquidity3 liquidity4 liquidity5 liquidity6
				 liquidity7 liquidity8  liquidity9 liquidity10 liquidity11 liquidity12
				 MOV_AVR120 liquidity_1 liquidity_2 WORK.CODE WORK.DATE WORK.DC;quit;
/*****************************************************************************************/
/*Risk Factor*/
/*****************************************************************************************/
data trade_ret;set trade_data; 
 rename adj_ret=ret; 
 month_id=(year-2000)*12+month;
 drop  trad_share trading_volume turnover price liq_size amihud;
run;

	proc expand data=trade_ret out=trade_std method=none;
		convert ret = std60 / transform=(movstd 60);
		convert ret = std120 / transform=(movstd 120);
		convert ret = std240 / transform=(movstd 240);
	
		convert num = num60 / transform=(movsum 60);
		convert num = num120 / transform=(movsum 120);
		convert num = num240 / transform=(movsum 240);
	by stkcd;
	run;

proc sort data=trade_std;by stkcd month_id;quit;
data trade_std; set trade_std;
	by stkcd month_id;
	if last.month_id;
	if num=. then do;
		std60=.;std120=.;std240=.;
		num60=.;num120=.;num240=.;
	end;
	if num60<30 then std60=.;
	if num120<60 then std120=.;
	if num240<120 then std240=.;
	drop num60 num120 num240 ret num time;
run;

proc import
  datafile="/home/u38414700/factor momentum/data/index.csv"
  dbms=csv
  out=work.index
  replace;
run;

data index; set index; format date date9.; proc sort; by date; quit;
data index;set index;ret_m=(snh_index/lag_p_c-1)*100;run;


proc sql; 
	create table trade_ret2 
	as select a.*, b.ret_m, c.class 
	from trade_ret as a
	left join index as b on a.trddt=b.date
	left join class as c on a.stkcd=c.stkcd;
quit;

proc sort data=trade_ret2; by trddt class;quit;
proc means data=trade_ret2 noprint;
	var ret;
	output out=ret_ind_mean
	mean=ret_i;
	by trddt class;
run;
data trade_ret3;
	merge trade_ret2 ret_ind_mean;
	by trddt class;
	drop _type_ _freq_ class;
proc sort;by trddt; run;

data day_id;set trade_ret3; keep trddt; proc sort nodupkey;by trddt;run;
data day_id;set day_id;day_id=_n_;run;

data trade_ret4;
	merge trade_ret3(in=a) day_id;
	by trddt; if a=1;
	rename trddt=date;
run;
proc sort data=trade_ret4;by stkcd date; run;

proc sort data=trade_ret; by month_id descending trddt ;run;
data trade_month;set trade_ret;
	by month_id; 
	if first.month_id;
	if month_id ne .;
	keep  Trddt month month_id;
run;

proc sql; 
	create table trade_month2 
	as select a.*, b.day_id
	from trade_month as a
	left join day_id as b on a.trddt=b.trddt;
quit;	
data trade_month2; set trade_month2;rename trddt=date;

/*i=240变量出现内存不足问题
%beta(dsetin=trade_ret4,dsetin_dayid=trade_month2,dsetout=trade_beta);
data "/home/u38414700/factor momentum/data/trade_beta";set trade_beta;run;
*/			  


proc sort data=trade_beta;by date;quit;
proc sql; 
	create table trade_beta2 
	as select a.*, b.month_id
	from trade_beta as a
	left join trade_month as b on a.date=b.trddt;
quit;
proc sort data=trade_beta2;by stkcd month_id;quit;


data risk;
	merge trade_beta2(in=a) trade_std(drop= trddt adj_price);
	by stkcd month_id;
	if a;
	if month_id^=.;
	drop month_id size year month;
run;
/*data "/home/u38414700/factor momentum/data/characteristic/risk";set risk;run;*/
proc delete data=WORK.REG_TEMP1 WORK.REG_TEMP2 WORK.REG_TEMP3
				  WORK.RESULT60  WORK.RESULT120 WORK.RESULT240
				  WORK.TEMP  WORK.TEMP1 WORK.TEMP2 WORK.TEMP3 WORK.TEMP4
				  WORK.RET_IND_MEAN  WORK.TRADE_MONTH WORK.TRADE_MONTH2
				   WORK.TRADE_BETA WORK.TRADE_BETA2 WORK.RET WORK.RET_BASED
				    WORK.TRADE_MONTH WORK.TRADE_MONTH2  WORK.TRADE_RET  WORK.TRADE_RETURN
				     WORK.TRADE_STD;quit;
/*****************************************************************************************/
/*Return Factor*/
/*****************************************************************************************/
/*
data trade_ret41;set trade_ret4;
	if stkcd<=650;
	drop  adj_price month month_id size year;
run;
data trade_ret42;set trade_ret4;
	if 650<stkcd<=1500;
	drop  adj_price month month_id size year;
run;
data trade_ret43;set trade_ret4;
	if 1500<stkcd<=2250;
	drop  adj_price month month_id size year;
run;
data trade_ret44;set trade_ret4;
	if 2250<stkcd<=2500;
	drop  adj_price month month_id size year;
run;
data trade_ret45;set trade_ret4;
	if 2500<stkcd<=2750;
	drop  adj_price month month_id size year;
run;
data trade_ret46;set trade_ret4;
	if 2750<stkcd<=600050;
	drop  adj_price month month_id size year;
run;
data trade_ret47;set trade_ret4;
	if 600050<stkcd<=600350;
	drop  adj_price month month_id size year;
run;
data trade_ret48;set trade_ret4;
	if 600350<stkcd<=600650;
	drop  adj_price month month_id size year;
run;
data trade_ret49;set trade_ret4;
	if 600650<stkcd<=600900;
	drop  adj_price month month_id size year;
run;
data trade_ret410;set trade_ret4;
	if 600900<stkcd<=602000;
	drop  adj_price month month_id size year;
run;
data trade_ret411;set trade_ret4;
	if 602000<stkcd<=603600;
	drop  adj_price month month_id size year;
run;
data trade_ret412;set trade_ret4;
	if 603600<stkcd;
	drop  adj_price month month_id size year;
run;
%macro momentum;
	%do k=1 %to 12;
	%return_data(dsetin=trade_ret4&k,dsetin_dayid=trade_month2,dsetout=trade_return&k);
	proc sort data=trade_return&k;by stkcd day_id;run;
	proc delete data=trade_ret4&k;run;
	%end;
%mend;
%momentum;

proc sort data=monthly;by trddt;run;
data monthly_3;
	merge monthly(in=a) day_id;
	by trddt;
	if a;
run;
proc sort data=monthly_3;by stkcd day_id;run;
data trade_return;
	merge monthly_3(in=a) trade_return1 trade_return2 trade_return3 trade_return4 trade_return5
						trade_return6 trade_return7 trade_return8 trade_return9 trade_return10
						trade_return11 trade_return12;
 	 by stkcd day_id;
 	 if a;
run;
data "/home/u38414700/factor momentum/data/return";set trade_return;run;

proc delete data= WORK.TRADE_RETURN1 WORK.TRADE_RETURN2 WORK.TRADE_RETURN3 WORK.TRADE_RETURN4
				  WORK.TRADE_RETURN5 WORK.TRADE_RETURN6 WORK.TRADE_RETURN7 WORK.TRADE_RETURN8
				  WORK.TRADE_RETURN9 WORK.TRADE_RETURN10 WORK.TRADE_RETURN11 WORK.TRADE_RETURN12
				  WORK.TEMP_REG WORK.TEMP_RES WORK.MOV_NUM WORK.MOV_RES WORK.MOV_RET 
				  WORK.MONTHLY WORK.MONTHLY_2 WORK.MONTHLY_3  WORK.TRADE_RET4;
				  run;
*/ 
data return ;set "/home/u38414700/factor momentum/data/return" ;run;

/*****************************************************************************************/
/*Max Return*/
/*****************************************************************************************/
data ret_max;set ret;
	keep  Stkcd year month adj_ret;
run;
proc means data=ret_max noprint;
	var adj_ret;
	by stkcd year month;
	output out=max_ret
	max=max_ret;
run;
data "/home/u38414700/factor momentum/data/max_ret";set max_ret;
	drop _TYPE_ _FREQ_;
run;
