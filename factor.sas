proc sort data=ret;by stkcd year month trddt;quit;
data monthly;set ret;
	by stkcd year month;
	if last.month;
	drop year month;
	rename trddt=date;
run;
data size;set monthly;
	
	size=size*1000;
 
	year0=year(date);
	month0=month(date);
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
 	drop trading_volume adj_ret liq_size price trad_share;
run;
data price;set size;
	l_p=lag(adj_price);
	month_ret=adj_price/l_p-1;
run;
data month_ret;set price;
	by stkcd year0 month0;
	if first.stkcd then month_ret=.;
	drop l_p adj_price;
run;
	



data asset; set "/home/u38414700/factor momentum/data/characteristic/asset.sas7bdat";
drop Accper;proc sort;by stkcd year month;run;
data profit; set "/home/u38414700/factor momentum/data/characteristic/profit.sas7bdat";
drop Accper;proc sort;by stkcd year month;run;
data growth; set "/home/u38414700/factor momentum/data/characteristic/growth.sas7bdat";
drop Accper month_id;proc sort;by stkcd year month;run;
proc sort data=month_ret;by stkcd year month;quit;
data char;
	merge month_ret(in=a) asset profit growth;
	by stkcd year month;
	if a;
run;

data valuation; set "/home/u38414700/factor momentum/data/characteristic/valuation.sas7bdat";
drop month_id;proc sort;by stkcd date;run;
data liquidity; set "/home/u38414700/factor momentum/data/characteristic/liqudity.sas7bdat";
rename trddt=date;proc sort;by stkcd date;run;
data risk; set "/home/u38414700/factor momentum/data/characteristic/risk.sas7bdat";
proc sort;by stkcd date;run;
proc sort data=char;by stkcd date;quit;
data chars_raw;
	merge char(in=a) valuation liquidity risk;
	by stkcd date;
	if a;
run;

proc delete data= WORK.ASSET  WORK.GROWTH WORK.liquidity WORK.PROFIT 
				 WORK.RISK WORK.VALUATION;quit;
proc sort data=chars_raw; by stkcd date;quit;
data chars;set chars_raw;
array trimvars{115}
	
 	pb g_pe o_pe t_pe o_pcf t_pcf sg_peg so_peg st_peg pwc 
 	i1_pb i1_g_pe i1_o_pe i1_t_pe i1_o_pcf i1_t_pcf i1_sg_peg i1_so_peg i1_st_peg i1_pwc 
	i2_pb i2_g_pe i2_o_pe i2_t_pe i2_o_pcf i2_t_pcf i2_sg_peg i2_so_peg i2_st_peg i2_pwc 
	i3_pb i3_g_pe i3_o_pe i3_t_pe i3_o_pcf i3_t_pcf i3_sg_peg i3_so_peg i3_st_peg i3_pwc 
 
 	turnover_1 trading_volume_1 amihud_1 
 	turnover_2 trading_volume_2 amihud_2 
 	turnover_3 trading_volume_3 amihud_3 
 	turnover_4 trading_volume_4 amihud_4 
 	ln_turnover_1 c1_turnover_1 c2_turnover_1 c3_turnover_1 
 	ln_trading_volume_1 c1_trading_volume_1 c2_trading_volume_1 c3_trading_volume_1 
 	ln_amihud_1 c1_amihud_1 c2_amihud_1 c3_amihud_1 
 	ln_turnover_2 c1_turnover_2 c2_turnover_2 c3_turnover_2 
 	ln_trading_volume_2 c1_trading_volume_2 c2_trading_volume_2 c3_trading_volume_2 
 	ln_amihud_2 c1_amihud_2 c2_amihud_2 c3_amihud_2 
 	ln_turnover_3 c1_turnover_3 c2_turnover_3 c3_turnover_3 
 	ln_trading_volume_3 c1_trading_volume_3 c2_trading_volume_3 c3_trading_volume_3 
 	ln_amihud_3 c1_amihud_3 c2_amihud_3 c3_amihud_3 
 	ln_turnover_4 c1_turnover_4 c2_turnover_4 c3_turnover_4 
 	ln_trading_volume_4 c1_trading_volume_4 c2_trading_volume_4 c3_trading_volume_4 
 	ln_amihud_4 c1_amihud_4 c2_amihud_4 c3_amihud_4  	
 
 	mse60 beta60 beta_p60 beta_n60 
 	mse120 beta120 beta_p120 beta_n120 
 	mse240 beta240 beta_p240 beta_n240 
 	std60 std120 std240;
		do xi = 1 to dim(trimvars);
            trimvars{xi}=lag(trimvars{xi});
			by stkcd date;
			if first.stkcd then trimvars{xi}=.;
		end;
	drop xi;
	if year0>2002;
	if (p_y>119)&(p_m>14)&(ld=.);
run;
/*%missing(dsetin=chars_raw, dsetout=missing);
*/
/*
%macro missing_year;
	%do i=0 %to 19;
		data chars&i;set chars_raw;
			if year0=%eval(2000+&i);
		run;
		%missing(dsetin=chars&i, dsetout=missing&i);
	%end;
%mend;
%missing_year;	
*/		
%let asset=lev_inv lev_cur_a lev_fix_a lev_ap lev_cur_l 
			   lev_longd lev_tot_l lev_stock lev_re lev_tot_e;
%let    profit=g_roe g_roa o_roe o_roa t_roe t_roa n_roe n_roa 
			   g_p_m o_p_m e_t_a e_t_s t_t_a t_t_s e_q; 
%let	growth=g_inventory g_current_asset g_fixed_asset g_total_asset g_A_P g_current_liability g_long_term_liability 
				 g_total_liability g_capital_stock g_book_value g_retained_earning g_ttm_sales g_ttm_gross_profit g_ttm_expanse 
				 g_ttm_operating_profit g_ttm_total_profit g_ttm_tax g_ttm_net_profit g_ttm_operating_cash_flow g_ttm_invest_net 
				 g_ttm_financing_debt g_ttm_financing_equity g_sq_sales g_sq_gross_profit g_sq_expanse g_sq_operating_profit g_sq_total_profit 
				 g_sq_tax g_sq_net_profit g_sq_operating_cash_flow g_sq_invest_net g_sq_capital_expenditure g_sq_financing_debt g_sq_financing_equity;
 			   
%let valuation=pb g_pe o_pe t_pe o_pcf t_pcf tg_peg to_peg tt_peg sg_peg so_peg st_peg pwc 
			   i1_pb i1_g_pe i1_o_pe i1_t_pe i1_o_pcf i1_t_pcf i1_tg_peg i1_to_peg i1_tt_peg i1_sg_peg i1_so_peg i1_st_peg i1_pwc 
			   i2_pb i2_g_pe i2_o_pe i2_t_pe i2_o_pcf i2_t_pcf i2_tg_peg i2_to_peg i2_tt_peg i2_sg_peg i2_so_peg i2_st_peg i2_pwc 
			   i3_pb i3_g_pe i3_o_pe i3_t_pe i3_o_pcf i3_t_pcf i3_tg_peg i3_to_peg i3_tt_peg i3_sg_peg i3_so_peg i3_st_peg i3_pwc; 
 
%let   liquidity=turnover_1 trading_volume_1 amihud_1 
 				turnover_2 trading_volume_2 amihud_2 
 				turnover_3 trading_volume_3 amihud_3 
 				turnover_4 trading_volume_4 amihud_4 
 				ln_turnover_1 c1_turnover_1 c2_turnover_1 c3_turnover_1 
 				ln_trading_volume_1 c1_trading_volume_1 c2_trading_volume_1 c3_trading_volume_1 
 				ln_amihud_1 c1_amihud_1 c2_amihud_1 c3_amihud_1 
 				ln_turnover_2 c1_turnover_2 c2_turnover_2 c3_turnover_2 
 				ln_trading_volume_2 c1_trading_volume_2 c2_trading_volume_2 c3_trading_volume_2 
 				ln_amihud_2 c1_amihud_2 c2_amihud_2 c3_amihud_2 
 				ln_turnover_3 c1_turnover_3 c2_turnover_3 c3_turnover_3 
 				ln_trading_volume_3 c1_trading_volume_3 c2_trading_volume_3 c3_trading_volume_3 
 				ln_amihud_3 c1_amihud_3 c2_amihud_3 c3_amihud_3 
 				ln_turnover_4 c1_turnover_4 c2_turnover_4 c3_turnover_4 
 				ln_trading_volume_4 c1_trading_volume_4 c2_trading_volume_4 c3_trading_volume_4 
 				ln_amihud_4 c1_amihud_4 c2_amihud_4 c3_amihud_4; 
 				
%let 	   risk=mse60 beta60 beta_p60 beta_n60 
				mse120 beta120 beta_p120 beta_n120 
				mse240 beta240 beta_p240 beta_n240 
 				std60 std120 std240; 
 				
proc sort data=chars; by year0 stkcd month0;
data stock_monthly;set chars;
	proc sort; by year0 month0;
run;
/*市值分组*/
data stock_yearly;set chars;
	by year0 stkcd;
	if last.stkcd;
run;
proc univariate data = stock_yearly noprint;
    by year0;
    var size;
    output out = breakpoint_size PCTLPTS = 50 PCTLPRE = size PCTLNAME = S B;
run;
data size_year;
	merge stock_yearly(in=a) breakpoint_size;
	by year0;
	if a;
	keep date size sizeS Stkcd year0 month0 year month;
run;
proc sort data=size_year;by stkcd year0;quit;
data size_group;set size_year;
	by stkcd year0;
	if size> sizeS then size_group='B';else size_group='S';
	keep  Stkcd year0 month0 year month size_group;
run;	
data size_group;set size_group;
	year0=year0+1;
	if year0<2020;
	keep stkcd year0 size_group;
run;
proc delete data= size_year;

%sort_month(name=valuation,vars=&valuation);
%sort_month(name=liquidity,vars=&liquidity);
%sort_month(name=risk,vars=&risk);

%sort_month(name=asset,vars=&asset);
%sort_month(name=profit,vars=&profit);
%sort_month(name=growth,vars=&growth);

data "/home/u38414700/factor momentum/data/factor";
	merge factor_asset factor_profit factor_growth 
		  factor_valuation factor_liquidity factor_risk;
	by year0 month0;
run;
