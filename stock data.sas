FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret0
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;

FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr1.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret1
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;

FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr2.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret2
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;

FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr3.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret3
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;
FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr4.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret4
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;
FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr5.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret5
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;
FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr6.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret6
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;
FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr7.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret7
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;
FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr8.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret8
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;

FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr9.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret9
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;

FILENAME REFFILE '/home/u38414700/factor momentum/data/ret/TRD_Dalyr10.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.ret10
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;

data ret_raw;set ret0 ret1 ret2 ret3 ret4 ret5 ret6 ret7 ret8 ret9 ret10;
	year=year(Trddt);
	month=month(Trddt);
	month_id=(year-1999)*12+month;
	num=1;
run;
%rename(dsetin=ret_raw, vars= Adjprcwd Clsprc Dnshrtrd Dnvaltrd Dretwd Dsmvosd Dsmvtll, 
varn=adj_price price trad_share trading_volume adj_ret liq_size size);

/*monthly*/
proc sort data=ret_raw;by stkcd year month trddt;quit;
data monthly;set ret_raw;
	by stkcd year month;
	if last.month;
	rename trddt=date;
run;
/*month return*/
data price;set monthly;
	l_p=lag(adj_price);
	month_ret=adj_price/l_p-1;
run;
data month_ret;set price;
	by stkcd year month;
	if first.stkcd then month_ret=.;
	keep stkcd date year month month_ret;
run;
proc delete data=price;quit;

/*根据市值排序*/
proc sort data=monthly;by year month;quit;
proc rank data=monthly groups=10 out=size_rank_r;
	by year month; 
	var size;
	ranks size_rank;
run;
data size_rank;set size_rank_r;
	month_id=month_id+1;
	if month_id<247;
	keep stkcd month_id size_rank;
	proc sort;by stkcd month_id;
run;
proc delete data=size_rank_r;quit;

/*标记上市不到一年的股票*/
FILENAME REFFILE '/home/u38414700/factor momentum/data/IPO_Ipoday.txt';

PROC IMPORT DATAFILE=REFFILE
	DBMS=tab
	OUT=WORK.list
	REPLACE;
	GETNAMES=YES;
	GUESSINGROWS=2000;
RUN;

proc sort data=ret_raw; by stkcd;quit;
data ld;
	merge list ret_raw(in=a);
	by stkcd;
	if a;
	ld=trddt-listdt;
	if ld<183;
	keep stkcd trddt ld;
	rename trddt=date;
run;

/*统计上一年/上一月交易日*/
data date;set ret_raw;
	keep trddt;
run;
proc sort data=date nodupkey;by trddt;quit;
data code;set ret_raw;
	keep stkcd;
run;
proc sort data=code nodupkey;by stkcd;quit;
proc sql;
 create table dc as
 select *
 from code,date;
quit; 
proc sort data= ret_raw;by stkcd trddt;quit;
proc sort data= dc;by stkcd trddt;quit;
data trade_data;
	merge ret_raw dc(in=a);
	by stkcd trddt;
	if a;
run;

proc expand data=trade_data out=count method=none;
	convert  num= p_y / transform=(lag 1 movsum 250);/*交易年*/
	convert  num= p_m / transform=(lag 1 movsum 20);/*交易月*/ 
	by stkcd;
run;
data count_past;set count;
	if price ne .;
	keep  Stkcd Trddt p_m p_y;
	rename trddt=date;
run;
proc delete data=date code dc trade_data count;quit;
/*
data count_past_month;set count_past;
	by stkcd year month;
	if last.month;
run;
proc export data = count_past_month
outfile = "/home/u38414700/factor momentum/data/count_past_month.csv"
dbms = dlm;
delimiter = '09'x;
run;
*/
/*可选股票*/
data size;set monthly;
	size=size*1000;
	month_id=month_id+1;
	keep stkcd size month_id;
run;

proc sort data=monthly;by stkcd date;quit;
data oldstock;
	merge monthly(in=a) ld count_past month_ret(drop=year month);
	by stkcd date;
	if a&(ld = .)&(p_y>119)&(p_m>14);
	drop ld p_y p_m;
run;
data selected;
	merge oldstock(in=a) size_rank;
	by stkcd month_id;
	if a&(size_rank>2);
	drop size_rank adj_price adj_ret 
	liq_size num price size 
	trad_share trading_volume;
run;
proc delete data=ld count_past size_rank monthly oldstock;quit;

data ret;set ret_raw;run;
	

proc delete data=ret0 ret1 ret2 ret3 ret4 ret5 ret6 ret7 ret8 ret9 ret10
				 WORK.LIST WORK.RET_RAW WORK.MONTH_RET WORK.SIZE;quit;

