%% @author Владимир Щербина <vns.scherbina@gmail.com>
%% @copyright 2011 Владимир Щербина
%% @version 1.0.0
%% @doc Модуль {@module} реализует хранилище временных рядов.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(tss).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export(
	[
		store/3,load/4,delete/2,list_series/0,get_last_tick_timestamp/1,
		get_last_timestamp/2,get_first_timestamp/2,ts2str/1,
		new_series/0,update_series/2,store_series/1,store_series_parallel/1,
		export_to_csv/1
	]
).

-include_lib("kernel/include/file.hrl").


-define(seriesNamesFileName,"series_names.dat").
-define(dataDir,"data").
-define(freeSpaceRefSize,8).
-define(seriePeriodSize,4).
-define(serieNameSize,256).
-define(indexSize,200*8).
-define(indexOffset,?freeSpaceRefSize+?seriePeriodSize+?serieNameSize).
-define(headerSize,?freeSpaceRefSize+?seriePeriodSize+?serieNameSize+?indexSize).
-define(emptyValue,-1.0e300).
-define(zeroValue,+1.0e300).
-define(block(Size,Value),list_to_binary(string:left("",Size,Value))).
-define(emptyBlock(Size),
	begin
		Value=0.0,
		ValueBin=list_to_binary([<<Value:8/little-float-unit:8>>]),
		binary:copy(ValueBin,Size)
	end
).
-define(replace(Str,SubStr,SubStr2),lists:flatten(io_lib:format("~s",[re:replace(Str,SubStr,SubStr2,[global])]))).

-record(config,{
	path_to_files="",
	names=gb_sets:empty()
}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
%%%%%%%%%%%%%%%%%%%%%%
%%% public функции %%%
%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec ({s,SerieName},{i,SeriePeriod},{matrix,Data}) -> {ok,Status}
%% @doc Функция сохранение значений Data серии SerieName с периодом SeriePeriod.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
store(
	{s,SerieName},
	{i,SeriePeriod},
	{matrix,Data}
) ->
	case Data of
		[] ->
			ok;
		[[]] ->
			ok;
		_ ->
			case catch(gen_server:call(?MODULE,{check_file,SerieName,SeriePeriod,true},infinity)) of
				{ok,FileName} ->
					catch(store_data(FileName,SeriePeriod,Data));
				_ ->
					error
			end
	end,
    {ok,{s,"Stored."}};
store(_,_,_) ->
	{ok,{s,"Stored."}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec ({s,SerieName},{i,SeriePeriod},Start,Finish) -> {ok,{matrix,Data}}
%% @doc Функция чтения значений серии SerieName с периодом SeriePeriod
%% на интервале от Start до Finish включительно.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
load(
	{s,SerieName},
	{i,SeriePeriod},
	{ts,_StartYear,_StartMonth,_StartDay,_StartHour,_StartMinute,_StartSecond}=Start,
	{ts,_FinishYear,_FinishMonth,_FinishDay,_FinishHour,_FinishMinute,_FinishSecond}=Finish
) ->
	case catch(gen_server:call(?MODULE,{check_file,SerieName,SeriePeriod,false},infinity)) of
		{ok,FileName} ->
			case catch(load_data(FileName,SeriePeriod,Start,Finish)) of
				{ok,{matrix,Mat}} ->
					{ok,{matrix,Mat}};
				_ ->
					{ok,{matrix,[[]]}}
			end;
		_ ->
			{ok,{matrix,[[]]}}
	end;
load(_,_,_,_) ->
	{ok,{matrix,[[]]}}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec ({s,SerieName},{i,SeriePeriod}) -> {ok,Status}
%% @doc Функция удаления серии SerieName с периодом SeriePeriod.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
delete({s,SerieName},{i,SeriePeriod}) ->
	(catch gen_server:call(?MODULE,{delete,SerieName,SeriePeriod},infinity)),
	{ok,{s,"Deleted."}};
delete(_,_) ->
	{ok,{s,"Deleted."}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec () -> {ok,{matrix,Series}}
%% @doc Функция получения списка серий.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
list_series() ->
	{ok,Series}=gen_server:call(?MODULE,list_series,infinity),
	{ok,{matrix,Series}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec (Ticker) -> {ok,Timestamp}
%% @doc Функция определение последнего по времени значения серии.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_last_tick_timestamp({ticker,Ticker}) ->
    TS=ts_now(),
    SerieName=lists:flatten([Ticker,".PRICE"]),
    get_last_tick_timestamp({s,SerieName},TS,add_seconds(TS,-86400*31)).

get_last_tick_timestamp(SerieName,TS,FinishTS) ->
    case gt(TS,FinishTS) of
        true ->
            {ts,Year,Month,Day,_,_,_}=TS,
            case load(SerieName,{i,60},{ts,Year,Month,Day,0,0,0},{ts,Year,Month,Day,23,59,59}) of
                {ok,{matrix,[[]]}} ->
                    get_last_tick_timestamp(SerieName,add_seconds({ts,Year,Month,Day,0,0,0},-86400),FinishTS);
                {ok,{matrix,Data}} ->
                    [[LastTimestamp,_]|_]=lists:reverse(Data),
                    {ok,LastTimestamp};
                _Other ->
                    io:format("~p~n",[_Other]),
                    {ok,ts_now()}
            end;
        false ->
            {ok,FinishTS}
    end.

get_last_timestamp(
	{s,SerieName},
	{i,SeriePeriod}) ->
	case catch(gen_server:call(?MODULE,{check_file,SerieName,SeriePeriod,false},infinity)) of
		{ok,FileName} ->
			case catch(find_last_timestamp(FileName,SerieName,SeriePeriod)) of
				{ok,{ts,Year,Month,Day,Hour,Minute,Second}} ->
					{ok,{ts,Year,Month,Day,Hour,Minute,Second}};
				_ ->
					{ok,{ts,1970,1,1,0,0,0}}
			end;
		_ ->
			{ok,{ts,1970,1,1,0,0,0}}
	end;
get_last_timestamp(_,_) ->
	{ok,{ts,1970,1,1,0,0,0}}.

get_first_timestamp(
	{s,SerieName},
	{i,SeriePeriod}) ->
	case catch(gen_server:call(?MODULE,{check_file,SerieName,SeriePeriod,false},infinity)) of
		{ok,FileName} ->
			case catch(find_first_timestamp(FileName,SerieName,SeriePeriod)) of
				{ok,{ts,Year,Month,Day,Hour,Minute,Second}} ->
					{ok,{ts,Year,Month,Day,Hour,Minute,Second}};
				_ ->
					{ok,{ts,1970,1,1,0,0,0}}
			end;
		_ ->
			{ok,{ts,1970,1,1,0,0,0}}
	end;
get_first_timestamp(_,_) ->
	{ok,{ts,1970,1,1,0,0,0}}.
	
find_last_timestamp(FileName,SerieName,SeriePeriod) ->
	{ok,File}=file:open(FileName,[read,raw,binary,{read_ahead,86400*8}]),
	Result=case catch(find_finish(File,SerieName,SeriePeriod)) of
		{ok,{ts,Year,Month,Day,Hour,Minute,Second}} ->
			{ok,{ts,Year,Month,Day,Hour,Minute,Second}};
		_ ->
			{ok,{ts,1970,1,1,0,0,0}}
	end,
	ok=file:close(File),
	Result.

find_first_timestamp(FileName,SerieName,SeriePeriod) ->
	{ok,File}=file:open(FileName,[read,raw,binary,{read_ahead,86400*8}]),
	Result=case catch(find_start(File,SerieName,SeriePeriod)) of
		{ok,{ts,Year,Month,Day,Hour,Minute,Second}} ->
			{ok,{ts,Year,Month,Day,Hour,Minute,Second}};
		_ ->
			{ok,{ts,1970,1,1,0,0,0}}
	end,
	ok=file:close(File),
	Result.
	
find_finish(File,SerieName,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	{ok,Year,_YearIndex}=get_last_year(File,SeriePeriod,2099),
	{ok,{matrix,YearData}}=load(
		{s,SerieName},
		{i,SeriePeriod},
		{ts,Year,1,1,0,0,0},
		{ts,Year,12,31,23,59,59}
	),
	[[Timestamp,_]|_]=lists:reverse(YearData),
	{ok,Timestamp};
find_finish(File,SerieName,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod<86400 ->
	{ok,Year,YearIndex}=get_last_year(File,SeriePeriod,2099),
	{ok,DayPos}=get_last_day(File,YearIndex,365),
	{_,Month,Day}=calendar:gregorian_days_to_date(
		calendar:date_to_gregorian_days(Year,1,1)+DayPos
	),
	{ok,{matrix,DayData}}=load(
		{s,SerieName},
		{i,SeriePeriod},
		{ts,Year,Month,Day,0,0,0},
		{ts,Year,Month,Day,23,59,59}
	),
	[[Timestamp,_]|_]=lists:reverse(DayData),
	{ok,Timestamp}.

find_start(File,SerieName,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	{ok,Year,_YearIndex}=get_first_year(File,SeriePeriod,1900),
	{ok,{matrix,YearData}}=load(
		{s,SerieName},
		{i,SeriePeriod},
		{ts,Year,1,1,0,0,0},
		{ts,Year,12,31,23,59,59}
	),
	[[Timestamp,_]|_]=YearData,
	{ok,Timestamp};
find_start(File,SerieName,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod<86400 ->
	{ok,Year,YearIndex}=get_first_year(File,SeriePeriod,1900),
	{ok,DayPos}=get_first_day(File,YearIndex,0),
	{_,Month,Day}=calendar:gregorian_days_to_date(
		calendar:date_to_gregorian_days(Year,1,1)+DayPos
	),
	{ok,{matrix,DayData}}=load(
		{s,SerieName},
		{i,SeriePeriod},
		{ts,Year,Month,Day,0,0,0},
		{ts,Year,Month,Day,23,59,59}
	),
	[[Timestamp,_]|_]=DayData,
	{ok,Timestamp}.

get_last_year(_File,_SeriePeriod,Year) when is_integer(Year),Year<1900 ->
	none;
get_last_year(File,SeriePeriod,Year) ->
	YearOffset=((Year-1900) rem 200)*8,
	case get_index_by_ref(File,?indexOffset+YearOffset) of
		none ->
			get_last_year(File,SeriePeriod,Year-1);
		{value,YearIndex} ->
			{ok,Year,YearIndex}
	end.

get_first_year(_File,_SeriePeriod,Year) when is_integer(Year),Year>2099 ->
	none;
get_first_year(File,SeriePeriod,Year) ->
	YearOffset=((Year-1900) rem 200)*8,
	case get_index_by_ref(File,?indexOffset+YearOffset) of
		none ->
			get_first_year(File,SeriePeriod,Year+1);
		{value,YearIndex} ->
			{ok,Year,YearIndex}
	end.

get_last_day(_File,_YearIndex,Day) when is_integer(Day),Day<0 ->
	none;
get_last_day(File,YearIndex,Day) ->
	DayOffset=Day*8,
	case get_index_by_ref(File,YearIndex+DayOffset) of
		none ->
			get_last_day(File,YearIndex,Day-1);
		{value,_DayIndex} ->
			{ok,Day}
	end.

get_first_day(_File,_YearIndex,Day) when is_integer(Day),Day>365 ->
	none;
get_first_day(File,YearIndex,Day) ->
	DayOffset=Day*8,
	case get_index_by_ref(File,YearIndex+DayOffset) of
		none ->
			get_first_day(File,YearIndex,Day+1);
		{value,_DayIndex} ->
			{ok,Day}
	end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec () -> EmptySeriesCache
%% @doc Функция создания пустой коллекции значений серий.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
new_series() ->
	gb_trees:empty().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec (SeriesData,Series) -> SeriesCache
%% @doc Функция обновления новыми значениями SeriesData коллекции серий Series.
%% SeriesData представляет собой массив с элементами вида [SerieName,SeriePeriod,Timestamp,Value].
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
update_series(SeriesData,Series) ->
	lists:foldl(
		fun([SerieName,SeriePeriod,Key,Value],AccIn) ->
			Serie=case gb_trees:lookup({SerieName,SeriePeriod},AccIn) of
				{value,S} ->
					S;
				none ->
					gb_sets:empty()
			end,
			UpdatedSerie=gb_sets:add_element([Key,Value],Serie),
			gb_trees:enter({SerieName,SeriePeriod},UpdatedSerie,AccIn)
		end,
		Series,
		SeriesData
	).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec (Series) -> ok
%% @doc Функция сохранения коллекции серий Series.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
store_series(Series) ->
	SeriesData=gb_trees:to_list(Series),
	lists:foreach(
		fun({{SerieName,SeriePeriod},Serie}) ->
			SerieData=gb_sets:to_list(Serie),
			SerieMatrix=case SerieData of
				[] ->
					{matrix,[[]]};
				_ ->
					{matrix,SerieData}
			end,
			store(SerieName,SeriePeriod,SerieMatrix)
		end,
		SeriesData
	),
	ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec (Series) -> ok
%% @doc Функция параллельного сохранения коллекции серий Series.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
store_series_parallel(Series) ->
	SeriesData=gb_trees:to_list(Series),
	plists:foreach(
		fun({{SerieName,SeriePeriod},Serie}) ->
			SerieData=gb_sets:to_list(Serie),
			SerieMatrix=case SerieData of
				[] ->
					{matrix,[[]]};
				_ ->
					{matrix,SerieData}
			end,
			store(SerieName,SeriePeriod,SerieMatrix)
		end,
		SeriesData,
		{processes,length(SeriesData)}
	),
	ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server функции %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec () -> {ok, State}
%% @doc Функция запуска gen_server.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec (Options::list) -> {ok, State}
%% @doc Функция инициализации при запуске gen_server.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init(_Options) ->
	CheckedPathToFiles=get_path_to_files(),
	SeriesNamesFileName=lists:flatten([CheckedPathToFiles,"/",?seriesNamesFileName]),
	ok=filelib:ensure_dir(SeriesNamesFileName),
	SeriesNames=case file:read_file(SeriesNamesFileName) of
		{ok,Bin} ->
			erlang:binary_to_term(Bin);
		_ ->
			gb_sets:empty()
	end,
	State=#config{
		path_to_files=CheckedPathToFiles,
		names=SeriesNames
	},
	garbage_collect(),
    {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%
%%% callback функции %%%
%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec ({check_file,SerieName,SeriePeriod,DoCreate}, _From, State) -> FileName
%% @doc Callback функция для gen_server:call().
%% Запрос на проверку файла серии SerieName с периодом SeriePeriod 
%% и признаком создания нового файла DoCreate.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_call({check_file,SerieName,SeriePeriod,DoCreate}, _From, State) ->
	FileName=lists:flatten(
		[
			State#config.path_to_files,
			"/",
			?dataDir,
			"/",
			get_dir_file_prefix(SerieName,SeriePeriod),
			".serie"
		]
	),
	Res=case file:read_file_info(FileName) of
		{ok,FileInfo} ->
			case (FileInfo#file_info.size>=(?freeSpaceRefSize+?seriePeriodSize+?serieNameSize+?indexSize)) of
				true ->
					{ok,FileName};
				false ->
					do_create_file(SerieName,SeriePeriod,FileName,DoCreate)
			end;
		_ ->
			do_create_file(SerieName,SeriePeriod,FileName,DoCreate)
	end,
	{Reply,NewState}=case Res of
		{ok,CheckedFileName} ->
			Names=State#config.names,
			Key=[{s,SerieName},{i,SeriePeriod}],
			case gb_sets:is_member(Key,Names) of
				true ->
					{{ok,CheckedFileName},State};
				false ->
					NewNames=gb_sets:add_element(Key,Names),
					Bin=erlang:term_to_binary(NewNames),
					SeriesNamesFileName=lists:flatten([State#config.path_to_files,"/",?seriesNamesFileName]),
					ok=file:write_file(SeriesNamesFileName,Bin,[write,raw,binary]),
					{{ok,CheckedFileName},State#config{names=NewNames}}
			end;
		_ ->
			{Res,State}
	end,
    {reply, Reply, NewState};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec ({delete,SerieName,SeriePeriod}, _From, State) -> ok
%% @doc Callback функция для gen_server:call().
%% Запрос на удалениесерии SerieName с периодом SeriePeriod.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_call({delete,SerieName,SeriePeriod}, _From, State) ->
	FileName=lists:flatten(
		[
			State#config.path_to_files,
			"/",
			?dataDir,
			"/",
			get_dir_file_prefix(SerieName,SeriePeriod),
			".serie"
		]
	),
	file:delete(FileName),
	SecondDir=filename:dirname(FileName),
	SecondLevel=lists:flatten(
		[
			SecondDir,
			"/*.serie"
		]
	),
	case filelib:wildcard(SecondLevel) of
		[] ->
			file:del_dir(SecondDir);
		_ ->
			ok
	end,
	FirstDir=filename:dirname(SecondDir),
	FirstLevel=lists:flatten(
		[
			FirstDir,
			"/*"
		]
	),
	case filelib:wildcard(FirstLevel) of
		[] ->
			file:del_dir(FirstDir);
		_ ->
			ok
	end,	
	Names=State#config.names,
	Key=[{s,SerieName},{i,SeriePeriod}],
	NewNames=gb_sets:delete_any(Key,Names),
	Bin=erlang:term_to_binary(NewNames),
	SeriesNamesFileName=lists:flatten([State#config.path_to_files,"/",?seriesNamesFileName]),
	ok=file:write_file(SeriesNamesFileName,Bin,[write,raw,binary]),
	NewState=State#config{names=NewNames},
	Reply=ok,
    {reply, Reply, NewState};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec (list_series, _From, State) -> Path
%% @doc Callback функция для gen_server:call().
%% Запрос на получение списка серий.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_call(list_series, _From, State) ->
	Reply={ok,gb_sets:to_list(State#config.names)},
    {reply, Reply, State};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec (Request, From, State) -> ok
%% @doc Callback функция для gen_server:call().
%% Для всех не распознанных запросов.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_call(Request, From, State) ->
    Reply = {error, {unknown_request, Request, From, State}},
    {reply, Reply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec (_Msg, State) -> ok
%% @doc Callback функция для gen_server:cast().
%% Для запроса на остановку gen_server.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_cast(stop, State) ->
    {stop, normal, State};


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec (_Msg, State) -> ok
%% @doc Callback функция для gen_server:cast().
%% Для всех не распознанных запросов.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_cast(_Msg, State) ->
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec (stop, State) -> ok
%% @doc Callback функция при получении сообщения stop gen_server.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_info(stop, State) ->
    {stop, normal, State};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec (_Info, State) -> ok
%% @doc Callback функция при получении сообщений gen_server.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_info(_Info, State) ->
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec (_Reason, _State) -> ok
%% @doc Callback функция при завершение работы gen_server.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
terminate(_Reason, _State) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @hidden
%% @spec (_OldVsn, State, _Extra) -> {ok, State}
%% @doc Callback функция для обновление состояния gen_server во время смены кода.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

        
%%%%%%%%%%%%%%%%%%%%%%%
%%% private функции %%%
%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Функции для файловых операций.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_create_file(_SerieName,_SeriePeriod,_FileName,false) ->
	{error,not_created};
do_create_file(SerieName,SeriePeriod,FileName,true) ->
	create_file(SerieName,SeriePeriod,FileName).
	
create_file(SerieName,SeriePeriod,FileName) ->
	SerieNameSize=?serieNameSize,
	IndexSize=?indexSize,
	FreeSpaceRefSize=?freeSpaceRefSize,
	SeriePeriodSize=?seriePeriodSize,
	FreeSpaceRef=?headerSize,
	FreeSpaceRefBin=list_to_binary([<<FreeSpaceRef:FreeSpaceRefSize/little-unsigned-integer-unit:8>>]),
	SeriePeriodBin=list_to_binary([<<SeriePeriod:SeriePeriodSize/little-unsigned-integer-unit:8>>]),
	CheckedSerieNameStr=string:left(SerieName,SerieNameSize,0),
	SerieNameBin=list_to_binary(CheckedSerieNameStr),
	IndexBin=?block(IndexSize,0),
	HeaderBin=list_to_binary([FreeSpaceRefBin,SeriePeriodBin,SerieNameBin,IndexBin]),
	?headerSize=byte_size(HeaderBin),
	ok=filelib:ensure_dir(FileName),
	ok=file:write_file(FileName,HeaderBin,[write,raw,binary]),
	{ok,FileName}.

create_year_block(File,Year) ->
	YearBlock=empty_block(86400),
	YearBlockSize=byte_size(YearBlock),
	{value,FreeSpaceRef}=get_index_by_ref(File,0),
	NewFreeSpace=FreeSpaceRef+YearBlockSize,
	NewFreeSpaceBin=list_to_binary([<<NewFreeSpace:8/little-unsigned-integer-unit:8>>]),
	ok=file:pwrite(File,0,NewFreeSpaceBin),
	YearBlockOffset=FreeSpaceRef,
	IndexBin=list_to_binary([<<YearBlockOffset:8/little-unsigned-integer-unit:8>>]),
	YearOffset=((Year-1900) rem 200)*8,
	ok=file:pwrite(File,YearBlockOffset,YearBlock),
	ok=file:pwrite(File,?indexOffset+YearOffset,IndexBin),
	{value,YearBlockOffset}.

create_day_block(File,SeriePeriod,YearIndex,DayOffset) ->
	DayBlock=empty_block(SeriePeriod),
	DayBlockSize=byte_size(DayBlock),
	{value,FreeSpaceRef}=get_index_by_ref(File,0),
	NewFreeSpace=FreeSpaceRef+DayBlockSize,
	NewFreeSpaceBin=list_to_binary([<<NewFreeSpace:8/little-unsigned-integer-unit:8>>]),
	ok=file:pwrite(File,0,NewFreeSpaceBin),
	DayBlockOffset=FreeSpaceRef,
	DayIndexBin=list_to_binary([<<DayBlockOffset:8/little-unsigned-integer-unit:8>>]),
	ok=file:pwrite(File,YearIndex+DayOffset,DayIndexBin),
	ok=file:pwrite(File,DayBlockOffset,DayBlock),
	{value,DayBlockOffset}.

get_index_by_ref(File,Offset) ->
	{ok,<<Index:8/little-unsigned-integer-unit:8>>}=file:pread(File,Offset,8),
	case Index==0 of
		true ->
			none;
		false ->
			{value,Index}
	end.

write_value(File,SeriePeriod,BlockIndex,{ts,_Year,_Month,_Day,_Hour,_Minute,_Second}=Timestamp,Value) ->
	ValueOffset=get_value_offset(Timestamp,SeriePeriod),
	ValueBin=list_to_binary([<<Value:8/little-float-unit:8>>]),
	ok=file:pwrite(File,BlockIndex+ValueOffset,ValueBin).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Функции чтения и обработки данных из файлов.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
load_data(FileName,SeriePeriod,Start,Finish) ->
	{ok,File}=file:open(FileName,[read,raw,binary,{read_ahead,86400*8}]),
	Result=process_read_data(File,SeriePeriod,Start,Finish,m_arr:m()),
	ok=file:close(File),
	Result.
	
process_read_data(File,SeriePeriod,Start,Finish,AccIn) ->
	case lte(Start,Finish) of
		true ->
			Left=Start,
			Next=get_period_next(Start,SeriePeriod),
			Right=case lt(Finish,Next) of
				true ->
					Finish;
				false ->
					get_period_finish(Start,SeriePeriod)
			end,
			NewAccIn=case get_index(File,SeriePeriod,Left,false) of
				{value,BlockIndex} ->
					process_read_values(File,SeriePeriod,BlockIndex,Left,Right,AccIn);
				none ->
					AccIn
			end,
			process_read_data(File,SeriePeriod,Next,Finish,NewAccIn);
		false ->
			Mat=m_arr:to_res(
				AccIn,
				fun(Cell) ->
					Cell
				end
			),
			{ok,Mat}
	end.

process_read_values(File,SeriePeriod,BlockIndex,Left,Right,AccIn) ->
	Offset=get_value_offset(Left,SeriePeriod),
	Quantity=get_value_offset(Right,SeriePeriod)-Offset+8,
	{ok,Bin}=file:pread(File,BlockIndex+Offset,Quantity),
	process_binary_read_values(Left,SeriePeriod,Bin,AccIn).
	
process_binary_read_values(Timestamp,SeriePeriod,Bin,AccIn) when byte_size(Bin)>0 ->
    {<<ReadValue:8/little-float-unit:8>>,Tail}=erlang:split_binary(Bin,8),
    NewAccIn=case convert_read_value(ReadValue) of
    	empty ->
    		AccIn;
    	Value ->
    		Row=m_arr:m_rows(AccIn)+1,
    		M1=m_arr:m(AccIn,Row,1,Timestamp),
    		M2=m_arr:m(M1,Row,2,{f,Value}),
    		M2
    end,
    NewTimestamp=get_next_timestamp(Timestamp,SeriePeriod),
    process_binary_read_values(NewTimestamp,SeriePeriod,Tail,NewAccIn);
process_binary_read_values(_Timestamp,_SeriePeriod,Bin,AccIn) when byte_size(Bin)=<0 ->
	AccIn.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Функции обработки и записи данных в файлы.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
store_data(FileName,SeriePeriod,Data) ->
	ParsedData=parse_data(SeriePeriod,Data),
	{ok,File}=file:open(FileName,[read,write,raw,binary,{read_ahead,86400*8}]),
	ok=process_write_data(File,SeriePeriod,gb_trees:iterator(ParsedData)),
	ok=file:close(File).

parse_data(SeriePeriod,Data) when is_integer(SeriePeriod),SeriePeriod<86400 ->
	lists:foldl(
		fun(Row,AccIn) ->
			case Row of
				[{ts,Year,Month,Day,Hour,Minute,Second},{f,Value}] ->
					Entry=case gb_trees:lookup({ts,Year,Month,Day,0,0,0},AccIn) of
						{value,TSEntry} ->
							TSEntry;
						none ->
							gb_trees:empty()
					end,
					NewEntry=gb_trees:enter({ts,Year,Month,Day,Hour,Minute,Second},Value,Entry),
					gb_trees:enter({ts,Year,Month,Day,0,0,0},NewEntry,AccIn);
				_ ->
					AccIn
			end
		end,
		gb_trees:empty(),
		Data
	);
parse_data(SeriePeriod,Data) when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	lists:foldl(
		fun([{ts,Year,Month,Day,Hour,Minute,Second},{f,Value}],AccIn) ->
			Entry=case gb_trees:lookup({ts,Year,1,1,0,0,0},AccIn) of
				{value,TSEntry} ->
					TSEntry;
				none ->
					gb_trees:empty()
			end,
			NewEntry=gb_trees:enter({ts,Year,Month,Day,Hour,Minute,Second},Value,Entry),
			gb_trees:enter({ts,Year,1,1,0,0,0},NewEntry,AccIn)
		end,
		gb_trees:empty(),
		Data
	).

process_write_data(File,SeriePeriod,Iterator) ->
	case gb_trees:next(Iterator) of
		none ->
			ok;
		{Timestamp,Entry,NewIterator} ->
			{value,BlockIndex}=get_index(File,SeriePeriod,Timestamp,true),
			ok=process_write_values(File,SeriePeriod,BlockIndex,gb_trees:iterator(Entry)),
			process_write_data(File,SeriePeriod,NewIterator)
	end.

process_write_values(File,SeriePeriod,BlockIndex,Iterator) ->
	case gb_trees:next(Iterator) of
		none ->
			ok;
		{Timestamp,Value,NewIterator} ->
			write_value(File,SeriePeriod,BlockIndex,Timestamp,convert_write_value(Value)),
			process_write_values(File,SeriePeriod,BlockIndex,NewIterator)
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Вспомогательные функции для работы с файлами.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_index(File,SeriePeriod,{ts,Year,_Month,_Day,_Hour,_Minute,_Second}=_Timestamp,false)
		when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	YearOffset=((Year-1900) rem 200)*8,
	get_index_by_ref(File,?indexOffset+YearOffset);

get_index(File,SeriePeriod,{ts,Year,Month,Day,_Hour,_Minute,_Second}=Timestamp,false)
		when is_integer(SeriePeriod),SeriePeriod<86400 ->
	case get_index(File,86400,Timestamp,false) of
		none ->
			none;
		{value,YearIndex} ->
			DayOffset=(day_of_year(Year,Month,Day)-1)*8,
			get_index_by_ref(File,YearIndex+DayOffset)
	end;

get_index(File,SeriePeriod,{ts,Year,_Month,_Day,_Hour,_Minute,_Second}=Timestamp,true)
		when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	case get_index(File,SeriePeriod,Timestamp,false) of
		none ->
			create_year_block(File,Year);
		{value,YearIndex} ->
			{value,YearIndex}
	end;
	
get_index(File,SeriePeriod,{ts,Year,Month,Day,_Hour,_Minute,_Second}=Timestamp,true)
		when is_integer(SeriePeriod),SeriePeriod<86400 ->
	{value,YearIndex}=get_index(File,86400,Timestamp,true),
	DayOffset=(day_of_year(Year,Month,Day)-1)*8,
	case get_index_by_ref(File,YearIndex+DayOffset) of
		none ->
			create_day_block(File,SeriePeriod,YearIndex,DayOffset);
		{value,DayIndex} ->
			{value,DayIndex}
	end.

get_value_offset({ts,_Year,_Month,_Day,Hour,Minute,Second}=_Timestamp,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod<60 ->
	(Hour*3600+Minute*60+Second)*8;
get_value_offset({ts,_Year,_Month,_Day,Hour,Minute,_Second}=_Timestamp,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=60,SeriePeriod<86400 ->
	(Hour*60+Minute)*8;
get_value_offset({ts,Year,Month,Day,_Hour,_Minute,_Second}=_Timestamp,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	(day_of_year(Year,Month,Day)-1)*8.

get_hash(SerieName,SeriePeriod) ->
	erlang:phash([{s,SerieName},{i,SeriePeriod}],16#100000000).

get_dir_file_prefix(SerieName,SeriePeriod) ->
	ID=get_hash(SerieName,SeriePeriod),
	FilePrefix=lists:flatten(io_lib:format("~8.16.0B",[ID])),
	DirName=lists:flatten(io_lib:format("~3.16.0B/~3.16.0B",
		[
			(ID bsr 20) band 16#00000FFF,
			(ID bsr 8) band 16#00000FFF
		]
	)),
	lists:flatten(
		[
			DirName,
			"/",
			FilePrefix
		]
	).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Вспомогательные функции для работы с данными.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
convert_write_value(0.0) -> 
	?zeroValue;
convert_write_value(?emptyValue) -> 
	0.0;
convert_write_value(Value) -> 
	Value.

convert_read_value(?emptyValue) ->
	empty;
convert_read_value(0.0) ->
	empty;
convert_read_value(?zeroValue) ->
	0.0;
convert_read_value(Value) ->
	Value.
	
empty_block(SeriePeriod) when is_integer(SeriePeriod),SeriePeriod<60 ->
	?emptyBlock(86400);
empty_block(SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=60,SeriePeriod<86400 ->
	?emptyBlock(1440);
empty_block(SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	?emptyBlock(366).	


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Функции для операций с метками времени.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
day_of_year(Year,Month,Day) ->
    BeforeMonthsDays=months_days(Year,Month),
    BeforeMonthsDays+Day.

months_days(_Year,BeforeMonth) when is_integer(BeforeMonth), BeforeMonth<0 ->
    0;
months_days(Year,BeforeMonth) ->
    months_days(Year,BeforeMonth,0).

months_days(_Year,0,Sum) ->
    Sum;
months_days(_Year,1,Sum) ->
    Sum;
months_days(Year,BeforeMonth,Sum) ->
    LastDayOfMonth=calendar:last_day_of_the_month(Year,BeforeMonth-1),
    NewSum=Sum+LastDayOfMonth,
    NewBeforeMonth=BeforeMonth-1,
    months_days(Year,NewBeforeMonth,NewSum).
    
get_period_finish({ts,Year,Month,Day,_Hour,_Minute,_Second}=_Start,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod<86400 ->
	{ts,Year,Month,Day,23,59,59};
get_period_finish({ts,Year,_Month,_Day,_Hour,_Minute,_Second}=_Start,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	{ts,Year,12,31,23,59,59}.
	
get_period_next({ts,Year,Month,Day,_Hour,_Minute,_Second}=_Start,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod<86400 ->
	{NewYear,NewMonth,NewDay}=case Day==calendar:last_day_of_the_month(Year,Month) of
		true ->
			case Month==12 of
				true ->
					{Year+1,1,1};
				false ->
					{Year,Month+1,1}
			end;
		false ->
			{Year,Month,Day+1}
	end,
	{ts,NewYear,NewMonth,NewDay,0,0,0};
get_period_next({ts,Year,_Month,_Day,_Hour,_Minute,_Second}=_Start,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	{ts,Year+1,1,1,0,0,0}.
	
get_next_timestamp({ts,Year,Month,Day,Hour,Minute,Second}=_Timestamp,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod<60 ->
	Seconds=Hour*3600+Minute*60+Second+1,
	NewHour=Seconds div 3600,
	NewMinute=(Seconds div 60) rem 60,
	NewSecond=Seconds rem 60,
	AddDay=Seconds div 86400,
	case AddDay==0 of
		true ->
			{ts,Year,Month,Day,NewHour,NewMinute,NewSecond};
		false ->
			{NewYear,NewMonth,NewDay}=next_day(Year,Month,Day),
			{ts,NewYear,NewMonth,NewDay,0,0,0}
	end;
get_next_timestamp({ts,Year,Month,Day,Hour,Minute,_Second}=_Timestamp,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=60,SeriePeriod<86400 ->
	Seconds=Hour*3600+Minute*60+60,
	NewHour=Seconds div 3600,
	NewMinute=(Seconds div 60) rem 60,
	NewSecond=Seconds rem 60,
	AddDay=Seconds div 86400,
	case AddDay==0 of
		true ->
			{ts,Year,Month,Day,NewHour,NewMinute,NewSecond};
		false ->
			{NewYear,NewMonth,NewDay}=next_day(Year,Month,Day),
			{ts,NewYear,NewMonth,NewDay,0,0,0}
	end;
get_next_timestamp({ts,Year,Month,Day,_Hour,_Minute,_Second}=_Timestamp,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	{NewYear,NewMonth,NewDay}=next_day(Year,Month,Day),
	{ts,NewYear,NewMonth,NewDay,0,0,0}.


next_day(Year,12,31) ->
	{Year+1,1,1};
next_day(Year,Month,Day) ->
	case Day==calendar:last_day_of_the_month(Year,Month) of
		true ->
			{Year,Month+1,1};
		false ->
			{Year,Month,Day+1}
	end.

ts_now() ->
    {{Year,Month,Day},{Hour,Minute,Second}}=calendar:now_to_universal_time(now()),
    TS={ts,Year,Month,Day,Hour,Minute,Second},
    case is_dst(TS) of
        false ->
            add_seconds(TS,1*60*60);
        true ->
            add_seconds(TS,2*60*60)
    end.

add_seconds({ts,Year,Month,Day,Hour,Minute,Second},Seconds) ->
	V=calendar:datetime_to_gregorian_seconds({{Year,Month,Day},{Hour,Minute,Second}}),
    {{NewYear,NewMonth,NewDay}, {NewHour,NewMinute,NewSecond}} =
        calendar:gregorian_seconds_to_datetime(V+Seconds),
    {ts,NewYear,NewMonth,NewDay,NewHour,NewMinute,NewSecond}.

is_dst({ts,Year,_Month,_Day,_Hour,_Minute,_Second}=TS) ->
    {{M1,D1},{M2,D2}}=dst_period(Year),
    TS1={ts,Year,M1,D1,0,0,0},
    TS2={ts,Year,M2,D2,0,0,0},
    lte(TS1,TS) and lt(TS,TS2);
is_dst(_) ->
    false.


dst_period(1999) -> {{4,4},{10,31}};
dst_period(2000) -> {{4,2},{10,29}};
dst_period(2001) -> {{4,1},{10,28}};
dst_period(2002) -> {{4,7},{10,27}};
dst_period(2003) -> {{4,6},{10,26}};
dst_period(2004) -> {{4,4},{10,31}};
dst_period(2005) -> {{4,3},{10,30}};
dst_period(2006) -> {{4,2},{10,29}};
dst_period(2007) -> {{3,11},{11,4}};
dst_period(2008) -> {{3,9},{11,2}};
dst_period(2009) -> {{3,8},{11,1}};
dst_period(2010) -> {{3,14},{11,7}};
dst_period(2011) -> {{3,13},{11,6}};
dst_period(2012) -> {{3,11},{11,4}};
dst_period(2013) -> {{3,10},{11,3}};
dst_period(2014) -> {{3,9},{11,2}};
dst_period(2015) -> {{3,8},{11,1}};
dst_period(2016) -> {{3,13},{11,6}};
dst_period(2017) -> {{3,12},{11,5}};
dst_period(2018) -> {{3,11},{11,4}};
dst_period(2019) -> {{3,10},{11,3}};
dst_period(_) -> {{3,10},{11,3}}.

value2dec({ts,Year,Month,Day,Hour,Minute,Second}) ->
	Year*10000000000+Month*100000000+Day*1000000+Hour*10000+Minute*100+Second;
value2dec(Value) ->
	Value.

ts2str({ts,Year,Month,Day,Hour,Minute,Second}) ->
	V=value2dec({ts,Year,Month,Day,Hour,Minute,Second}),
	int2str(V).
	
int2str(V) ->
	int2str(V,[]).
	
int2str(0,Acc) ->
	Acc;
int2str(V,Acc) ->
	Digit=V rem 10,
	int2str(V div 10,[48+Digit|Acc]).
	
comp(Value1,Value2,CompareFun) ->
	V1=value2dec(Value1),
	V2=value2dec(Value2),
	CompareFun(V1,V2).

% eq(Timestamp1,Timestamp2) ->
% 	comp(Timestamp1,Timestamp2,fun(A,B) -> A==B end).

% neq(Timestamp1,Timestamp2) ->
% 	comp(Timestamp1,Timestamp2,fun(A,B) -> A/=B end).
	
lt(Timestamp1,Timestamp2) ->
	comp(Timestamp1,Timestamp2,fun(A,B) -> A<B end).

gt(Timestamp1,Timestamp2) ->
	comp(Timestamp1,Timestamp2,fun(A,B) -> A>B end).

lte(Timestamp1,Timestamp2) ->
	comp(Timestamp1,Timestamp2,fun(A,B) -> A=<B end).

% gte(Timestamp1,Timestamp2) ->
% 	comp(Timestamp1,Timestamp2,fun(A,B) -> A>=B end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Функции для работы с файлом конфигурации.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_path_to_files() ->
	ConfigFilename=lists:flatten(
        [
            code:lib_dir(tss),
            "/priv/config/",
            "tss.conf"
        ]
    ),
	ok=filelib:ensure_dir(ConfigFilename),
	Config=case file:consult(ConfigFilename) of
      {ok,Data} ->
          Data;
      _Other ->
          []
    end,
	PathToFiles=proplists:get_value(path_to_files,Config,""),
	L=length(PathToFiles),
	case string:sub_string(PathToFiles,L-1,L) of
		"/" ->
			string:sub_string(PathToFiles,1,L-1);
		_ ->
			PathToFiles
	end.

%%%%%%%%%%%%%%%%%%%%
%%% test функции %%%
%%%%%%%%%%%%%%%%%%%%
export_to_csv(Path) ->
	{ok,{matrix,Series}}=list_series(),
	FilteredSeries=lists:filter(
		fun([{s,_Name},{i,Period}]) ->
			(Period==86400)
		end,
		Series
	),	
	io:format("Start time: ~p~n",[calendar:local_time()]),
	io:format("FilteredSeries: ~p~n",[FilteredSeries]),
	io:format("length(FilteredSeries): ~p~n",[length(FilteredSeries)]),
	process_export(Path,FilteredSeries).

process_export(Path,[[{s,SerieName},{i,SeriePeriod}]|Tail]) ->
	case catch(gen_server:call(?MODULE,{check_file,SerieName,SeriePeriod,false},infinity)) of
		{ok,FileName,".serie"} ->
			{ok,TS1}=get_first_timestamp({s,SerieName},{i,SeriePeriod}),
			{ok,TS2}=get_last_timestamp({s,SerieName},{i,SeriePeriod}),
			ExportFileName=lists:flatten(
				[
					Path,
					"/",
					filename:basename(FileName,".serie"),
					".txt"
				]
			),
			io:format("~w, ~s~n~w, ~w~n~ts~n",[SeriePeriod,SerieName,TS1,TS2,lists:flatten(["\"",ExportFileName,"\""])]),
    		ok=filelib:ensure_dir(ExportFileName),
    		{ok,File}=file:open(
    			ExportFileName,
    			[raw,write,binary,{delayed_write,50000000,600000}]
    		),
			Header=list_to_binary(
				[
					io_lib:format("~w.~n",[{header,{s,SerieName},{i,SeriePeriod},TS1,TS2}])
				]
			),
			ok=file:write(File,Header),
			process_export_serie(File,SerieName,SeriePeriod,TS1,TS2),
			ok=file:close(File),
			Cmd=lists:flatten(
				[
					"7z a -t7z -mx=1 \"",
					filename:dirname(ExportFileName),
					"/",
					filename:basename(ExportFileName,".txt"),
					".7z\" \"",
					ExportFileName,
					"\""
				]
			),
			io:format("~s~n",[Cmd]),
			CmdRes=os:cmd(Cmd),
			io:format("~s~n",[CmdRes]),
			ok=file:delete(ExportFileName),
			garbage_collect();
		_ ->
			ok
	end,
	process_export(Path,Tail);
process_export(_Path,[]) ->
	ok.

process_export_serie(File,SerieName,SeriePeriod,Start,Finish) ->
	case lte(Start,Finish) of
		true ->
			Left=Start,
			Right=get_period_finish(Start,SeriePeriod),
			Next=get_period_next(Start,SeriePeriod),
			io:format("~s, ~w, ~w, ~w~n",[SerieName,SeriePeriod,Left,Right]),
			{ok,{matrix,Data}}=load({s,SerieName},{i,SeriePeriod},Left,Right),
			file:write(File,list_to_binary(io_lib:format("~w.~n",[{matrix,Data}]))),
			process_export_serie(File,SerieName,SeriePeriod,Next,Finish);
		false ->
			ok
	end.

-ifdef(TEST).

test1_body() ->
	?debugVal(file:get_cwd()),
	ok=application:start(tss),
	SerieName="TEST.PRICE",
	SeriePeriod=60,
	TS=ts_now(),
	Data=[
		[TS,{f,random:uniform()}]
	],
	{ok,_}=store(
		{s,SerieName},
		{i,SeriePeriod},
		{matrix,Data}
	),
	Ticker={ticker,"TEST"},
	?debugVal(timer:tc(?MODULE,get_last_tick_timestamp,[Ticker])),
	{ts,Year,Month,Day,_,_,_}=TS,
	{ok,{matrix,LoadedData}}=load(
		{s,SerieName},
		{i,SeriePeriod},
		{ts,Year,Month,Day,0,0,0},
		{ts,Year,Month,Day,23,59,59}
	),
	?debugVal(LoadedData),
	ok=application:stop(tss).

test1_test_() ->
	[
		{timeout,60000,fun test1_body/0}
	].

-endif.