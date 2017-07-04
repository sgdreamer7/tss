%% @author Владимир Щербина <vns.scherbina@gmail.com>
%% @copyright 2017 Владимир Щербина
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
		store/3,load/4,load_many/4,delete/2,list_series/0,get_last_tick_timestamp/1,
		get_last_timestamp/2,get_first_timestamp/2,ts2str/1,
		new_series/0,update_series/2,store_series/1,store_series_parallel/1,
		export_to_csv/3,export_to_csv/1,process_export/2,process_export_serie/5,
		get_dir_file_prefix/2,
		export/3
		% calculate_size/2
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

%%%%%%%%%%%%%%%%%%%%%%
%%% public функции %%%
%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec (SerieName,SeriePeriod,{m,Data}) -> {ok,Status}
%% @doc Функция сохранение значений Data серии SerieName с периодом SeriePeriod.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
store(
	SerieName,
	SeriePeriod,
	{m,Data}
) ->
	case Data of
		[] ->
			ok;
		[[]] ->
			ok;
		_ ->
			case catch(check_file(SerieName,SeriePeriod,true)) of
				{ok,FileName} ->
					catch(store_data(FileName,SeriePeriod,Data));
				_ ->
					error
			end
	end,
    {ok,"Stored."};
store(_,_,_) ->
	{ok,"Stored."}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec (SerieName,SeriePeriod,Start,Finish) -> {ok,{m,Data}}
%% @doc Функция чтения значений серии SerieName с периодом SeriePeriod
%% на интервале от Start до Finish включительно.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
load(
	SerieName,
	SeriePeriod,
	{ts,StartYear,StartMonth,StartDay,StartHour,StartMinute,StartSecond}=Start,
	{ts,FinishYear,FinishMonth,FinishDay,FinishHour,FinishMinute,FinishSecond}=Finish
) ->
	% Time1=erlang:timestamp(),
    V1=calendar:datetime_to_gregorian_seconds({{StartYear,StartMonth,StartDay},{StartHour,StartMinute,StartSecond}}),
    V2=calendar:datetime_to_gregorian_seconds({{FinishYear,FinishMonth,FinishDay},{FinishHour,FinishMinute,FinishSecond}}),
	{Start1,Finish1}=case ((erlang:abs(V2-V1)>(86400*2)) and (SeriePeriod==1)) of
		true ->
			{{Yr,Mt,Dy},{Hr,Mn,Sc}}=calendar:gregorian_seconds_to_datetime(V2-86400*2),
			{{ts,Yr,Mt,Dy,Hr,Mn,Sc},Finish};
		false ->
			{Start,Finish}
	end,
    case SerieName of
		"test_sec" ->
			Data=gen_sample_data(Start,Finish,1),
			{ok,{m,Data}};
		"test_min" ->
			Data=gen_sample_data(Start,Finish,60),
			{ok,{m,Data}};
		"test_year" ->
			Data=gen_sample_data(Start,Finish,86400),
			{ok,{m,Data}};
		_ ->
			case catch(check_file(SerieName,SeriePeriod,false)) of
				{ok,FileName} ->
					case catch(load_data(FileName,SeriePeriod,align_timestamp(Start1,SeriePeriod),align_timestamp(Finish1,SeriePeriod))) of
						{'EXIT',_} ->
							{ok,{m,[[]]}};
						{ok,{m,Mat}} ->
							{ok,{m,Mat}};
						_Other ->
							io:format("tss: load: ~p error:~n~p~n",[{SerieName,SeriePeriod,Start,Finish},_Other]),
							{ok,{m,[[]]}}
					end;
				_-> {ok,{m,[[]]}}
			end
	end;
load(_,_,_,_) ->
	{ok,{m,[[]]}}.

load_many(
	{m,SeriesNamesValues},
	SeriePeriod,
	{ts,StartYear,StartMonth,StartDay,StartHour,StartMinute,StartSecond}=Start,
	{ts,FinishYear,FinishMonth,FinishDay,FinishHour,FinishMinute,FinishSecond}=Finish
) ->
	V1=calendar:datetime_to_gregorian_seconds({{StartYear,StartMonth,StartDay},{StartHour,StartMinute,StartSecond}}),
    V2=calendar:datetime_to_gregorian_seconds({{FinishYear,FinishMonth,FinishDay},{FinishHour,FinishMinute,FinishSecond}}),
	{Start1,Finish1}=case ((erlang:abs(V2-V1)>86400) and (SeriePeriod==1)) of
		true ->
			{{Yr,Mt,Dy},{Hr,Mn,Sc}}=calendar:gregorian_seconds_to_datetime(V2-86400),
			{{ts,Yr,Mt,Dy,Hr,Mn,Sc},Finish};
		false ->
			{Start,Finish}
	end,
	MatValue=lists:foldr(
		fun(SeriesNames,AccIn) ->
			{m,Mat}=load_many_internal(SeriesNames,SeriePeriod,Start1,Finish1),
			Mat++AccIn
		end,
		[],
		SeriesNamesValues
	),
	{ok,{m,MatValue}};
load_many(_,_,_,_) ->
	{ok,{m,[[]]}}.

load_many_internal(SeriesNames,SeriePeriod,Start1,Finish1) ->
	CheckFiles=[catch(check_file(SerieName,SeriePeriod,false)) || SerieName <- SeriesNames],
	AllFilesChecked=lists:all(fun({ok,_}) -> true; (_) -> false end,CheckFiles),
	case AllFilesChecked of
		true ->
			FilesNames=[FileName || {ok,FileName} <- CheckFiles],
			case catch(load_data_many(FilesNames,SeriePeriod,align_timestamp(Start1,SeriePeriod),align_timestamp(Finish1,SeriePeriod))) of
				{ok,{m,Mat}} ->
					{m,Mat};
				_Error ->
					io:format("tss: load_many_internal: ~p~nerror: ~p~n",[{SeriesNames,SeriePeriod,Start1,Finish1},_Error]),
					{m,[[]]}
			end;
		_ ->
			{m,[[]]}
	end.


% calculate_size(SerieName,SeriePeriod) when is_list(SerieName),is_integer(SeriePeriod) ->
%     case SerieName of
% 		"test_sec" ->
% 			{ok,0};
% 		"test_min" ->
% 			{ok,0};
% 		"test_year" ->
% 			{ok,0};
% 		_ ->
% 			case catch(check_file(SerieName,SeriePeriod,false)) of
% 				{ok,FileName} ->
% 					case file:read_file_info(FileName) of
% 						{ok,FileInfo} ->
% 							{ok,FileInfo#file_info.size};
% 						_ ->
% 							{ok,0}
% 					end;
% 				_ ->
% 					{ok,0}
% 			end
% 	end;
% calculate_size(_,_) ->
% 	{ok,0}.

export(SerieName,SeriePeriod,PathToFiles) when is_list(SerieName),is_integer(SeriePeriod),is_list(PathToFiles) ->
    case SerieName of
		"test_sec" ->
			{ok,0};
		"test_min" ->
			{ok,0};
		"test_year" ->
			{ok,0};
		_ ->
			case catch(check_file(SerieName,SeriePeriod,false)) of
				{ok,SrcFileName} ->
					DstFileName=lists:flatten(
						[
							PathToFiles,
							"/",
							?dataDir,
							"/",
							get_dir_file_prefix(SerieName,SeriePeriod),
							".serie"
						]
					),
				    ok=filelib:ensure_dir(DstFileName),
					{ok,BytesCopied}=file:copy(SrcFileName,DstFileName),
					{ok,BytesCopied};
				_ ->
					{ok,0}
			end
	end;
export(_,_,_) ->
	{ok,0}.


export_to_csv(SerieName,SeriePeriod,PathToFiles) when is_list(SerieName),is_integer(SeriePeriod),is_list(PathToFiles) ->
    case SerieName of
		"test_sec" ->
			{ok,0};
		"test_min" ->
			{ok,0};
		"test_year" ->
			{ok,0};
		_ ->
			{ok,TS1}=get_first_timestamp(SerieName,SeriePeriod),
			{ok,TS2}=get_last_timestamp(SerieName,SeriePeriod),
			ExportFileName=lists:flatten(
				[
					PathToFiles,
					"/",
					?dataDir,
					"/",
					get_dir_file_prefix(SerieName,SeriePeriod),
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
					io_lib:format("~w.~n",[{header,SerieName,SeriePeriod,TS1,TS2}])
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
			garbage_collect(),
			{ok,0}
	end;
export_to_csv(_,_,_) ->
	{ok,0}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec (SerieName,SeriePeriod) -> {ok,Status}
%% @doc Функция удаления серии SerieName с периодом SeriePeriod.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
delete(SerieName,SeriePeriod) when is_list(SerieName),is_integer(SeriePeriod) ->
	(catch gen_server:call(?MODULE,{delete,SerieName,SeriePeriod},infinity)),
	{ok,"Deleted."};
delete(_,_) ->
	{ok,"Deleted."}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec () -> {ok,{m,Series}}
%% @doc Функция получения списка серий.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
list_series() ->
	{ok,Series}=gen_server:call(?MODULE,list_series,infinity),
	{ok,{m,Series}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @spec (Ticker) -> {ok,Timestamp}
%% @doc Функция определение последнего по времени значения серии.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_last_tick_timestamp({ticker,Ticker}) ->
    TS=ts_now(),
    SerieName=lists:flatten([Ticker,".PRICE"]),
    get_last_tick_timestamp(SerieName,TS,add_seconds(TS,-86400*31)).

get_last_tick_timestamp(SerieName,TS,FinishTS) ->
    case gt(TS,FinishTS) of
        true ->
            {ts,Year,Month,Day,_,_,_}=TS,
            case load(SerieName,60,{ts,Year,Month,Day,0,0,0},{ts,Year,Month,Day,23,59,59}) of
                {ok,{m,[[]]}} ->
                    get_last_tick_timestamp(SerieName,add_seconds({ts,Year,Month,Day,0,0,0},-86400),FinishTS);
                {ok,{m,Data}} ->
                    [[LastTimestamp,_]|_]=lists:reverse(Data),
                    {ok,LastTimestamp};
                _Other ->
                    io:format("~p~n",[_Other]),
                    {ok,ts_now()}
            end;
        false ->
            {ok,FinishTS}
    end.

get_last_timestamp(SerieName,SeriePeriod) when is_list(SerieName),is_integer(SeriePeriod) ->
	case catch(check_file(SerieName,SeriePeriod,false)) of
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

get_first_timestamp(SerieName,SeriePeriod) when is_list(SerieName),is_integer(SeriePeriod) ->
	case catch(check_file(SerieName,SeriePeriod,false)) of
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
	{ok,{m,YearData}}=load(
		SerieName,
		SeriePeriod,
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
	{ok,{m,DayData}}=load(
		SerieName,
		SeriePeriod,
		{ts,Year,Month,Day,0,0,0},
		{ts,Year,Month,Day,23,59,59}
	),
	[[Timestamp,_]|_]=lists:reverse(DayData),
	{ok,Timestamp}.

find_start(File,SerieName,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	{ok,Year,_YearIndex}=get_first_year(File,SeriePeriod,1900),
	{ok,{m,YearData}}=load(
		SerieName,
		SeriePeriod,
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
	{ok,{m,DayData}}=load(
		SerieName,
		SeriePeriod,
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
					{m,[[]]};
				_ ->
					{m,SerieData}
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
					{m,[[]]};
				_ ->
					{m,SerieData}
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
	Config=fex_main:get_properties("tss.conf"),
    io:format("tss: Config=~p~n",[Config]),
	PathToFiles=proplists:get_value(path_to_files,Config,""),
	
    io:format("tss: PathToFiles=~p~n",[PathToFiles]),
	L=length(PathToFiles),
	CheckedPathToFiles=case string:sub_string(PathToFiles,L-1,L) of
		"/" ->
			string:sub_string(PathToFiles,1,L-1);
		_ ->
			PathToFiles
	end,
	io:format("tss: CheckedPathToFiles=~p~n",[CheckedPathToFiles]),
	SeriesNamesFileName=lists:flatten([CheckedPathToFiles,"/",?seriesNamesFileName]),
	io:format("tss: SeriesNamesFileName=~p~n",[SeriesNamesFileName]),
	ok=filelib:ensure_dir(SeriesNamesFileName),
	io:format("tss: filelib:ensure_dir(SeriesNamesFileName)~n",[]),
	SeriesNames=case file:read_file(SeriesNamesFileName) of
		{ok,Bin} ->
			case catch(erlang:binary_to_term(Bin)) of
				{'EXIT',_} ->
					  gb_sets:empty();
				Res ->
					Res
			end;
		_ ->
			gb_sets:empty()
	end,
	application:set_env(fex,tss_path_to_files,CheckedPathToFiles),
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
			Key=[SerieName,SeriePeriod],
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
	io:format("~ts",[FileName]),
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
	Key=[SerieName,SeriePeriod],
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
	Reply={ok,[["test_sec",1],["test_min",60],["test_year",86400]]++gb_sets:to_list(State#config.names)},
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
    normal.

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

check_file(SerieName,SeriePeriod,DoCreate) ->
	% T1=erlang:timestamp(),
	{ok,PathToFiles}=application:get_env(fex,tss_path_to_files),
	FileName=lists:flatten(
		[
			PathToFiles,
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
					catch(gen_server:call(?MODULE,{check_file,SerieName,SeriePeriod,DoCreate},infinity))
			end;
		_ ->
			catch(gen_server:call(?MODULE,{check_file,SerieName,SeriePeriod,DoCreate},infinity))
	end,
	% T2=erlang:timestamp(),
	% io:format("check_file: ~w~n",[timer:now_diff(T2,T1)]),
	Res.
	
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
	Result=process_read_data(File,SeriePeriod,Start,Finish,[]),
	ok=file:close(File),
	Result.
	
load_data_many(FilesNames,SeriePeriod,Start,Finish) ->
	Files=[
		begin
			{ok,File}=file:open(FileName,[read,raw,binary,{read_ahead,86400*8}]),
			File
		end || FileName <- FilesNames],
	Result=process_read_data_many(Files,SeriePeriod,Start,Finish,[]),
	[
		begin
			ok=file:close(File)
		end || File <- Files],
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
					case catch(process_read_values(File,SeriePeriod,BlockIndex,Left,Right,AccIn)) of
						{'EXIT',_Error} ->
							io:format("tss: process_read_data: ~p~n~p~n",[{SeriePeriod,Start,Finish},_Error]),
							AccIn;
						Result ->
							Result
					end;
				none ->
					AccIn
			end,
			process_read_data(File,SeriePeriod,Next,Finish,NewAccIn);
		false ->
			Mat={m,lists:reverse(AccIn)},
			{ok,Mat}
	end.

process_read_data_many(Files,SeriePeriod,Start,Finish,AccIn) ->
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
			Offset=get_value_offset(Left,SeriePeriod),
			Quantity=get_value_offset(Right,SeriePeriod)-Offset+8,
			EmptyBin=?emptyBlock(Quantity div 8),
			Bins=lists:map(
				fun(File) ->
					case get_index(File,SeriePeriod,Left,false) of
						{value,BlockIndex} ->
							{ok,Bin}=file:pread(File,BlockIndex+Offset,Quantity),
							Bin;
						none ->
							EmptyBin
					end
				end,
				Files
			),
			NewAccIn=process_binary_read_values_many(Left,SeriePeriod,Bins,AccIn),
			process_read_data_many(Files,SeriePeriod,Next,Finish,NewAccIn);
		false ->
			Mat={m,lists:reverse(AccIn)},
			{ok,Mat}
	end.

process_read_values(File,SeriePeriod,BlockIndex,Left,Right,AccIn) ->
	Offset=get_value_offset(Left,SeriePeriod),
	Quantity=get_value_offset(Right,SeriePeriod)-Offset+8,
	{ok,Bin}=file:pread(File,BlockIndex+Offset,Quantity),
	Result=process_binary_read_values(Left,SeriePeriod,Bin,AccIn),
	Result.

process_binary_read_values(Timestamp,SeriePeriod,Bin,AccIn) when byte_size(Bin)>0 ->
	{<<ReadValue:8/little-float-unit:8>>,Tail}=erlang:split_binary(Bin,8),
    NewAccIn=convert_read_value(ReadValue,Timestamp,AccIn),
	NewTimestamp=get_next_timestamp(Timestamp,SeriePeriod),
	process_binary_read_values(NewTimestamp,SeriePeriod,Tail,NewAccIn);
process_binary_read_values(_Timestamp,_SeriePeriod,Bin,AccIn) when byte_size(Bin)=<0 ->
	AccIn.
	
process_binary_read_values_many(Timestamp,SeriePeriod,Bins,AccIn) ->
    case lists:all(fun(Bin) -> byte_size(Bin)>0 end,Bins) of
    	true ->
    		ValuesAndTails=[
    			begin
	    			{<<ReadValue:8/little-float-unit:8>>,Tail}=erlang:split_binary(Bin,8),
	    			Value=convert_read_value(ReadValue),
	    			{Value,Tail}
	    		end || Bin <- Bins

    		],
    		{Values,Tails}=lists:unzip(ValuesAndTails),
    		NewAccIn=case lists:any(fun(undefined) -> true; (_) -> false end,Values) of
    			true ->
    				AccIn;
    			false ->
    				[[Timestamp|Values]|AccIn]
    		end,
			NewTimestamp=get_next_timestamp(Timestamp,SeriePeriod),
			process_binary_read_values_many(NewTimestamp,SeriePeriod,Tails,NewAccIn);
		false ->
			AccIn
	end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc Функции обработки и записи данных в файлы.
%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
store_data(FileName,SeriePeriod,Data) ->
	% T1=erlang:timestamp(),
	ParsedData=parse_data(SeriePeriod,Data),
	{ok,File}=file:open(FileName,[read,write,raw,binary,{read_ahead,86400*8}]),
	ok=process_write_data(File,SeriePeriod,gb_trees:iterator(ParsedData)),
	ok=file:close(File),
	% T2=erlang:timestamp(),
	% io:format("store_data: ~w~n",[timer:now_diff(T2,T1)]),
	ok.

parse_data(SeriePeriod,Data) when is_integer(SeriePeriod),SeriePeriod<86400 ->
	lists:foldl(
		fun(Row,AccIn) ->
			case Row of
				[{ts,Year,Month,Day,Hour,Minute,Second},Value] ->
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
		fun([{ts,Year,Month,Day,Hour,Minute,Second},Value],AccIn) ->
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
	undefined;
convert_read_value(0.0) ->
	undefined;
convert_read_value(?zeroValue) ->
	0.0;
convert_read_value(Value) ->
	Value.

convert_read_value(?emptyValue,_Timestamp,AccIn) ->
	AccIn;
convert_read_value(0.0,_Timestamp,AccIn) ->
	AccIn;
convert_read_value(?zeroValue,Timestamp,AccIn) ->
	[[Timestamp,0.0]|AccIn];
convert_read_value(Value,Timestamp,AccIn) ->
	[[Timestamp,Value]|AccIn].
	
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

align_timestamp(Timestamp,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod<60 ->
	Timestamp;
align_timestamp({ts,Year,Month,Day,Hour,Minute,_Second}=_Timestamp,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=60,SeriePeriod<86400 ->
	{ts,Year,Month,Day,Hour,Minute,0};
align_timestamp({ts,Year,Month,Day,_Hour,_Minute,_Second}=_Timestamp,SeriePeriod) when is_integer(SeriePeriod),SeriePeriod>=86400 ->
	{ts,Year,Month,Day,0,0,0}.

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
	{{Year,Month,Day},{Hour,Minute,Second}}=localtime:utc_to_local(calendar:now_to_universal_time(erlang:timestamp()),"Europe/Berlin"),
    {ts,Year,Month,Day,Hour,Minute,Second}.

add_seconds({ts,Year,Month,Day,Hour,Minute,Second},Seconds) ->
	V=calendar:datetime_to_gregorian_seconds({{Year,Month,Day},{Hour,Minute,Second}}),
    {{NewYear,NewMonth,NewDay}, {NewHour,NewMinute,NewSecond}} =
        calendar:gregorian_seconds_to_datetime(V+Seconds),
    {ts,NewYear,NewMonth,NewDay,NewHour,NewMinute,NewSecond}.

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

lt(Timestamp1,Timestamp2) ->
	comp(Timestamp1,Timestamp2,fun(A,B) -> A<B end).

gt(Timestamp1,Timestamp2) ->
	comp(Timestamp1,Timestamp2,fun(A,B) -> A>B end).

lte(Timestamp1,Timestamp2) ->
	comp(Timestamp1,Timestamp2,fun(A,B) -> A=<B end).

%%%%%%%%%%%%%%%%%%%%
%%% test функции %%%
%%%%%%%%%%%%%%%%%%%%
export_to_csv(Path) ->
	{ok,{m,Series}}=list_series(),
	FilteredSeries=lists:filter(
		fun([Name,Period]) -> ((Period==60) and (string:str(Name,":CME")>0)) end,
		Series
	),
	io:format("Start time: ~p~n",[calendar:local_time()]),
	io:format("FilteredSeries: ~p~n",[FilteredSeries]),
	io:format("length(FilteredSeries): ~p~n",[length(FilteredSeries)]),
	process_export(Path,FilteredSeries).

process_export(Path,[[SerieName,SeriePeriod]|Tail]) ->
	export_to_csv(SerieName,SeriePeriod,Path),
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
			{ok,{m,Data}}=load(SerieName,SeriePeriod,Left,Right),
			file:write(File,list_to_binary(io_lib:format("~w.~n",[{m,Data}]))),
			process_export_serie(File,SerieName,SeriePeriod,Next,Finish);
		false ->
			ok
	end.
gen_sample_data(Start,Finish,Period) ->
	StartTS=val(align_timestamp(Start,Period)),
	FinishTS=val(align_timestamp(Finish,Period)),
	gen_sample_data(StartTS,FinishTS,Period,Period*30,[]).

gen_sample_data(StartTS,FinishTS,Step,Period,Acc) ->
	case StartTS>FinishTS of
		true ->
			Acc;
		false ->
			TS=align_timestamp(ts(FinishTS),Step),
			Phase=(FinishTS rem Period)/Period,
			Value=1000*math:sin(2*math:pi()*Phase),
			NewRow=[TS,Value],
			NewAcc=[NewRow|Acc],
			gen_sample_data(StartTS,FinishTS-Step,Step,Period,NewAcc)
	end.

val({ts,Year,Month,Day,Hour,Minute,Second}) ->
	V1=calendar:datetime_to_gregorian_seconds({{Year,Month,Day},{Hour,Minute,Second}}),
    V2=calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}),
    V1-V2.

ts(V) ->
    Seconds = calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
    {{NewYear,NewMonth,NewDay}, {NewHour,NewMinute,NewSecond}} =
        calendar:gregorian_seconds_to_datetime(Seconds+V),
    {ts,NewYear,NewMonth,NewDay,NewHour,NewMinute,NewSecond}.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

test_() ->
	ok.

-endif.
