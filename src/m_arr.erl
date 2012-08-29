-module(m_arr).

-export(
    [
        m/0,
        m/1,
        m/2,
        m/3,
        m/4,
        m_rows/1,
        m_cols/1,
        to_res/2
    ]
).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

m() ->
    {m,0,0,array:new([{default,array:new([{default,undefined}])}])}.

m(Data) when is_list(Data) ->
    {_,M}=lists:foldl(
        fun(RowData,{RowPos,MAcc}) ->
            {_,MRow}=lists:foldl(
                fun(ColData,{ColPos,MAcc2}) ->
                    {ColPos+1,m(MAcc2,RowPos,ColPos,ColData)}
                end
            ,{1,MAcc},RowData),
            {RowPos+1,MRow}
        end
    ,{1,m()},Data),
    M.

m(Rows,Cols) when is_integer(Rows),is_integer(Cols) ->
    case (Rows>=1) and (Cols>=1) of
        true ->
            {m,Rows,Cols,array:new([{default,array:new([{default,undefined}])}])};
        false ->
           erlang:error("Bad arguments")
    end.

m({m,Rows,Cols,Values},Row,Col) when is_integer(Rows),is_integer(Cols),is_integer(Row),is_integer(Col) ->
    case (Row=<Rows) and (Row>=1) and (Col=<Cols) and (Col>=1) of
        true ->
        	R=array:get(Row-1,Values),
        	array:get(Col-1,R);
        false ->
           erlang:error("Out of bounds")
    end.

m({m,Rows,Cols,Values},Row,Col,Value) when is_integer(Rows),is_integer(Cols),is_integer(Row),is_integer(Col) ->
    case (Row>=1) and (Col>=1) of
        true ->
        	R=array:get(Row-1,Values),
        	NewR=array:set(Col-1,Value,R),
        	NewValues=array:set(Row-1,NewR,Values),
            NewRows=case Row>Rows of
                false ->
                    Rows;
                true ->
                    Row
            end,
            NewCols=case Col>Cols of
                false ->
                    Cols;
                true ->
                    Col
            end,
            {m,NewRows,NewCols,NewValues};
        false ->
            erlang:error("Out of bounds")
    end.

m_rows({m,Rows,Cols,_Values}) when is_integer(Rows),is_integer(Cols) ->
    Rows.

m_cols({m,Rows,Cols,_Values}) when is_integer(Rows),is_integer(Cols) ->
    Cols.

to_res({m,Rows,Cols,Values},Fun) when is_integer(Rows),is_integer(Cols) ->
    case (Rows>=1) and (Cols>=1) of
        true ->
            M=array:foldr(
                fun(_Row,RowData,AccIn) ->
                    NewRow=array:foldr(
				        fun(_Col,Cell,AccIn2) ->
		                    V=Fun(Cell),
		                    [V|AccIn2]
				        end,
				        [],
				        RowData
				    ),
				    [NewRow|AccIn]
                end,
                [],
                Values
            ),
            {matrix,M};
        false ->
           {matrix,[[]]}
    end.
    
%%%%%%%%%%%%%%%%%%%%
%%% test функции %%%
%%%%%%%%%%%%%%%%%%%%
-ifdef(TEST).
for(Start,Finish,Increment,Fun,Arg) when is_integer(Start),is_integer(Finish),is_integer(Increment),is_function(Fun,2) ->
    case (Start=<Finish) and (Increment>0) of
        true ->
            for_up(Start,Finish,Increment,Fun,Arg);
        false ->
            case (Start>=Finish) and (Increment<0) of
                true ->
                    for_down(Start,Finish,Increment,Fun,Arg);
                false ->
                    erlang:error("Bad arguments")
            end
    end.

for_up(Start,Finish,Increment,Fun,Arg) ->
    case Start=<Finish of
        true ->
            Result=Fun(Start,Arg),
            for_up(Start+Increment,Finish,Increment,Fun,Result);
        false ->
            Arg
    end.

for_down(Start,Finish,Increment,Fun,Arg) ->
    case Start>=Finish of
        true ->
            Result=Fun(Start,Arg),
            for_down(Start-Increment,Finish,Increment,Fun,Result);
        false ->
            Arg
    end.

test(Q) ->
	for(
		1,Q,1,
		fun(N,M0) ->
			Rows=m_rows(M0)+1,
			M1=m(M0,Rows,1,1*N),
			M2=m(M1,Rows,2,2*N),
			M2
		end,
		m()
	).

test_body() ->
	{Time,_Result}=timer:tc(?MODULE,test,[100000]),
	?debugVal(Time).
	
all_test_() ->
	[
		{timeout,3600,fun test_body/0}
	].

-endif.
