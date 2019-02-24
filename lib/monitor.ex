# Ilyas Saltykov (is916) and Shashwat Dalal (spd16)

# distributed algorithms, n.dulay 11 feb 19
# coursework 2, paxos made moderately complex

defmodule Monitor do

def start config do
  Process.send_after self(), :print, config.print_after
  next config, 0, Map.new, Map.new, Map.new, Map.new, Map.new, Map.new
end # start

defp next config, clock, requests, updates, transactions, scouts, commanders, commander_decisions do
  receive do
  { :db_update, db, seqnum, transaction } ->
    { :move, amount, from, to } = transaction

    done = Map.get updates, db, 0

    if seqnum != done + 1  do
      IO.puts "  ** error db #{db}: seq #{seqnum} expecting #{done+1}"
      System.halt 
    end

    transactions = 
      case Map.get transactions, seqnum do
      nil ->
        # IO.puts "db #{db} seq #{seqnum} #{done}"
        Map.put transactions, seqnum, %{ amount: amount, from: from, to: to }   

      t -> # already logged - check transaction
        if amount != t.amount or from != t.from or to != t.to do
	  IO.puts " ** error db #{db}.#{done} [#{amount},#{from},#{to}] " <>
            "= log #{done}/#{map_size transactions} [#{t.amount},#{t.from},#{t.to}]"
          System.halt 
        end
        transactions
      end # case

    updates = Map.put updates, db, seqnum 
    next config, clock, requests, updates, transactions, scouts, commanders, commander_decisions
      
  { :client_request, server_num } ->  # client requests seen by replicas
    seen = Map.get requests, server_num, 0
    requests = Map.put requests, server_num, seen + 1
    next config, clock, requests, updates, transactions, scouts, commanders, commander_decisions

  { :scout_spawned, server_num } -> # increment active scouts  
    spawned = Map.get scouts, server_num, 0
    scouts = Map.put scouts, server_num, spawned + 1
    next config, clock, requests, updates, transactions, scouts, commanders, commander_decisions

  { :scout_finished, server_num } -> # decrement active scouts 
    scouts = Map.replace! scouts, server_num, scouts[server_num] - 1
    next config, clock, requests, updates, transactions, scouts, commanders, commander_decisions

  { :commander_spawned, server_num } -> # increment active commanders
    spawned = Map.get commanders, server_num, 0
    commanders = Map.put commanders, server_num, spawned + 1
    next config, clock, requests, updates, transactions, scouts, commanders, commander_decisions

  { :commander_finished, server_num } -> # decrement active commanders
    commanders = Map.replace! commanders, server_num, commanders[server_num] - 1
    next config, clock, requests, updates, transactions, scouts, commanders, commander_decisions

  { :commander_success, server_num } -> # success on making a decision
    commander_decisions = Map.put commander_decisions, server_num, Map.get(commander_decisions, server_num, 0) + 1
    next config, clock, requests, updates, transactions, scouts, commanders, commander_decisions
  # ** ADD ADDITIONAL MESSAGES HERE

  :print -> 
    clock = clock + config.print_after 
    sorted = updates |> Map.to_list |> List.keysort(0)
    IO.puts "time = #{clock}  updates done = #{inspect sorted}"
    sorted = requests |> Map.to_list |> List.keysort(0)
    IO.puts "time = #{clock} requests seen = #{inspect sorted}"

    if config.debug_level == 1 do
      min_done = updates |> Map.values |> Enum.min
      n_requests = requests |> Map.values |> Enum.sum
      IO.puts "time = #{clock}    total seen = #{n_requests} max lag = #{n_requests-min_done}"

      sorted_commanders = commanders |> Map.to_list |> List.keysort(0)
      sorted = for {s, n} <- sorted_commanders, do: {s, "#{:erlang.float_to_binary((commander_decisions[s] / n) * 100, [decimals: 2])}%"}
      IO.puts "time = #{clock}       CS Rate = #{inspect sorted}"

      sorted = scouts |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}        scouts = #{inspect sorted}"
      sorted = commanders |> Map.to_list |> List.keysort(0)
      IO.puts "time = #{clock}    commanders = #{inspect sorted}"
    end

    IO.puts ""
    Process.send_after self(), :print, config.print_after
    next config, clock, requests, updates, transactions, scouts, commanders, commander_decisions

  _ -> 
    IO.puts "monitor: unexpected message"
    System.halt
  end # receive
end # next

end # Monitor

