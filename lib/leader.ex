defmodule Leader do
  def start(config, monitor) do
    receive do
      {:bind, acceptors, replicas} ->
        b_num = {0, config.server_num} # special case where we use SERVER_NUM instead of self()
        spawn(Scout, :start, [monitor, config, self(), acceptors, b_num])
        send monitor, {:scout_spawned, config.server_num} #notify monitor

        next(monitor, config, acceptors, replicas, b_num, false, Map.new(), 1)
    end
  end

  defp next(monitor, config, acceptors, replicas, b_num, active, proposals, sleep_time) do
    receive do
      {:replica_propose, slot_num, cmd} ->
        #IO.puts "=================================A PROPOSAL ARRIVED"
        unless Map.has_key?(proposals, slot_num) do
          # if slot is not in proposal map
          proposals = Map.put(proposals, slot_num, cmd)
          if active do
            # if waiting for a commit to happen on b_num
            p_val = {b_num, slot_num, cmd}
            c_pid = spawn(Commander, :start, [monitor, config, self(), acceptors, replicas, p_val])
            #IO.puts "Leader #{inspect(self())} Spawning commander #{inspect(c_pid)} with p_val: #{inspect(p_val)}"
            send monitor, {:commander_spawned, config.server_num} #notify monitor
          end
          next(monitor, config, acceptors, replicas, b_num, active, proposals, sleep_time)
        else
          next(monitor, config, acceptors, replicas, b_num, active, proposals, sleep_time)
        end
      {:scout_adopted, scout_b_num, pvals} ->
        if scout_b_num == b_num do
          # majority number of acceptors have the same b_num as requested proposal
          #IO.puts "Leader #{inspect(self())} has adopted #{inspect(scout_b_num)}"
          #proposals = update_proposals(proposals, Map.to_list(pmax(MapSet.to_list(pvals), Map.new())))
          proposals = update_proposals_test(proposals, MapSet.to_list(pvals), Map.new())
          Enum.each(
            proposals,
            fn {s, c} ->
              p_val = {scout_b_num, s, c}
              spawn(Commander, :start, [monitor, config, self(), acceptors, replicas, p_val])
              #IO.puts "Leader #{inspect(self())} Spawning commander #{inspect(c_id)} with p_val: #{inspect(p_val)}"
              send monitor, {:commander_spawned, config.server_num}
              #notify monitor
            end
          )
          next(monitor, config, acceptors, replicas, b_num, true, proposals, sleep_time)
        else
          next(monitor, config, acceptors, replicas, b_num, false, proposals, sleep_time)
        end
      {:preempt_leader, {sqn, _} = preempt_b_num} ->
        if preempt_b_num > b_num do
          # cannot go through with proposal or commit as a acceptor has b_num greater than the b_num that has been
          # proposed or sent for commit
          # hence increment to ballot number to the lowest possible number that can still be accepted by acceptors
          b_num = {sqn + 1, elem(b_num, 1)}
          Process.sleep(sleep_time)
          s_pid = spawn(Scout, :start, [monitor, config, self(), acceptors, b_num])
          #IO.puts "Leader #{inspect(self())} one uped with #{inspect(b_num)} by creating #{inspect(s_pid)}"
          send monitor, {:scout_spawned, config.server_num} # notify monitor

          next(monitor, config, acceptors, replicas, b_num, false, proposals, sleep_time * 2)
        else
          next(monitor, config, acceptors, replicas, b_num, false, proposals, sleep_time)
        end
      {:commander_success} -> next(monitor, config, acceptors, replicas, b_num, active, proposals, max(1, sleep_time - 5))
    end
  end

  defp pmax([], max_pvals) do
    max_pvals
  end

  defp pmax([{b_num, slot_num, _} = pval | pvals], max_pvals) do
    pmax(pvals, elem(Map.get_and_update( max_pvals, slot_num, fn val ->
            if val != nil do
              # exists in map
              if elem(val, 0) < b_num do
                # current b_num is larger so replace it
                {val, pval}
              else
                # no not update
                {val, val}
              end
            else
              {val, pval}
            end
          end
        ),
        1
      )
    )
  end

  defp update_proposals(proposals, []) do
    #IO.puts "UPDATE PROPOSAL BASE=#{Map.size(proposals)}"
    proposals
  end

  defp update_proposals(proposals, [{slot_num, {_, _, cmd}} = pval | max_pvals]) do
    update_proposals(
      Map.put(proposals, slot_num, cmd),
      max_pvals
    )
  end

  defp update_proposals_test(proposals, accepted, pmax) do
    # Updates proposals for each slot with the slot proposal from the highest ballot
    if length(accepted) == 0 do
      proposals
    else
      [{ ballot, slot, op } | tail] = accepted
      if !Map.has_key?(pmax, slot) or Map.get(pmax, slot) < ballot do
        pmax = Map.put(pmax, slot, ballot)
        proposals = Map.put(proposals, slot, op)
        update_proposals_test(proposals, tail, pmax)
      else
        update_proposals_test(proposals, tail, pmax)
      end # if
    end # if
  end # update
end
