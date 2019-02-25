# Ilyas Saltykov (is916) and Shashwat Dalal (spd16)

defmodule Leader do
  def start(config, monitor) do
    receive do
      {:bind, acceptors, replicas} ->
        b_num = {0, config.server_num} # special case where we use SERVER_NUM instead of self()
        spawn(Scout, :start, [monitor, config, self(), acceptors, b_num])

        next(monitor, config, acceptors, replicas, b_num, false, Map.new())
    end
  end

  defp next(monitor, config, acceptors, replicas, b_num, active, proposals) do
    receive do
      {:replica_propose, slot_num, cmd} ->
        unless Map.has_key?(proposals, slot_num) do
          # if slot is not in proposal map
          proposals = Map.put(proposals, slot_num, cmd)
          if active do
            # if waiting for a commit to happen on b_num
            p_val = {b_num, slot_num, cmd}
            c_pid = spawn(Commander, :start, [monitor, config, self(), acceptors, replicas, p_val])
          end
          next(monitor, config, acceptors, replicas, b_num, active, proposals)
        else
          next(monitor, config, acceptors, replicas, b_num, active, proposals)
        end
      {:scout_adopted, scout_b_num, pvals} ->
        if scout_b_num == b_num do
          # majority number of acceptors have the same b_num as requested proposal
          proposals = update_proposals(proposals, Map.to_list(pmax(MapSet.to_list(pvals), Map.new())))
          Enum.each(
            proposals,
            fn {s, c} ->
              p_val = {scout_b_num, s, c}
              spawn(Commander, :start, [monitor, config, self(), acceptors, replicas, p_val])
              #notify monitor
            end
          )
          next(monitor, config, acceptors, replicas, b_num, true, proposals)
        else
          next(monitor, config, acceptors, replicas, b_num, false, proposals)
        end
      {:preempt_leader, {sqn, _} = preempt_b_num} ->
        if preempt_b_num > b_num do
          # cannot go through with proposal or commit as a acceptor has b_num greater than the b_num that has been
          # proposed or sent for commit
          # hence increment to ballot number to the lowest possible number that can still be accepted by acceptors
          b_num = {sqn + 1, elem(b_num, 1)}
          sleep_time = Enum.random(1..Map.size(proposals))
          Process.sleep(sleep_time)
          s_pid = spawn(Scout, :start, [monitor, config, self(), acceptors, b_num])
          #IO.puts "Leader #{inspect(self())} one uped with #{inspect(b_num)} by creating #{inspect(s_pid)}"

          next(monitor, config, acceptors, replicas, b_num, false, proposals)
        else
          next(monitor, config, acceptors, replicas, b_num, false, proposals)
        end
      {:commander_success} -> next(monitor, config, acceptors, replicas, b_num, active, proposals)
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
    proposals
  end

  defp update_proposals(proposals, [{slot_num, {_, _, cmd}} = pval | max_pvals]) do
    update_proposals(
      Map.put(proposals, slot_num, cmd),
      max_pvals
    )
  end
end
