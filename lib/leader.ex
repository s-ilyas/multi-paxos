defmodule Leader do
  def start(l_id, acceptors, replicas) do
    b_num = {0, l_id}
    spawn(Scout, :start, [self(), acceptors, b_num])
    next(acceptors, replicas, b_num, false, MapSet.new())
  end

  defp next(acceptors, replicas, b_num, active, proposals) do
    receive do
      {:replica_propose, slot_num, cmd} ->
        unless Map.has_key?(proposals, slot_num) do
          # if slot is not in proposal map
          proposals = Map.put(proposals, slot_num, cmd)
          if active do
            # if waiting for a commit to happen on b_num
            p_val = {b_num, slot_num, cmd}
            spawn(Commander, :start, [self(), acceptors, replicas, p_val])
          end
          next(acceptors, replicas, b_num, active, proposals)
        end
      {:scout_adopted, scout_b_num, pvals} ->
        # majority number of acceptors have the same b_num as requested proposal
        proposals = update_proposals(MapSet.to_list(proposals), pmax(pvals, Map.new()), MapSet.new())
        Enum.each(proposals, fn {s, c} ->
            p_val = {b_num, s, c}
            spawn(Commander, :start, [self(), acceptors, replicas, p_val])
        end)
        next(acceptors, replicas, b_num, true, proposals)
      {:preempt_leader, preempt_b_num = {sqn, _}} ->
        if preempt_b_num > b_num do
          # cannot go throught with proposal or commit as a acceptor has b_num greater than the b_num that has been
          # proposed or sent for commit
          # hence increment to ballot number to the lowest possible number that can still be accepted by acceptors
          active = false
          b_num = {sqn + 1, l_id}
          spawn(Scout, :start, [self(), acceptors, b_num])
          next(acceptors, replicas, b_num, false, proposals)
        else
          next(acceptors, replicas, b_num, active, proposals)
        end
    end
  end

  defp pmax([], max_pvals) do
    max_pvals
  end

  defp pmax([pval={b_num, slot_num, cmd} | pvals], max_pvals) do
    pmax(pvals, elem(Map.get_and_update(max_pvals, slot_num, fn val ->
      if val != nil do
        # exits in map
        if elem(val,0) < b_num do
          # current b_num is larger so replace it
          {val, pval}
        else
          # no not update
          {val, val}
        end
      else
        # does not exist in map
        {val, pval}
      end
    end),1))
  end

  defp update_proposals([], _, updated_proposals) do
    updated_proposals
  end

  defp update_proposals([{slot_num, cmd} | proposals], max_pvals, updated_proposals) do
    update_proposals(proposals, max_pvals,
      MapSet.put(updated_proposals, {slot_num, elem(Map.get(max_pvals, slot_num, {-1, -1, cmd}), 2)}))
  end
end
