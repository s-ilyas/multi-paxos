# Ilyas Saltykov (is916) and Shashwat Dalal (spd16)

# distributed algorithms, n.dulay 11 feb 19
# coursework 2, paxos made moderately complex

defmodule Scout do
  def start(l_pid, acceptors, b_num) do
    # send proposal of b_num to all acceptors
    for a_pid <- acceptors, do: send a_pid, {:scout_p1a, self(), b_num}
    next(l_pid, acceptors, b_num, MapSet.new(acceptors), MapSet.new())
  end

  defp next(l_pid, acceptors, b_num, waitfor, p_vals) do
    receive do
      {:acceptor_p1b, a_pid, a_b_num, a_p_vals} ->
        if b_num == a_b_num do
          # update our proposed values
          p_vals = MapSet.union(p_vals, a_p_vals)
          waitfor = MapSet.delete(waitfor, a_pid)
          if MapSet.size(waitfor) < (MapSet.size(acceptors) / 2) do
            # majority of acceptors have made a promise on our proposal
            send l_pid, {:scout_adopted, b_num, p_vals}
            # kill node
            exit()
          end
        else
          # an acceptor has rejected our proposal because it
          # has already received a higher ballot number
          send l_pid, {:scout_preempted, a_b_num}
          exit()
        end
    end
  end

  defp exit() do
    Process.exit(self(),:normal)
  end

end
