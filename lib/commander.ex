# Ilyas Saltykov (is916) and Shashwat Dalal (spd16)

# distributed algorithms, n.dulay 11 feb 19
# coursework 2, paxos made moderately complex

defmodule Commander do

  def start(monitor, config, l_pid, acceptors, replicas, pval) do
    send monitor, {:commander_spawned, config.server_num}
    # send commit request of pval = {b_num, slot_num, cmd} to all acceptors
    for a <- acceptors, do: send a, {:commander_p2a, self(), pval}
    next(monitor, config, l_pid, acceptors, replicas, MapSet.new(acceptors), pval)
  end

  defp next(monitor, config, l_pid, acceptors, replicas, waitfor, {b_num, slot_num, cmd} = pval) do
    receive do
      {:acceptor_p2b, a_pid, a_b_num} ->
        if b_num == a_b_num do
          # update our proposed values
          waitfor = MapSet.delete(waitfor, a_pid)
          if MapSet.size(waitfor) < (length(acceptors) / 2) do
            # majority of acceptors have commited our request so send all replicas the decision
            for r_pid <- replicas, do: send r_pid, {:commander_decision, slot_num, cmd}
            # tell leader commander succeeded
            send l_pid, {:commander_success}
            # add to monitor decisions tally
            send monitor, {:commander_success, config.server_num}
            # kill node
            exit(monitor, config)
          end
          next(monitor, config, l_pid, acceptors, replicas, waitfor, pval)
        else
          # an acceptor has rejected our request for commit because it
          # has already received a higher ballot number
          send l_pid, {:preempt_leader, a_b_num}
          exit(monitor, config)
        end
    end
  end

  defp exit(monitor, config) do
    #send monitor, {:commander_finished, config.server_num}
    Process.exit(self(), :normal)
  end

end

