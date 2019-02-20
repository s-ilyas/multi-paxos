# Ilyas Saltykov (is916) and Shashwat Dalal (spd16)

# distributed algorithms, n.dulay 11 feb 19
# coursework 2, paxos made moderately complex

defmodule Acceptor do

  def start() do
    # -1 instead of the default BOTTOM value
    b_num = -1
    accepted = MapSet.new()
    next(b_num, accepted)
  end

  defp next(b_num, accepted) do
    receive do
      {:scout_p1a, s_pid, s_b_num} ->
        # if condition is true then proposal accepted
        # otherwise proposal rejected
        b_num =
          case  s_b_num > b_num do
            true -> s_b_num
            false -> b_num
          end
        send s_pid, {:acceptor_p1b, self(), b_num, accepted}
        next(b_num, accepted)
      {:commander_p2a, c_pid, pvalue} ->
        # if condition not true, then acceptor has seen a higher
        # ballot number, hence cannot procced with commit
        accepted =
          case c_b_num == b_num do
            true -> MapSet.put(accepted, pvalue)
            false -> accepted
          end
        send c_pid, {:acceptor_p2b, self(), b_num}
        next(b_num, accepted)
    end
  end
end
