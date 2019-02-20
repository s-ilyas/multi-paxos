defmodule Replica do

  def start(config, database_pid, _) do
    receive do
      {:bind, leaders} -> next(leaders, database_pid, config.window, 1, 1, [], Map.new, Map.new)
    end
  end

  defp next(leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions) do
    receive do
      {:client_request, cmd} ->
        #IO.puts "CLIENT_REQUEST"
        requests = requests ++ [cmd]
        propose(leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions)
      {:commander_decision, slot, cmd} ->
        #IO.puts "COMMANDER_DECISION"
        decisions = Map.put(decisions, slot, {slot, cmd})
        process_decision(leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions)
    end
  end

  defp process_decision(leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions) do
    if Map.has_key?(decisions, slot_out) do
      {_, decision_cmd} = Map.get(decisions, slot_out)
      # decisions contain command at slot_out
      if Map.has_key?(proposals, slot_out) do
        # proposal also contain command at slot_out so update proposals and requests accordingly
        {{_, proposal_cmd}, proposals} = Map.pop(proposals, slot_out)
        requests =
          case decision_cmd == proposal_cmd do
            true -> requests
            # proposal command and decision command are not equal
            # so re-add the proposal command to requests
            false -> requests ++ [proposal_cmd]
          end
        perform(leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions, decision_cmd)
      else
        perform(leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions, decision_cmd)
      end
    else
      # end performing decisions, as decisions do not contain values at slot_out
      propose(leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions)
    end
  end

  defp propose(leaders, database_pid, window, slot_in, slot_out, [], proposals, decisions) do
    # all requests within window were sent to leaders for proposal, so end while loop
    next(leaders, database_pid, window, slot_in, slot_out, [], proposals, decisions)
  end

  defp propose(leaders, database_pid, window, slot_in, slot_out, [req | reqs], proposals, decisions) do
    if (slot_in < slot_out + window) do
      unless Map.has_key?(decisions, slot_in) do
        # slot_in command not in decision
        for l_pid <- leaders, do: send l_pid, {:replica_propose, slot_in, req}
      end

      requests =
        case Map.has_key?(decisions, slot_in) do
          false -> reqs
          true -> [req | reqs]
        end
      proposals =
        case Map.has_key?(decisions, slot_in) do
          false -> Map.put(proposals, slot_in, {slot_in, req})
          true -> proposals
        end
      propose(leaders, database_pid, window, slot_in + 1, slot_out, requests, proposals, decisions)
    else
      # stop proposing requests as it oustide window
      next(leaders, database_pid, window, slot_in, slot_out, [req | reqs], proposals, decisions)
    end
  end

  defp perform(leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions, command) do
    cmd_matches = Enum.filter(Map.to_list(decisions), fn {s, cmd} -> cmd == command && s < slot_out end)
    unless length(cmd_matches) > 0 do
      send database_pid, {:execute, command}
    end
    process_decision(leaders, database_pid, window, slot_in, slot_out + 1, requests, proposals, decisions)
  end
end
