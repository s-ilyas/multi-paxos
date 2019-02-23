defmodule Replica do

  def start(config, database_pid, monitor) do
    receive do
      {:bind, leaders} -> next(monitor, config, leaders, database_pid, config.window, 1, 1, [], Map.new, Map.new)
    end
  end

  defp next(monitor, config, leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions) do
    receive do
      {:commander_decision, slot, cmd} ->
        decisions = Map.put(decisions, slot, {slot, cmd})
        process_decision(monitor, config, leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions)
      {:client_request, cmd} ->
        send monitor, {:client_request, config.server_num} #notify monitor that a request has been received
        requests = requests ++ [cmd]
        propose(monitor, config, leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions)
    end
  end

  defp process_decision(monitor, config, leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions) do
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
        perform(monitor, config, leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions, decision_cmd)
      else
        perform(monitor, config, leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions, decision_cmd)
      end
    else
      # end performing decisions, as decisions do not contain values at slot_out
      propose(monitor, config, leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions)
    end
  end

  defp propose(monitor, config, leaders, database_pid, window, slot_in, slot_out, [], proposals, decisions) do
   # all requests within window were sent to leaders for proposal, so end while loop
    next(monitor, config, leaders, database_pid, window, slot_in, slot_out, [], proposals, decisions)
  end

  defp propose(monitor, config, leaders, database_pid, window, slot_in, slot_out, [req | reqs], proposals, decisions) do
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
      propose(monitor, config, leaders, database_pid, window, slot_in + 1, slot_out, requests, proposals, decisions)
    else
      # stop proposing requests as it oustide window
      next(monitor, config, leaders, database_pid, window, slot_in, slot_out, [req | reqs], proposals, decisions)
    end
  end

  defp perform(monitor, config, leaders, database_pid, window, slot_in, slot_out, requests, proposals, decisions, command) do
    unless Enum.any?(decisions, fn {s, {_, cmd}} -> cmd == command and s < slot_out end) do
      send database_pid, {:execute, elem(command,2)}
    end
    process_decision(monitor, config, leaders, database_pid, window, slot_in, slot_out + 1, requests, proposals, decisions)
  end
end
