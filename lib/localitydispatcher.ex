alias Experimental.GenStage

defmodule LocalityDispatcher do
  @moduledoc """
  A dispatcher that sends batches to the highest demand.

  This is the default dispatcher used by `GenStage`. In order
  to avoid greedy consumers, it is recommended that all consumers
  have exactly the same maximum demand.
  """

  @behaviour GenStage.Dispatcher

  @doc false
  def init(opts) do
    mapper = Keyword.get(opts, :mapper)

    if is_nil(mapper) do
      raise ArgumentError, "the :mapper option must be specified for the locality dispatcher"
    end

    {:ok, {[], 0, nil, mapper}}
  end

  @doc false
  def notify(msg, {demands, _, _, _} = state) do
    Enum.each(demands, fn {_, pid, ref, _} ->
      Process.send(pid, {:"$gen_consumer", {self(), ref}, {:notification, msg}}, [:noconnect])
    end)
    {:ok, state}
  end

  @doc false
  def subscribe(opts, {pid, ref}, {demands, pending, max, mapper}) do
    locale = Keyword.get(opts, :locale)

    if is_nil(locale) do
      raise ArgumentError, "the :locale option is required when subscribing to a producer with locality dispatcher"
    end

    {:ok, 0, {demands ++ [{0, pid, ref, locale}], pending, max, mapper}}
  end

  @doc false
  def cancel({_, ref}, {demands, pending, max, mapper}) do
    {current, _locale, demands} = pop_demand(ref, demands)
    {:ok, 0, {demands, current + pending, max, mapper}}
  end

  @doc false
  def ask(counter, {pid, ref}, {demands, pending, max, mapper}) do
    max = max || counter

    if counter > max do
      :error_logger.warning_msg('GenStage producer DemandDispatcher expects a maximum demand of ~p. ' ++
                                'Using different maximum demands will overload greedy consumers. ' ++
                                'Got demand for ~p events from ~p~n', [max, counter, pid])
    end

    {current, locale, demands} = pop_demand(ref, demands)
    demands = add_demand(current + counter, pid, ref, locale, demands)

    already_sent = min(pending, counter)
    {:ok, counter - already_sent, {demands, pending - already_sent, max, mapper}}
  end

  @doc false
  def dispatch(events, length, {demands, pending, max, mapper}) do
    {events, demands} = dispatch_demand(events, length, demands, mapper)
    {:ok, events, {demands, pending, max, mapper}}
  end

  defp dispatch_demand([], _length, demands, _) do
    {[], demands}
  end
  defp dispatch_demand(events, num_dispatch, demands, mapper) do
    by_locale = Enum.group_by(events, mapper)

    {by_locale, _, demands} = Enum.reduce(
      demands,
      {by_locale, num_dispatch, demands},

      fn
        {0, _, _, _}, acc -> acc

        {counter, pid, ref, locale}, {by_locale, num_dispatch, demands} ->
          case Map.get(by_locale, locale, []) do
            [] ->
              {by_locale, num_dispatch, demands}
            local_events ->
              {now, later, counter} = split_events(local_events, counter)
              Process.send(pid, {:"$gen_consumer", {self(), ref}, now}, [:noconnect])

              demands = Enum.filter(demands, fn
                {_, _, ^ref, _} -> false
                _ -> true
              end)
              demands = add_demand(counter, pid, ref, locale, demands)
              by_locale = Map.put(by_locale, locale, later)
              {by_locale, num_dispatch - length(now), demands}
          end
    end)

    leftovers = Enum.flat_map(by_locale, fn {_, events} -> events end)

    {leftovers, demands}
  end

  defp split_events(events, counter) when length(events) <= counter do
    {events, [], counter - length(events)}
  end
  defp split_events(events, counter) do
    {now, later} = Enum.split(events, counter)
    {now, later, 0}
  end

  defp add_demand(counter, pid, ref, locale, [{c, _, _, _} | _] = demands) when counter > c,
    do: [{counter, pid, ref, locale} | demands]
  defp add_demand(counter, pid, ref, locale, [demand | demands]),
    do: [demand | add_demand(counter, pid, ref, locale, demands)]
  defp add_demand(counter, pid, ref, locale, []) when is_integer(counter),
    do: [{counter, pid, ref, locale}]

  defp pop_demand(ref, demands) do
    {{current, _pid, ^ref, locale}, rest} = List.keytake(demands, ref, 2)
    {current, locale, rest}
  end
end
