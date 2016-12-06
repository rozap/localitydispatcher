# Localitydispatcher

## Usage

Using the dispatcher

```elixir
defmodule LocalityProducer do
  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(:ok) do
    {:producer, :ok, dispatcher: LocalityDispatcher, mapper: fn event ->
      # This returns the "locale" of the event - the dispatcher
      # will dispatch the event to the subscribers whose locale
      # matches this
      event.node
    end}
  end

  defp generate_events(demand) do
    nodes = [Node.self | Node.list]

    Enum.map(0..demand, fn i ->
      # In real life, events may need to be sent to
      # some specific node in the cluster (for data locality purposes),
      # here we just generate events that prefer
      # some random locale
      %{"name" => "Some event #{i}", node: Enum.random(nodes)}
    end)
  end

  def handle_demand(demand, state) do
    events = generate_events(demand)
    {:noreply, events, state}
  end
end
```

Subscribing

```elixir
{:ok, producer} = LocalityProducer.start_link()


Enum.each([Node.self | Node.list], fn node ->
  {:ok, c} = SomeConsumer.start_link()

  # Subscribe passing the locale option which specifies
  # what location the node is in, in the cluster
  GenStage.sync_subscribe(c, to: producer, locale: Node.self)
end)
```

Now events will be dispatched to the node who's locale matches the
event locale, which is the result of applying the `mapper/1` function
to the event.