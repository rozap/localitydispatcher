alias Experimental.GenStage

defmodule LocalityDispatcherTest do
  use ExUnit.Case, async: true
  doctest LocalityDispatcher
  import ExUnit.CaptureLog

  alias LocalityDispatcher, as: D

  defp dispatcher(opts \\ [mapper: :noop]) do
    {:ok, state} = D.init(opts)
    state
  end

  test "subscribes and cancels" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher()

    {:ok, 0, disp} = D.subscribe([locale: :a], {pid, ref}, disp)
    assert disp == {[{0, pid, ref, :a}], 0, nil, :noop}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert disp == {[], 0, nil, :noop}
  end

  test "subscribes, asks and cancels" do
    pid  = self()
    ref  = make_ref()
    disp = dispatcher()

    # Subscribe, ask and cancel and leave some demand
    {:ok, 0, disp} = D.subscribe([locale: :a], {pid, ref}, disp)
    assert disp == {[{0, pid, ref, :a}], 0, nil, :noop}

    {:ok, 10, disp} = D.ask(10, {pid, ref}, disp)
    assert disp == {[{10, pid, ref, :a}], 0, 10, :noop}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert disp == {[], 10, 10, :noop}

    # Subscribe, ask and cancel and leave the same demand
    {:ok, 0, disp} = D.subscribe([locale: :a], {pid, ref}, disp)
    assert disp == {[{0, pid, ref, :a}], 10, 10, :noop}

    {:ok, 0, disp} = D.ask(5, {pid, ref}, disp)
    assert disp == {[{5, pid, ref, :a}], 5, 10, :noop}

    {:ok, 0, disp} = D.cancel({pid, ref}, disp)
    assert disp == {[], 10, 10, :noop}
  end

  test "subscribes, asks and dispatches" do
    pid  = self()
    ref  = make_ref()
    mapper = fn e -> e end
    disp = dispatcher(mapper: mapper)
    {:ok, 0, disp} = D.subscribe([locale: :a], {pid, ref}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    assert disp == {[{3, pid, ref, :a}], 0, 3, mapper}

    {:ok, [], disp} = D.dispatch([:a], 1, disp)
    assert disp == {[{2, pid, ref, :a}], 0, 3, mapper}
    assert_received {:"$gen_consumer", {_, ^ref}, [:a]}

    {:ok, 3, disp} = D.ask(3, {pid, ref}, disp)
    assert disp == {[{5, pid, ref, :a}], 0, 3, mapper}

    {:ok, [:a, :a], disp} = D.dispatch([:a, :a, :a, :a, :a, :a, :a], 7, disp)
    assert disp == {[{0, pid, ref, :a}], 0, 3, mapper}
    assert_received {:"$gen_consumer", {_, ^ref}, [:a, :a, :a, :a, :a]}

    {:ok, [:i, :j], disp} = D.dispatch([:i, :j], 2, disp)
    assert disp == {[{0, pid, ref, :a}], 0, 3, mapper}
    refute_received {:"$gen_consumer", {_, ^ref}, _}
  end

  test "subscribes, asks multiple consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    ref3 = make_ref()
    mapper = fn e -> e end
    disp = dispatcher([mapper: mapper])

    {:ok, 0, disp} = D.subscribe([locale: :a], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([locale: :a], {pid, ref2}, disp)
    {:ok, 0, disp} = D.subscribe([locale: :b], {pid, ref3}, disp)

    {:ok, 4, disp} = D.ask(4, {pid, ref1}, disp)
    {:ok, 2, disp} = D.ask(2, {pid, ref2}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref3}, disp)
    assert disp == {[
      {4, pid, ref1, :a},
      {3, pid, ref3, :b},
      {2, pid, ref2, :a}
    ], 0, 4, mapper}

    {:ok, 2, disp} = D.ask(2, {pid, ref3}, disp)
    assert disp == {[
      {5, pid, ref3, :b},
      {4, pid, ref1, :a},
      {2, pid, ref2, :a}
    ], 0, 4, mapper}

    {:ok, 4, disp} = D.ask(4, {pid, ref2}, disp)
    assert disp == {[
      {6, pid, ref2, :a},
      {5, pid, ref3, :b},
      {4, pid, ref1, :a}
    ], 0, 4, mapper}
  end

  test "subscribes, asks and dispatches to multiple consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    mapper = fn e -> e end
    disp = dispatcher([mapper: mapper])

    {:ok, 0, disp} = D.subscribe([locale: :a], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([locale: :b], {pid, ref2}, disp)

    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
    {:ok, 2, disp} = D.ask(2, {pid, ref2}, disp)
    assert disp == {[{3, pid, ref1, :a}, {2, pid, ref2, :b}], 0, 3, mapper}

    # One batch fits all
    {:ok, [], disp} = D.dispatch([:a, :b, :a, :b, :a], 5, disp)
    assert disp == {[{0, pid, ref1, :a}, {0, pid, ref2, :b}], 0, 3, mapper}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a, :a, :a]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:b, :b]}

    {:ok, [:a, :b, :c], disp} = D.dispatch([:a, :b, :c], 3, disp)
    assert disp == {[{0, pid, ref1, :a}, {0, pid, ref2, :b}], 0, 3, mapper}
    refute_received {:"$gen_consumer", {_, _}, _}

    # Two batches with left over
    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref2}, disp)
    assert disp == {[
        {3, pid, ref1, :a},
        {3, pid, ref2, :b}
      ],
      0, 3, mapper
    }

    {:ok, [], disp} = D.dispatch([:a, :b], 2, disp)

    assert disp == {[
        {2, pid, ref1, :a},
        {2, pid, ref2, :b}
      ],
      0, 3, mapper
    }
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:b]}

    {:ok, [:c], disp} = D.dispatch([:a, :b, :c], 2, disp)
    assert disp == {[
      {1, pid, ref1, :a},
      {1, pid, ref2, :b}
    ], 0, 3, mapper}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a]}
    assert_received {:"$gen_consumer", {_, ^ref2}, [:b]}

    # Eliminate the left-over
    {:ok, [:a, :a], disp} = D.dispatch([:a, :a, :a], 3, disp)
    assert disp == {[
      {1, pid, ref2, :b},
      {0, pid, ref1, :a}
    ], 0, 3, mapper}
    assert_received {:"$gen_consumer", {_, ^ref1}, [:a]}
  end

  test "delivers notifications to all consumers" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    mapper = fn e -> e end
    disp = dispatcher([mapper: mapper])

    {:ok, 0, disp} = D.subscribe([locale: :a], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([locale: :b], {pid, ref2}, disp)
    {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)

    {:ok, notify_disp} = D.notify(:hello, disp)
    assert disp == notify_disp

    assert_received {:"$gen_consumer", {_, ^ref1}, {:notification, :hello}}
    assert_received {:"$gen_consumer", {_, ^ref2}, {:notification, :hello}}
  end

  test "warns on demand mismatch" do
    pid  = self()
    ref1 = make_ref()
    ref2 = make_ref()
    mapper = fn e -> e end
    disp = dispatcher([mapper: mapper])
    {:ok, 0, disp} = D.subscribe([locale: :a], {pid, ref1}, disp)
    {:ok, 0, disp} = D.subscribe([locale: :b], {pid, ref2}, disp)

    assert capture_log(fn ->
      {:ok, 3, disp} = D.ask(3, {pid, ref1}, disp)
      {:ok, 4, disp} = D.ask(4, {pid, ref2}, disp)
      disp
    end) =~ "GenStage producer DemandDispatcher expects a maximum demand of 3"
  end
end
