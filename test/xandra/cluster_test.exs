defmodule Xandra.ClusterTest do
  use ExUnit.Case, async: false

  import Mox

  alias Xandra.ConnectionError
  alias Xandra.Cluster
  alias Xandra.Cluster.Host
  alias Xandra.Cluster.Pool

  @protocol_version XandraTest.IntegrationCase.protocol_version()
  @port XandraTest.IntegrationCase.port()

  defmodule PoolMock do
    use Supervisor

    def start_link(opts) do
      map_opts =
        opts
        |> Map.new()
        |> Map.update(:connection_options, %{}, &Map.new/1)

      Supervisor.start_link(__MODULE__, map_opts)
    end

    @impl true
    def init(opts) do
      {test_pid, test_ref} = :persistent_term.get(:clustering_test_info)
      send(test_pid, {test_ref, __MODULE__, :init_called, opts})
      Supervisor.init([], strategy: :one_for_one)
    end
  end

  defmodule ControlConnectionMock do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))
    def stop(pid), do: GenServer.stop(pid)

    @impl true
    def init(args) do
      {test_pid, test_ref} = :persistent_term.get(:clustering_test_info)
      send(test_pid, {test_ref, __MODULE__, :init_called, args})
      {:ok, {test_pid, test_ref}}
    end

    @impl true
    def handle_info({:started_pool, %Host{}}, state) do
      {:noreply, state}
    end
  end

  defmacrop assert_telemetry(postfix, meta) do
    quote do
      event = [:xandra, :cluster] ++ unquote(postfix)
      telemetry_ref = var!(telemetry_ref)
      assert_receive {^event, ^telemetry_ref, measurements, unquote(meta)}, 2000
      assert measurements == %{}
    end
  end

  setup :set_mox_global
  setup :verify_on_exit!

  setup do
    test_ref = make_ref()
    :persistent_term.put(:clustering_test_info, {self(), test_ref})
    on_exit(fn -> :persistent_term.erase(:clustering_test_info) end)
    %{test_ref: test_ref}
  end

  setup do
    opts = [nodes: ["127.0.0.1:#{@port}"], sync_connect: 1000]

    opts =
      if @protocol_version do
        Keyword.put(opts, :protocol_version, @protocol_version)
      else
        opts
      end

    %{base_options: opts}
  end

  setup context do
    case Map.fetch(context, :telemetry_events) do
      {:ok, events} -> %{telemetry_ref: :telemetry_test.attach_event_handlers(self(), events)}
      :error -> %{}
    end
  end

  describe "start_link/1 validation" do
    test "with the :nodes option" do
      message = ~r/invalid list in :nodes option: invalid value for list element/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["foo:bar"])
      end

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042x"])
      end
    end

    test "with the :autodiscovered_nodes_port option" do
      message = ~r/invalid value for :autodiscovered_nodes_port option/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], autodiscovered_nodes_port: 99_999)
      end
    end

    test "with the :load_balancing option" do
      message = ~r/expected :load_balancing option to match at least one given type/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], load_balancing: :inverse)
      end
    end

    test "with the :refresh_topology_interval option" do
      message = ~r/invalid value for :refresh_topology_interval option/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], refresh_topology_interval: -1)
      end
    end

    test "with the :target_pools option" do
      message = ~r/invalid value for :target_pools option: expected positive integer/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], target_pools: -1)
      end
    end

    test "with the :name option" do
      message = ~r/expected :name/

      assert_raise ArgumentError, message, fn ->
        Xandra.Cluster.start_link(nodes: ["example.com:9042"], name: "something something")
      end
    end

    test "raises with an option that is not valid for the cluster and for the conn" do
      assert_raise NimbleOptions.ValidationError, ~r"unknown options \[:port\]", fn ->
        Xandra.Cluster.start_link(port: 9042)
      end
    end

    test "with the :inet6 transport option" do
      assert {:ok, _conn} = Xandra.Cluster.start_link(nodes: ["::1"], transport_options: [:inet6])

      assert {:ok, _conn} =
               Xandra.Cluster.start_link(nodes: ["::1:9042"], transport_options: [:inet6])

      assert {:ok, _conn} =
               Xandra.Cluster.start_link(nodes: ["[::1]"], transport_options: [:inet6])

      assert {:ok, _conn} =
               Xandra.Cluster.start_link(nodes: ["[::1]:9042"], transport_options: [:inet6])
    end
  end

  describe "start_link/1" do
    @tag telemetry_events: [[:xandra, :cluster, :control_connection, :failed_to_connect]]
    test "doesn't fail to start if the control connection fails to connect",
         %{base_options: opts, telemetry_ref: telemetry_ref} do
      opts = Keyword.merge(opts, nodes: ["127.0.0.1:8092"], sync_connect: false)
      pid = start_link_supervised!({Cluster, opts})

      assert_telemetry [:control_connection, :failed_to_connect], %{
        cluster_pid: ^pid,
        reason: :econnrefused
      }

      :sys.get_state(pid)
    end

    @tag telemetry_events: [[:xandra, :cluster, :control_connection, :failed_to_connect]]
    test "tries all the nodes for the control connection (asking the load-balancing policy first)",
         %{base_options: opts, telemetry_ref: telemetry_ref} do
      LBPMock
      |> expect(:init, fn _pid -> nil end)
      |> expect(:query_plan, fn nil -> {[%Host{address: "127.0.0.1", port: 8091}], nil} end)

      opts =
        Keyword.merge(opts,
          nodes: ["127.0.0.1:8092", "127.0.0.1:8093", "127.0.0.1:8094"],
          load_balancing: {LBPMock, self()},
          sync_connect: false
        )

      pid = start_link_supervised!({Cluster, opts})

      # First, the host returned by the LBP.
      assert_receive {[:xandra, :cluster, :control_connection, :failed_to_connect],
                      ^telemetry_ref, %{},
                      %{
                        reason: :econnrefused,
                        cluster_pid: ^pid,
                        host: %Host{address: {127, 0, 0, 1}, port: 8091}
                      }}

      # Then, the contact nodes.
      for port <- [8092, 8093, 8094] do
        assert_receive {[:xandra, :cluster, :control_connection, :failed_to_connect],
                        ^telemetry_ref, %{},
                        %{
                          reason: :econnrefused,
                          cluster_pid: ^pid,
                          host: %Host{address: {127, 0, 0, 1}, port: ^port}
                        }}
      end
    end

    @tag telemetry_events: [[:xandra, :connected]]
    test "waits for nodes to be up with :sync_connect",
         %{base_options: opts, telemetry_ref: telemetry_ref} do
      opts = Keyword.merge(opts, sync_connect: 1000)
      _pid = start_link_supervised!({Cluster, opts})

      # Assert that we already received events.
      assert_received {[:xandra, :connected], ^telemetry_ref, %{}, %{}}
    end

    test "times out correctly with :sync_connect", %{base_options: opts} do
      opts = Keyword.merge(opts, sync_connect: 0)
      assert {:error, :sync_connect_timeout} = Cluster.start_link(opts)
    end

    @tag :capture_log
    test "returns a normal error if the cluster crashes before :sync_connect",
         %{base_options: opts} do
      Process.flag(:trap_exit, true)
      opts = Keyword.merge(opts, sync_connect: 1000, nodes: ["127.0.0.1:8092"])
      :erlang.trace(:new_processes, _how = true, [:procs])

      %Task{pid: task_pid} =
        task =
        Task.async(fn ->
          assert {:error, 1} = Cluster.start_link(opts)
        end)

      assert_receive {:trace, ^task_pid, :spawn, cluster_pid, {_m, _f, _a}}
      Process.exit(cluster_pid, :kill)
      assert {:killed, _} = catch_exit(Task.await(task))
    end

    for name <- [:my_cluster_pool, {:global, :global_cluster_pool}] do
      test "supports GenServer name registration with :name set to #{inspect(name)}",
           %{base_options: opts} do
        name = unquote(name)
        opts = Keyword.merge(opts, name: name)
        pid = start_link_supervised!({Cluster, opts})
        assert GenServer.whereis(name) == pid
        stop_supervised!(name)
      end
    end

    test "supports GenServer name registration with :name set to {:via, ...}",
         %{base_options: opts, test: test_name} do
      registry_name = :"#{test_name}_registry"
      start_link_supervised!({Registry, keys: :unique, name: registry_name})

      name = {:via, Registry, {registry_name, :my_cluster_pool}}
      opts = Keyword.merge(opts, name: name)

      pid = start_link_supervised!({Cluster, opts})
      assert GenServer.whereis(name) == pid
      stop_supervised!(name)
    end

    @tag telemetry_events: [[:xandra, :connected]]
    test "successfully connect even with an invalid transport option",
         %{base_options: opts, telemetry_ref: telemetry_ref} do
      opts = Keyword.merge(opts, transport_options: [active: true])
      _pid = start_link_supervised!({Cluster, opts})

      # Assert that we already received events.
      assert_received {[:xandra, :connected], ^telemetry_ref, %{}, %{}}
    end
  end

  describe "starting up" do
    @tag telemetry_events: [
           [:xandra, :cluster, :control_connection, :connected],
           [:xandra, :cluster, :pool, :started],
           [:xandra, :cluster, :change_event],
           [:xandra, :connected]
         ]
    test "establishes a control connection and a pool",
         %{base_options: opts, telemetry_ref: telemetry_ref} do
      pid = start_link_supervised!({Cluster, opts})

      assert_telemetry [:control_connection, :connected], %{
        cluster_pid: ^pid,
        host: %Host{address: {127, 0, 0, 1}, port: @port}
      }

      assert_telemetry [:pool, :started], %{
        cluster_pid: ^pid,
        host: %Host{address: {127, 0, 0, 1}, port: @port}
      }

      assert_telemetry [:change_event], %{
        cluster_pid: ^pid,
        event_type: :host_added,
        host: %Host{address: {127, 0, 0, 1}, port: @port}
      }

      assert_receive {[:xandra, :connected], ^telemetry_ref, %{},
                      %{address: "127.0.0.1", port: @port}}

      cluster_state = get_state(pid)
      assert map_size(cluster_state.peers) == 1

      assert %{status: status, pool_pid: pool_pid, host: host} =
               cluster_state.peers[{{127, 0, 0, 1}, @port}]

      # Let's avoid race conditions...
      assert status in [:up, :connected]
      assert is_pid(pool_pid)
      assert %Host{address: {127, 0, 0, 1}, port: @port, data_center: "datacenter1"} = host
    end

    @tag :capture_log
    @tag telemetry_events: [
           [:xandra, :cluster, :control_connection, :connected],
           [:xandra, :cluster, :pool, :started],
           [:xandra, :cluster, :pool, :stopped],
           [:xandra, :cluster, :change_event],
           [:xandra, :connected],
           [:xandra, :failed_to_connect]
         ]
    test "starts pools in the order reported by the LBP", %{
      base_options: opts,
      telemetry_ref: telemetry_ref,
      test_ref: test_ref
    } do
      opts =
        Keyword.merge(opts,
          target_pools: 2,
          xandra_module: PoolMock,
          sync_connect: false,
          load_balancing:
            {Xandra.Cluster.LoadBalancingPolicy.DCAwareRoundRobin, local_data_center: "local_dc"}
        )

      pid = start_link_supervised!({Cluster, opts})

      assert_receive {^test_ref, PoolMock, :init_called,
                      %{pool_size: 1, connection_options: %{cluster_pid: ^pid} = conn_opts}}

      assert conn_opts[:nodes] == ["127.0.0.1:#{@port}"]

      remote_host = %Host{address: {198, 10, 0, 1}, port: @port, data_center: "remote_dc"}
      local_host1 = %Host{address: {198, 0, 0, 1}, port: @port, data_center: "local_dc"}
      local_host2 = %Host{address: {198, 0, 0, 2}, port: @port, data_center: "local_dc"}

      # Send the remote host first in the list.
      send(pid, {:discovered_hosts, [remote_host, local_host1, local_host2]})

      assert_telemetry [:pool, :started], %{
        cluster_pid: ^pid,
        host: %Host{address: {198, 0, 0, 1}, port: @port, data_center: "local_dc"}
      }

      assert_telemetry [:pool, :started], %{
        cluster_pid: ^pid,
        host: %Host{address: {198, 0, 0, 2}, port: @port, data_center: "local_dc"}
      }

      node1_address = "198.0.0.1:#{@port}"
      node2_address = "198.0.0.2:#{@port}"

      assert_receive {^test_ref, PoolMock, :init_called,
                      %{
                        pool_size: 1,
                        connection_options: %{nodes: [^node1_address], cluster_pid: ^pid}
                      }}

      assert_receive {^test_ref, PoolMock, :init_called,
                      %{
                        pool_size: 1,
                        connection_options: %{nodes: [^node2_address], cluster_pid: ^pid}
                      }}

      refute_receive {[:xandra, :cluster, :pool, :started], ^telemetry_ref, %{},
                      %{
                        cluster_pid: ^pid,
                        host: %Host{
                          address: {198, 10, 0, 1},
                          port: @port,
                          data_center: "remote_dc"
                        }
                      }}

      cluster_state = get_state(pid)

      assert %{pool_pid: pid, status: :up} = cluster_state.peers[{{198, 0, 0, 1}, @port}]
      assert is_pid(pid)
      assert %{pool_pid: pid, status: :up} = cluster_state.peers[{{198, 0, 0, 2}, @port}]
      assert is_pid(pid)
      assert %{pool_pid: nil, status: :up} = cluster_state.peers[{{198, 10, 0, 1}, @port}]
    end

    @tag :capture_log
    @tag telemetry_events: [
           [:xandra, :cluster, :control_connection, :connected],
           [:xandra, :cluster, :pool, :started],
           [:xandra, :cluster, :pool, :stopped],
           [:xandra, :cluster, :change_event],
           [:xandra, :connected],
           [:xandra, :failed_to_connect]
         ]
    test "starts a pool to a node that is reported in the cluster, but is not up",
         %{base_options: opts, telemetry_ref: telemetry_ref} do
      pid = start_link_supervised!({Cluster, opts})

      assert_telemetry [:change_event], %{
        event_type: :host_added,
        cluster_pid: ^pid,
        host: %Host{address: {127, 0, 0, 1}, port: @port} = existing_host
      }

      bad_host = %Host{address: {127, 0, 0, 1}, port: 8092}
      send(pid, {:discovered_hosts, [existing_host, bad_host]})

      assert_telemetry [:change_event], %{
        event_type: :host_added,
        cluster_pid: ^pid,
        host: ^bad_host
      }

      assert_receive {[:xandra, :failed_to_connect], ^telemetry_ref, %{},
                      %{address: "127.0.0.1", port: 8092}}

      assert_telemetry [:pool, :started], %{
        cluster_pid: ^pid,
        host: %Host{address: {127, 0, 0, 1}, port: 8092}
      }

      assert_telemetry [:pool, :stopped], %{
        cluster_pid: ^pid,
        host: %Host{address: {127, 0, 0, 1}, port: 8092}
      }

      cluster_state = get_state(pid)
      assert %{status: :down, host: host} = cluster_state.peers[{{127, 0, 0, 1}, 8092}]
      assert get_load_balancing_state(get_state(pid), host) == :down
    end

    @tag telemetry_events: [
           [:xandra, :connected]
         ]
    test "starts multiple connections to each node through the :pool_size option",
         %{base_options: opts, telemetry_ref: telemetry_ref} do
      opts = Keyword.merge(opts, pool_size: 3)
      pid = start_link_supervised!({Cluster, opts})

      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: pid1}}
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: pid2}}
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: pid3}}

      # Assert that the connections are different.
      assert Enum.uniq([pid1, pid2, pid3]) == [pid1, pid2, pid3]

      # Get the state to make sure the process is alive.
      get_state(pid)
    end
  end

  describe "child_spec/1" do
    test "is provided" do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1.example.com", "node2.example.com"]
      ]

      assert {:ok, _cluster} = start_supervised({Xandra.Cluster, opts})
    end
  end

  describe "connected_hosts/1" do
    test "returns a list of connected %Host{} structs" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = start_link_supervised!({Xandra.Cluster, opts})

      assert [%Host{} = host] = Xandra.Cluster.connected_hosts(cluster)

      assert host.address == {127, 0, 0, 1}
      assert host.port == @port
      assert host.data_center == "datacenter1"
      assert host.rack == "rack1"
      assert is_binary(host.release_version)
      assert is_binary(host.host_id)
      assert is_binary(host.schema_version)
      assert is_struct(host.tokens, MapSet)
    end
  end

  describe "execute/3" do
    test "executes a query" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = start_link_supervised!({Xandra.Cluster, opts})

      assert {:ok, %Xandra.Page{}} =
               Xandra.Cluster.execute(cluster, "SELECT * FROM system.local")

      assert {:ok, %Xandra.Page{}} =
               Xandra.Cluster.execute(
                 cluster,
                 "SELECT * FROM system.local WHERE key = ?",
                 _params = [{"varchar", "local"}],
                 _options = []
               )
    end

    test "executes batch queries", %{base_options: opts} do
      cluster = start_link_supervised!({Cluster, opts})

      Xandra.Cluster.run(cluster, fn conn ->
        :ok = XandraTest.IntegrationCase.setup_keyspace(conn, "cluster_batch_test")
      end)

      Xandra.Cluster.execute!(
        cluster,
        "CREATE TABLE cluster_batch_test.cluster_batches (id int PRIMARY KEY)"
      )

      batch =
        Xandra.Batch.new()
        |> Xandra.Batch.add("INSERT INTO cluster_batch_test.cluster_batches(id) VALUES (?)", [
          {"int", 1}
        ])

      assert {:ok, %Xandra.Void{}} = Xandra.Cluster.execute(cluster, batch)
    end

    test "returns an error if the cluster is not connected to any node", %{base_options: opts} do
      opts =
        Keyword.merge(opts,
          nodes: ["127.0.0.1:8092"],
          sync_connect: false,
          queue_checkouts_before_connecting: [max_size: 0]
        )

      cluster = start_link_supervised!({Cluster, opts})

      assert {:error, %ConnectionError{reason: {:cluster, :not_connected}}} =
               Xandra.Cluster.execute(cluster, "SELECT * FROM system.local")
    end
  end

  describe "execute!/3,4" do
    test "returns the result directly" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = start_link_supervised!({Xandra.Cluster, opts})

      assert %Xandra.Page{} = Xandra.Cluster.execute!(cluster, "SELECT * FROM system.local")

      assert %Xandra.Page{} =
               Xandra.Cluster.execute!(
                 cluster,
                 "SELECT * FROM system.local WHERE key = ?",
                 _params = [{"varchar", "local"}],
                 _options = []
               )
    end

    test "raises errors" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = start_link_supervised!({Xandra.Cluster, opts})

      assert_raise Xandra.Error, "Keyspace 'nonexisting_keyspace' does not exist", fn ->
        Xandra.Cluster.execute!(cluster, "USE nonexisting_keyspace")
      end

      assert_raise Xandra.Error, "Keyspace 'nonexisting_keyspace' does not exist", fn ->
        Xandra.Cluster.execute!(cluster, "USE nonexisting_keyspace", _params = [], _options = [])
      end
    end
  end

  describe "prepare/3" do
    test "prepares a query" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = start_link_supervised!({Xandra.Cluster, opts})

      assert {:ok, %Xandra.Prepared{} = prepared} =
               Xandra.Cluster.prepare(cluster, "SELECT * FROM system.local")

      assert {:ok, _page} = Xandra.Cluster.execute(cluster, prepared)
    end

    test "returns an error if the cluster is not connected to any node", %{base_options: opts} do
      opts =
        Keyword.merge(opts,
          nodes: ["127.0.0.1:8092"],
          sync_connect: false,
          queue_checkouts_before_connecting: [max_size: 0]
        )

      cluster = start_link_supervised!({Cluster, opts})

      assert {:error, %ConnectionError{reason: {:cluster, :not_connected}}} =
               Xandra.Cluster.prepare(cluster, "SELECT * FROM system.local")
    end
  end

  describe "prepare!/3" do
    test "returns the result directly for the bang! version" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = start_link_supervised!({Xandra.Cluster, opts})

      assert %Xandra.Prepared{} =
               prepared = Xandra.Cluster.prepare!(cluster, "SELECT * FROM system.local")

      assert {:ok, _page} = Xandra.Cluster.execute(cluster, prepared)
    end

    test "raises errors" do
      opts = [
        nodes: ["127.0.0.1:#{@port}"],
        sync_connect: 1000
      ]

      opts =
        if @protocol_version do
          Keyword.put(opts, :protocol_version, @protocol_version)
        else
          opts
        end

      cluster = start_link_supervised!({Xandra.Cluster, opts})

      assert_raise Xandra.Error, ~r/no viable alternative at input/, fn ->
        Xandra.Cluster.prepare!(cluster, "SELECT bad syntax")
      end
    end
  end

  describe "stream_pages!/4" do
    test "streams pages", %{base_options: opts} do
      cluster = start_link_supervised!({Cluster, opts})
      stream = Xandra.Cluster.stream_pages!(cluster, "SELECT * FROM system.local", _params = [])
      assert [%{}] = Enum.to_list(stream)
    end
  end

  describe "stop/1" do
    test "stops the cluster", %{test_ref: test_ref} do
      opts = [
        xandra_module: PoolMock,
        control_connection_module: ControlConnectionMock,
        nodes: ["node1"]
      ]

      cluster = start_link_supervised!({Xandra.Cluster, opts})
      assert_control_connection_started(test_ref)

      assert Xandra.Cluster.stop(cluster) == :ok
      refute Process.alive?(cluster)
    end
  end

  describe "Pool.checkout/1" do
    test "returns all connected pools", %{base_options: opts} do
      pid = start_supervised!({Cluster, opts})

      assert {:ok, pids_with_hosts} = Pool.checkout(pid)
      assert is_list(pids_with_hosts)

      assert Enum.all?(pids_with_hosts, fn {conn, %Host{}} -> is_pid(conn) end)

      expected_set_of_connected_hosts = MapSet.new([{{127, 0, 0, 1}, @port}])

      existing_set_of_connected_hosts =
        MapSet.new(pids_with_hosts, fn {_, host} -> Host.to_peername(host) end)

      assert existing_set_of_connected_hosts == expected_set_of_connected_hosts
    end

    test "returns {:error, :empty} when there are no pools", %{base_options: opts} do
      opts =
        Keyword.merge(opts,
          nodes: ["127.0.0.1:8092"],
          sync_connect: false,
          queue_checkouts_before_connecting: [timeout: 0, max_size: 0]
        )

      pid = start_supervised!({Cluster, opts})
      assert {:error, :empty} = Pool.checkout(pid)
    end
  end

  describe "handling change events" do
    @tag telemetry_events: [
           [:xandra, :cluster, :pool, :started],
           [:xandra, :cluster, :pool, :stopped],
           [:xandra, :cluster, :pool, :restarted]
         ]
    test ":host_down followed by :host_up", %{base_options: opts, telemetry_ref: telemetry_ref} do
      host = %Host{address: {127, 0, 0, 1}, port: @port}
      pid = start_supervised!({Cluster, opts})

      assert_receive {[:xandra, :cluster, :pool, :started], ^telemetry_ref, %{}, %{}}

      # Send the DOWN event.
      send(pid, {:host_down, host.address, host.port})

      assert_receive {[:xandra, :cluster, :pool, :stopped], ^telemetry_ref, %{}, meta}
      assert %Host{address: {127, 0, 0, 1}, port: @port} = meta.host

      assert %{status: :down, pool_pid: nil, host: %Host{address: {127, 0, 0, 1}, port: @port}} =
               get_state(pid).peers[Host.to_peername(host)]

      assert get_load_balancing_state(get_state(pid), host) == :down

      # Send the UP event.
      send(pid, {:host_up, host.address, host.port})

      assert_receive {[:xandra, :cluster, :pool, :restarted], ^telemetry_ref, %{}, meta}
      assert %Host{address: {127, 0, 0, 1}, port: @port} = meta.host

      assert %{
               status: :up,
               pool_pid: pool_pid,
               host: %Host{address: {127, 0, 0, 1}, port: @port}
             } = get_state(pid).peers[Host.to_peername(host)]

      assert get_load_balancing_state(get_state(pid), host) == :up

      assert is_pid(pool_pid)
    end

    @tag telemetry_events: [
           [:xandra, :cluster, :pool, :started],
           [:xandra, :cluster, :pool, :stopped],
           [:xandra, :cluster, :pool, :restarted],
           [:xandra, :cluster, :change_event]
         ]
    test "multiple :discovered_hosts where hosts are removed",
         %{base_options: opts, telemetry_ref: telemetry_ref} do
      good_host = %Host{address: {127, 0, 0, 1}, port: @port}
      bad_host = %Host{address: {127, 0, 0, 1}, port: 8092}
      pid = start_supervised!({Cluster, opts})

      assert_receive {[:xandra, :cluster, :pool, :started], ^telemetry_ref, %{}, %{}}

      assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                      %{event_type: :host_added}}

      send(pid, {:discovered_hosts, [good_host, bad_host]})

      assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                      %{
                        event_type: :host_added,
                        host: %Host{address: {127, 0, 0, 1}, port: 8092}
                      }}

      assert_receive {[:xandra, :cluster, :pool, :started], ^telemetry_ref, %{},
                      %{host: %Host{address: {127, 0, 0, 1}, port: 8092}}}

      # Now remove the bad host.
      send(pid, {:discovered_hosts, [good_host]})

      assert_receive {[:xandra, :cluster, :change_event], ^telemetry_ref, %{},
                      %{event_type: :host_removed, host: ^bad_host}}

      assert_receive {[:xandra, :cluster, :pool, :stopped], ^telemetry_ref, %{},
                      %{host: %Host{address: {127, 0, 0, 1}, port: 8092}}}

      assert get_state(pid).pool_supervisor
             |> Supervisor.which_children()
             |> List.keyfind({{127, 0, 0, 1}, 8092}, 0) == nil
    end

    @tag telemetry_events: [[:xandra, :cluster, :pool, :started]]
    test ":host_up is idempotent", %{base_options: opts, telemetry_ref: telemetry_ref} do
      pid = start_supervised!({Cluster, opts})

      assert_telemetry [:pool, :started], %{cluster_pid: ^pid}

      send(pid, {:host_up, {127, 0, 0, 1}, @port})
      get_state(pid)
    end
  end

  describe "resiliency" do
    @tag :capture_log
    test "if a single connection crashes, the pool process stays up and cleans up",
         %{base_options: opts} do
      opts = Keyword.merge(opts, sync_connect: 1000)
      cluster = start_link_supervised!({Cluster, opts})

      assert %{pool_pid: pool_pid, status: :connected, host: host} =
               get_state(cluster).peers[{{127, 0, 0, 1}, @port}]

      assert get_load_balancing_state(get_state(cluster), host) == :connected

      assert {:ok, [{conn_pid, %Host{}}]} = Pool.checkout(cluster)
      ref = Process.monitor(conn_pid)

      Process.exit(conn_pid, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}

      assert %{pool_pid: ^pool_pid, status: :connected, host: host} =
               get_state(cluster).peers[{{127, 0, 0, 1}, @port}]

      assert get_load_balancing_state(get_state(cluster), host) == :connected
    end

    @tag :capture_log
    test "if a connection pool crashes, the pool process stays up and cleans up",
         %{base_options: opts} do
      opts = Keyword.merge(opts, sync_connect: 1000)
      cluster = start_link_supervised!({Cluster, opts})

      assert %{pool_pid: pool_pid} = get_state(cluster).peers[{{127, 0, 0, 1}, @port}]
      ref = Process.monitor(pool_pid)

      Process.exit(pool_pid, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}

      assert %{status: :up, pool_pid: nil, pool_ref: nil, host: host} =
               get_state(cluster).peers[{{127, 0, 0, 1}, @port}]

      assert get_load_balancing_state(get_state(cluster), host) == :up
    end

    @tag :capture_log
    test "if the connection pool supervisor crashes, the pool crashes as well",
         %{base_options: opts} do
      # Don't *link* the cluster to the test process, to avoid having to trap exits.
      opts = Keyword.merge(opts, sync_connect: 1000)
      cluster = start_supervised!({Cluster, opts})
      cluster_ref = Process.monitor(cluster)

      pool_sup = get_state(cluster).pool_supervisor
      ref = Process.monitor(pool_sup)
      Process.exit(pool_sup, :kill)
      assert_receive {:DOWN, ^ref, _, _, _}

      assert_receive {:DOWN, ^cluster_ref, _, _, :killed}
    end

    @tag :capture_log
    test "if the control connection crashes, the pool crashes as well",
         %{base_options: opts} do
      # Don't *link* the cluster to the test process, to avoid having to trap exits.
      opts = Keyword.merge(opts, sync_connect: 1000)
      cluster = start_supervised!({Cluster, opts})
      cluster_ref = Process.monitor(cluster)

      control_conn = get_state(cluster).control_connection
      control_conn_ref = Process.monitor(control_conn)
      Process.exit(control_conn, :kill)
      assert_receive {:DOWN, ^control_conn_ref, _, _, _}

      assert_receive {:DOWN, ^cluster_ref, _, _, :killed}
    end

    @tag :capture_log
    @tag telemetry_events: [
           [:xandra, :cluster, :control_connection, :connected],
           [:xandra, :cluster, :control_connection, :disconnected]
         ]
    test "if the control connection shuts down with :closed, the pool is fine and starts a new one",
         %{base_options: opts, telemetry_ref: telemetry_ref} do
      opts = Keyword.merge(opts, sync_connect: 1000)
      cluster = start_link_supervised!({Cluster, opts})

      assert_telemetry [:control_connection, :connected], _meta

      control_conn = get_state(cluster).control_connection
      control_conn_ref = Process.monitor(control_conn)

      :ok = :gen_tcp.shutdown(:sys.get_state(control_conn).transport.socket, :read_write)
      assert_telemetry [:control_connection, :disconnected], _meta
      assert_receive {:DOWN, ^control_conn_ref, _, _, _}

      # Make sure we reconnect to the control connection.
      assert_telemetry [:control_connection, :connected], _meta
    end

    @tag :toxiproxy
    @tag telemetry_events: [
           [:xandra, :cluster, :pool, :stopped]
         ]
    test "when a single connection goes down", %{base_options: opts, telemetry_ref: telemetry_ref} do
      opts = Keyword.merge(opts, sync_connect: 1000, nodes: ["127.0.0.1:19052"])
      pid = start_link_supervised!({Cluster, opts})

      ToxiproxyEx.get!(:xandra_test_cassandra)
      |> ToxiproxyEx.down!(fn ->
        assert_telemetry [:pool, :stopped], %{cluster_pid: ^pid}
      end)
    end

    @tag :toxiproxy
    @tag telemetry_events: [
           [:xandra, :cluster, :control_connection, :failed_to_connect],
           [:xandra, :cluster, :control_connection, :connected]
         ]
    test "reconnects to the control connection if it goes down",
         %{base_options: opts, telemetry_ref: telemetry_ref} do
      test_pid = self()
      test_ref = make_ref()
      opts = Keyword.merge(opts, sync_connect: false, nodes: ["127.0.0.1:19052"])

      ToxiproxyEx.get!(:xandra_test_cassandra)
      |> ToxiproxyEx.down!(fn ->
        pid = start_link_supervised!({Cluster, opts})
        send(test_pid, {test_ref, :cluster_pid, pid})
        assert_telemetry [:control_connection, :failed_to_connect], %{cluster_pid: ^pid} = meta
        assert %ConnectionError{reason: :closed} = meta.reason
      end)

      assert_receive {^test_ref, :cluster_pid, pid}

      assert_telemetry [:control_connection, :connected], %{cluster_pid: ^pid}
    end
  end

  defp get_state(cluster) do
    assert {_state, data} = :sys.get_state(cluster)
    data
  end

  defp get_load_balancing_state(%Pool{load_balancing_state: lbs}, %Host{} = host) do
    {_host, state} =
      Enum.find(lbs, fn {lbs_host, _state} ->
        Host.to_peername(lbs_host) == Host.to_peername(host)
      end)

    state
  end

  defp assert_control_connection_started(test_ref) do
    assert_receive {^test_ref, ControlConnectionMock, :init_called, _start_args}
  end
end
