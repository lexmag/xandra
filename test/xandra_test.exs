defmodule XandraTest do
  use XandraTest.IntegrationCase, async: true

  import XandraTest.IntegrationCase, only: [default_start_options: 0]

  alias Xandra.ConnectionError
  alias Xandra.Transport

  doctest Xandra

  describe "start_link/1" do
    @describetag start_conn: false

    test "validates the :nodes option" do
      assert_raise NimbleOptions.ValidationError, ~r{invalid node: "foo:bar"}, fn ->
        Xandra.start_link(nodes: ["foo:bar"])
      end

      assert_raise NimbleOptions.ValidationError, ~r{invalid list in :nodes option}, fn ->
        Xandra.start_link(nodes: [:not_even_a_string])
      end

      # Invalid port
      assert_raise NimbleOptions.ValidationError, ~r{invalid port}, fn ->
        Xandra.start_link(nodes: ["127.0.0.1:65590"])
      end

      assert_raise ArgumentError, ~r{cannot use multiple nodes in the :nodes option}, fn ->
        Xandra.start_link(nodes: ["foo", "bar"])
      end

      # Empty list of nodes.
      assert_raise ArgumentError, ~r{the :nodes option can't be an empty list}, fn ->
        Xandra.start_link(nodes: [])
      end

      # IPv6 Connection
      assert {:ok, _conn} = Xandra.start_link(nodes: ["127.0.0.1"], transport_options: [:inet6])
    end

    test "validates the :authentication option" do
      assert_raise NimbleOptions.ValidationError, ~r{invalid value for :authentication}, fn ->
        Xandra.start_link(authentication: :nope)
      end
    end

    test "validates the :compressor option" do
      message =
        "invalid value for :compressor option: compressor module :nonexisting_module is not loaded"

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.start_link(compressor: :nonexisting_module)
      end

      message = ~r/expected compressor module to be a module/

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.start_link(compressor: "not even an atom")
      end
    end

    test "validates the :max_concurrent_requests_per_connection option" do
      message = ~r{invalid value for :max_concurrent_requests_per_connection option}

      assert_raise NimbleOptions.ValidationError, message, fn ->
        Xandra.start_link(max_concurrent_requests_per_connection: 0)
      end
    end

    test "returns an error if the connection is not established" do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [[:xandra, :failed_to_connect]])

      # Use a random port on localhost, because when you use a non-existent domain but you're
      # without an Internet connection then DSN causes issues.
      options = Keyword.merge(default_start_options(), nodes: ["localhost:65000"])

      conn = start_supervised!({Xandra, options})
      assert_receive {[:xandra, :failed_to_connect], ^telemetry_ref, %{}, %{connection: ^conn}}

      assert {:error, %ConnectionError{action: "check out connection", reason: :not_connected}} =
               Xandra.execute(conn, "USE some_keyspace")
    end

    test "supports the :name option as an atom", %{start_options: start_options} do
      assert {:ok, conn} = start_supervised({Xandra, [name: :my_test_conn] ++ start_options})
      assert Process.whereis(:my_test_conn) == conn
    end

    test "supports the :name option as {:global, name}", %{start_options: start_options} do
      name = {:global, :my_global_test_conn}
      assert {:ok, conn} = start_supervised({Xandra, [name: name] ++ start_options})
      assert GenServer.whereis(name) == conn
    end

    test "supports the :name option as {:via, mod, term}",
         %{start_options: start_options, test: test_name} do
      registry_name = :"Registry_#{test_name}"
      start_supervised!({Registry, keys: :unique, name: registry_name})

      name = {:via, Registry, {registry_name, :my_via_test_conn}}
      assert {:ok, conn} = start_supervised({Xandra, [name: name] ++ start_options})
      assert GenServer.whereis(name) == conn
    end

    test "supports the :keyspace option", %{keyspace: keyspace, start_options: start_options} do
      assert {:ok, conn} = start_supervised({Xandra, [keyspace: keyspace] ++ start_options})

      assert {:connected, state} = :sys.get_state(conn)
      assert state.current_keyspace == keyspace
    end

    # Regression for https://github.com/lexhide/xandra/issues/266
    test "supports the :default_consistency option",
         %{keyspace: keyspace, start_options: start_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :execute_query, :stop]
        ])

      assert {:ok, test_conn} =
               start_supervised({Xandra, [default_consistency: :three] ++ start_options})

      Xandra.execute!(test_conn, "USE #{keyspace}", _params = [],
        telemetry_metadata: %{ref: telemetry_ref}
      )

      assert_receive {[:xandra, :execute_query, :stop], ^telemetry_ref, %{},
                      %{
                        connection: ^test_conn,
                        extra_metadata: %{ref: ^telemetry_ref},
                        query: query
                      }}

      assert %Xandra.Simple{} = query
      assert query.default_consistency == :three
    end

    test "supports the :configure option, as an anonymous function",
         %{start_options: start_options} do
      test_pid = self()
      ref = make_ref()

      configure_fun = fn options ->
        send(test_pid, {ref, options})
        Keyword.replace!(options, :nodes, start_options[:nodes])
      end

      modified_start_options =
        Keyword.merge(start_options, configure: configure_fun, nodes: ["localhost:9999"])

      assert {:ok, _test_conn} = start_supervised({Xandra, modified_start_options})

      assert_receive {^ref, configure_start_options}
      assert configure_start_options[:node] == {"localhost", 9999}
    end

    test "supports the :configure option, as a MFA tuple", %{start_options: start_options} do
      ref = make_ref()

      configure_fun = {__MODULE__, :configure_fun, [start_options, self(), ref]}

      modified_start_options =
        Keyword.merge(start_options, configure: configure_fun, nodes: ["localhost:9999"])

      assert {:ok, _test_conn} = start_supervised({Xandra, modified_start_options})

      assert_receive {^ref, configure_start_options}
      assert configure_start_options[:node] == {"localhost", 9999}
    end

    test "handles connection drops that happen right after connecting" do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :failed_to_connect]
        ])

      assert {:ok, listen_socket} = :gen_tcp.listen(0, active: false, mode: :binary)
      assert {:ok, port} = :inet.port(listen_socket)

      task =
        Task.async(fn ->
          # Accept a socket (so that Xandra's :gen_tcp.connect/3 call succeeds),
          # and then shut the socket down right away.
          assert {:ok, socket} = :gen_tcp.accept(listen_socket, :infinity)
          :ok = :gen_tcp.close(socket)
        end)

      assert {:ok, conn} = start_supervised({Xandra, nodes: ["localhost:#{port}"]})

      assert {:error, %ConnectionError{action: "check out connection", reason: :not_connected}} =
               Xandra.execute(conn, "SELECT * FROM system.local")

      assert :ok = Task.await(task)

      assert_received {[:xandra, :failed_to_connect], ^telemetry_ref, %{}, %{connection: ^conn}}
    end

    test "invalid transport option gets forcibly overwritten" do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [[:xandra, :connected]])

      # Set a transport option `active: true` that normally would cause the
      # connection to fail. This connection should succeed because start_link
      # forcibly overrides and sets some necessary options.
      options = Keyword.merge(default_start_options(), transport_options: [active: true])

      conn = start_supervised!({Xandra, options})
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: ^conn}}
    end
  end

  describe "execute/3,4" do
    test "supports the :timeout option", %{conn: conn} do
      # Do this a few times to make it more reliable and make the timeout pop up.
      for _ <- 1..5 do
        case Xandra.execute(conn, "SELECT * FROM system.local", [], timeout: 0) do
          {:error, %ConnectionError{} = error} -> assert error.reason == :timeout
          {:ok, _} -> :yeah_alright_its_flaky
          other -> flunk("Unexpected return error: #{inspect(other)}")
        end
      end
    end

    # It's an annoyance to set up support for UDFs in Scylla in CI.
    @tag :cassandra_specific
    @tag start_conn: false
    test "returns an error if the max number of concurrent requests is reached",
         %{start_options: start_options, keyspace: keyspace} do
      modified_start_options =
        Keyword.merge(start_options,
          max_concurrent_requests_per_connection: 1,
          keyspace: keyspace
        )

      assert {:ok, conn} = start_supervised({Xandra, modified_start_options})

      # Not ideal, but here it is.
      # https://stackoverflow.com/questions/55497473/does-cassandra-have-a-sleep-cql-query
      Xandra.execute!(conn, """
      CREATE OR REPLACE FUNCTION #{keyspace}.sleep (time int)
      CALLED ON NULL INPUT RETURNS int LANGUAGE java AS
      '
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() < start + time);
      return time;
      ';
      """)

      results =
        1..2
        |> Task.async_stream(fn index ->
          {index, Xandra.execute(conn, "SELECT #{keyspace}.sleep(200) FROM system.local")}
        end)
        |> Enum.map(fn {:ok, result} -> result end)

      # The first call succeeds, but the second call fails because it goes over
      # the max concurrent conns.
      assert [
               {1, {:ok, %Xandra.Page{}}},
               {2, {:error, %ConnectionError{reason: :too_many_concurrent_requests} = error}}
             ] = Enum.sort_by(results, &elem(&1, 0))

      assert Exception.message(error) =~ "this connection has too many requests in flight"
    end

    # It's an annoyance to set up support for UDFs in Scylla in CI.
    @tag :cassandra_specific
    test "returns an error for requests that time out on the caller but only later on the server",
         %{conn: conn, keyspace: keyspace} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :debug, :received_timed_out_response]
        ])

      Xandra.execute!(conn, """
      CREATE OR REPLACE FUNCTION #{keyspace}.sleep (time int)
      CALLED ON NULL INPUT RETURNS int LANGUAGE java AS
      '
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() < start + time);
      return time;
      ';
      """)

      :erlang.trace(conn, true, [:receive])

      server_timeout = 200

      assert {:error, %ConnectionError{reason: :timeout}} =
               Xandra.execute(
                 conn,
                 "SELECT #{keyspace}.sleep(#{server_timeout}) FROM system.local",
                 [],
                 timeout: div(server_timeout, 5)
               )

      assert_receive {:trace, ^conn, :receive,
                      {:"$gen_cast", {:request_timed_out_at_caller, stream_id}}}

      assert {:connected, data} = :sys.get_state(conn)
      assert map_size(data.timed_out_ids) == 1
      assert %{^stream_id => _ts} = data.timed_out_ids
      assert data.in_flight_requests == %{}

      # Now trigger a flush.
      assert :ok = Xandra.Connection.trigger_flush_timed_out_stream_ids(conn)
      assert {:connected, data_after_flush} = :sys.get_state(conn)
      assert data_after_flush.timed_out_ids == data.timed_out_ids

      # Now actually wait for the original request to finish.
      assert_receive {[:xandra, :debug, :received_timed_out_response], ^telemetry_ref, %{},
                      %{connection: ^conn, stream_id: ^stream_id}},
                     1000

      assert {:connected, data} = :sys.get_state(conn)
      assert data.timed_out_ids == %{}
    end
  end

  describe "prepare/3" do
    test "works as expected", %{conn: conn} do
      assert {:ok, prepared} = Xandra.prepare(conn, "SELECT * FROM system.local")
      assert {:ok, %Xandra.Page{}} = Xandra.execute(conn, prepared, [])
    end
  end

  describe "failure handling" do
    test "reconnects if the connection drops", %{start_options: start_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :connected],
          [:xandra, :disconnected]
        ])

      conn = start_supervised!({Xandra, start_options ++ [backoff_min: 0]})
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: ^conn}}

      assert {:ok, %Transport{} = transport} = Xandra.Connection.get_transport(conn)
      assert :ok = transport.module.shutdown(transport.socket, :read_write)

      assert_receive {[:xandra, :disconnected], ^telemetry_ref, %{}, %{connection: ^conn}}
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: ^conn}}
    end

    test "reconnects if the connection errors out", %{start_options: start_options} do
      telemetry_ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:xandra, :connected],
          [:xandra, :disconnected]
        ])

      conn = start_supervised!({Xandra, start_options ++ [backoff_min: 0]})
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: ^conn}}

      # Yuck, we have to send the connection a fake TCP error message here.
      assert {:ok, %Transport{} = transport} = Xandra.Connection.get_transport(conn)
      send(conn, {:tcp_error, transport.socket, :econnrefused})

      assert_receive {[:xandra, :disconnected], ^telemetry_ref, %{}, %{connection: ^conn}}
      assert_receive {[:xandra, :connected], ^telemetry_ref, %{}, %{connection: ^conn}}
    end
  end

  # Regression for timeouts on native protocol v5:
  # https://github.com/whatyouhide/xandra/issues/356
  @tag :regression
  test "concurrent requests on a single connection", %{conn: conn} do
    1..5
    |> Task.async_stream(fn _i ->
      Xandra.execute(conn, "SELECT cluster_name FROM system.local", [], timeout: 5000)
    end)
    |> Enum.each(fn {:ok, result} ->
      assert {:ok, %Xandra.Page{}} = result
    end)
  end

  # Regression for encoding errors leaking stream ids and thus bricking connections
  # https://github.com/whatyouhide/xandra/issues/367
  @tag :regression
  @tag start_conn: false
  test "connection handles encoding errors without leaking stream IDs",
       %{keyspace: keyspace, start_options: start_options} do
    # we restrict to one conncurrent stream id per connection to make leaking deterministic
    start_options = Keyword.put(start_options, :max_concurrent_requests_per_connection, 1)
    conn = start_supervised!({Xandra, start_options})

    # setup test table
    statement = "CREATE TABLE #{keyspace}.encoding_error_test (id int PRIMARY KEY, value int)"
    Xandra.execute!(conn, statement)

    # verify initial successful request
    insert_statement = "INSERT INTO #{keyspace}.encoding_error_test (id, value) VALUES (?, ?)"
    assert {:ok, _} = Xandra.execute(conn, insert_statement, [{"int", 1}, {"int", 1}])

    # trigger encoding error with invalid value type
    assert_raise FunctionClauseError, fn ->
      Xandra.execute(conn, insert_statement, [{"int", 2}, {"int", "2"}])
    end

    # verify connection should still work after an encoding error
    assert {:ok, _} = Xandra.execute(conn, insert_statement, [{"int", 3}, {"int", 3}])
  end

  def configure_fun(options, original_start_options, pid, ref) do
    send(pid, {ref, options})
    Keyword.replace!(options, :nodes, original_start_options[:nodes])
  end
end
