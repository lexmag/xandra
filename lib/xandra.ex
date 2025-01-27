defmodule Xandra do
  @moduledoc """
  This module provides the main API to interface with Cassandra.

  This module handles the connection to Cassandra, queries, connection pooling,
  connection backoff, logging, and more.

  > #### `db_connection` {: .warning}
  >
  > Before v0.18.0, this library was built on top of the
  > [`db_connection`](http://hex.pm/packages/db_connection) library. Since v0.18.0,
  > this is not the case anymore. This allowed us to significantly improve the design
  > and architecture of Xandra, ultimately resulting in a client that is more fitted
  > for the Cassandra protocol.

  ## Errors

  Many of the functions in this module (whose names don't end with a `!`)
  return values in the form `{:ok, result}` or `{:error, error}`. While `result`
  varies based on the specific function, `error` is always one of the following:

    * a `Xandra.Error` struct: such structs represent errors returned by
      Cassandra. When such an error is returned, it means that communicating
      with the Cassandra server was successful, but the server returned an
      error. Examples of these errors are syntax errors in queries, non-existent
      tables, and so on. See `Xandra.Error` for more information.

    * a `Xandra.ConnectionError` struct: such structs represent errors in the
      communication with the Cassandra server. For example, if the Cassandra
      server dies while the connection is waiting for a response from the
      server, a `Xandra.ConnectionError` error will be returned. See
      `Xandra.ConnectionError` for more information.

  ## Parameters, encoding, and types

  Xandra supports parameterized queries (queries that specify "parameter" values
  through `?` or `:named_value`):

      SELECT * FROM users WHERE name = ? AND email = ?
      SELECT * FROM users WHERE name = :name AND email = :email

  When a query has positional parameters, parameters can be passed as a list to
  functions like `execute/4`: in this case, a parameter in a given position in
  the list will be used as the `?` in the corresponding position in the
  query. When a query has named parameters, parameters are passed as a map with
  string keys representing each parameter's name and values representing the
  corresponding parameter's value.

  ### Types

  For information about how Elixir types translate to Cassandra types and
  viceversa, see the ["Data types comparison table" page](data-types-comparison-table.html).

  Cassandra supports many types of values, and some types have "shades" that
  cannot be represented by Elixir types. For example, in Cassandra an integer
  could be a "bigint" (a 64 bit integer), an "int" (a 32 bit integer), a
  "smallint" (a 16 bit integer), or others; in Elixir, however, integers are
  just integers (with varying size to be precise), so it is impossible to
  univocally map Elixir integers to a specific Cassandra integer type. For this
  reason, when executing simple parameterized queries (statements) it is
  necessary to explicitly specify the type of each value.

  To specify the type of a value, that value needs to be provided as a
  two-element tuple where the first element is the value's type and the second
  element is the value itself. Types are expressed with the same syntax used in
  CQL: for example, 16-bit integers are represented as `"smallint"`, while maps
  of strings to booleans are represented as `"map<text, boolean>"`.

      # Using a list of parameters:
      statement = "INSERT INTO species (name, properties) VALUES (?, ?)"
      Xandra.execute(conn, statement, [
        {"text", "human"},
        {"map<text, boolean>", %{"legs" => true, "arms" => true, "tail" => false}},
      ])

      # Using a map of parameters:
      statement = "INSERT INTO species (name, properties) VALUES (:name, :properties)"
      Xandra.execute(conn, statement, %{
        "name" => {"text", "human"},
        "properties" => {"map<text, boolean>", %{"legs" => true, "arms" => true, "tail" => false}},
      })

  You only need to specify types for simple queries (statements): when using
  prepared queries, the type information of each parameter of the query is
  encoded in the prepared query itself.

      # Using a map of parameters:
      prepared = Xandra.prepare!(conn, "INSERT INTO species (name, properties) VALUES (:name, :properties)")
      Xandra.execute(conn, prepared, %{
        "name" => "human",
        "properties" => %{"legs" => true, "arms" => true, "tail" => false},
      })

  #### User-defined types

  Xandra supports user-defined types (UDTs). A UDT can be inserted as a map with
  string fields. For example, consider having created the following UDTs:

      CREATE TYPE full_name (first_name text, last_name text)
      CREATE TYPE profile (username text, full_name frozen<full_name>)

  and having the following table:

      CREATE TABLE users (id int PRIMARY KEY, profile frozen<profile>)

  Inserting rows will look something like this:

      prepared_insert = Xandra.prepare!(conn, "INSERT INTO users (id, profile) VALUES (?, ?)")

      profile = %{
        "username" => "bperry",
        "full_name" => %{"first_name" => "Britta", "last_name" => "Perry"},
      }
      Xandra.execute!(conn, prepared_insert, [_id = 1, profile])

  Note that inserting UDTs is only supported on prepared queries.

  When retrieved, UDTs are once again represented as maps with string
  keys. Retrieving the row inserted above would look like this:

      %{"profile" => profile} = conn |> Xandra.execute!("SELECT id, profile FROM users") |> Enum.fetch!(0)
      profile
      #=> %{"username" => "bperry", "full_name" => %{"first_name" => "Britta", "last_name" => "Perry"}}

  ### Values

  Xandra supports two special values: `nil` and `:not_set`. Using `nil` explicitly
  inserts a `null` value into the Cassandra table. This is useful to **delete a value** while
  inserting. Note however that explicitly inserting `null` values into Cassandra creates so
  called *tombstones* which negatively affects performance and resource utilisation and is
  thus usually not recommended.

  The `:not_set` value is a special value that allows to leave the value of a parametrized query
  *unset*, telling Cassandra not to insert anything for the given field. In contrast to explicit
  `null` values, no tombstone is created for this field. This is useful for prepared queries with
  optional fields. The `:not_set` value requires Cassandra native protocol v4, available since
  Cassandra `2.2.x`. You can force the protocol version to v4 with the `:protocol_version`
  option.

  ## Reconnections

  A Xandra connection is able to handle connection
  losses and to automatically reconnect to Cassandra. By default, reconnections
  are retried at exponentially increasing randomized intervals, but backoff can
  be configured through a subset of the options accepted by
  `start_link/2`.

  ## Clustering

  Xandra supports connecting to multiple nodes in a Cassandra cluster and
  executing queries on different nodes based on load balancing strategies. See
  the documentation for `Xandra.Cluster` for more information.

  ## Authentication

  Xandra supports Cassandra authentication. See the documentation for
  `Xandra.Authenticator` for more information.

  ## Retrying failed queries

  Xandra takes a customizable and extensible approach to retrying failed queries
  through "retry strategies" that encapsulate the logic for retrying
  queries. See `Xandra.RetryStrategy` for documentation on retry strategies.

  ## Compression

  Xandra supports compression. To inform the Cassandra server that the
  connections you start should use compression for data transmitted to and from
  the server, you can pass the `:compressor` option to `start_link/1`; this
  option should be a module that implements the `Xandra.Compressor`
  behaviour. After this, all compressed data that Cassandra sends to the
  connection will be decompressed using this behaviour module.

  To compress outgoing data (such as when issuing or preparing queries), the behavior
  of Xandra depends on the native protocol that it negotiated with the Cassandra server.
  For native protocol v4 and earlier, the `:compressor` option should be specified
  explicitly for every request where you want outgoing compressor. When it's specified, the
  given module will be used to compress data. If no `:compressor` option is
  passed, the outgoing data will not be compressed. For native protocol v5 or later,
  Xandra compresses all outgoing data since compression happens at the connection level,
  so it's not necessary to pass the `:compressor` option when preparing or executing queries.
  It's still good practice to pass it around so that your code stays agnostic of the
  native protocol being used.

  ## Native protocol

  Xandra supports the following versions of Cassandra's native protocol:
  #{Enum.join(Xandra.Frame.supported_protocols(), ", ")}.

  By default, Xandra will negotiate the protocol version with the Cassandra server.
  Xandra will start by trying to use the highest protocol it supports (which is
  #{Xandra.Frame.max_supported_protocol()}). If the server rejects that, then
  Xandra will reconnect with the protocol advertised by the server. If you
  want to force a specific version of the native protocol that Xandra should use,
  use the `:protocol_version` option.

  ## Logging

  Xandra connections log a few events like disconnections or connection failures.
  Logs contain the `:xandra_address` and `:xandra_port` metadata that you can
  choose to log if desired.

  ## Custom payloads

  The Cassandra native protocol supports exchanging **custom payloads** between
  server and client. A custom payload is a map of string keys to binary values
  (`t:custom_payload/0`).

  To *send* custom payloads to the server, you can pass the `:custom_payload`
  option to functions such as `prepare/3` and `execute/4`.

  If the server sends a custom payload in a response (`t:result/0`), you'll find
  it in the `:custom_payload` field of the corresponding struct (such as
  `Xandra.Page`, `Xandra.Void`, and so on).

  By default, Cassandra itself ignores custom payloads sent to the server. Other
  implementations built on top of the Cassandra native protocol might use
  custom payloads to provide implementation-specific functionality. One such
  example is Azure [Cosmos DB](https://azure.microsoft.com/en-us/services/cosmos-db/).
  """

  alias __MODULE__.{
    Batch,
    Connection,
    ConnectionError,
    Error,
    Frame,
    Prepared,
    Page,
    PageStream,
    RetryStrategy,
    Simple
  }

  @typedoc """
  A statement (query) to pass to `execute/4` and other functions.
  """
  @type statement :: String.t()

  @type values :: list | map

  @typedoc "The result of a query."
  @type result :: Xandra.Void.t() | Page.t() | Xandra.SetKeyspace.t() | Xandra.SchemaChange.t()

  @typedoc "A single Xandra connection process."
  @type conn :: :gen_statem.server_ref()

  @typedoc """
  An error that can be returned from a query.

  This is either a semantic error returned by Cassandra or a connection error.
  """
  @type error :: Error.t() | ConnectionError.t()

  @typedoc """
  Custom payload that Xandra can exchange with the server.

  See the ["Custom payloads"](#module-custom-payloads) section in the
  module documentation.
  """
  @type custom_payload :: %{optional(String.t()) => binary()}

  @typedoc """
  Cassandra native protocol version.
  """
  @typedoc since: "0.18.0"
  @type native_protocol_version() ::
          unquote(Enum.reduce(Frame.supported_protocols(), &quote(do: unquote(&1) | unquote(&2))))

  @typedoc """
  Possible values for the `:configure` option in `start_link/1`.
  """
  @typedoc since: "0.18.0"
  @type configure_fun() ::
          {module(), function_name :: atom(), [term()]} | (keyword() -> keyword())

  @valid_consistencies [
    :one,
    :two,
    :three,
    :serial,
    :all,
    :quorum,
    :local_one,
    :local_quorum,
    :each_quorum,
    :local_serial
  ]

  @typedoc """
  Consistency level for queries.

  This is supported as the default consistency on the connection level (see `start_link/1`)
  or as a per-query consistency (see `execute/3` and `execute/4`).
  """
  @typedoc since: "0.18.0"
  @type consistency ::
          unquote(Enum.reduce(@valid_consistencies, &quote(do: unquote(&1) | unquote(&2))))

  # Raw NimbleOptions schema before parsing. Broken out to work around
  # `mix format`.
  @start_link_opts_schema [
    atom_keys: [
      type: :boolean,
      default: false,
      doc: """
      Whether or not results of and parameters to `execute/4` will have atom
      keys. If `true`, the result maps will have column names returned as
      atoms rather than as strings. Additionally, maps that represent named
      parameters will need atom keys.
      """
    ],
    authentication: [
      type: {:custom, Xandra.OptionsValidators, :validate_authentication, []},
      type_doc: "tuple with `t:module/0` and `t:term/0`",
      doc: """
      Two-element tuple: the authenticator module to use for authentication
      and its supported options. See `Xandra.Authenticator`.
      """
    ],
    backoff_min: [
      type: :non_neg_integer,
      default: 1000,
      doc: "The minimum backoff interval (in milliseconds)."
    ],
    backoff_max: [
      type: :non_neg_integer,
      default: 30_000,
      doc: "The maximum backoff interval (in milliseconds)."
    ],
    backoff_type: [
      type: {:in, [:stop, :exp, :rand, :rand_exp]},
      default: :rand_exp,
      doc: """
      The backoff strategy. `:stop` means the connection will stop when a disconnection happens,
      `:exp` means exponential backoff, `:rand` is random backoff, and `:rand_exp` is random
      exponential backoff.
      """
    ],
    compressor: [
      type: {:custom, Xandra.OptionsValidators, :validate_module, ["compressor"]},
      type_doc: "`t:module/0`",
      doc: """
      The compressor module to use for compressing and decompressing data.
      See the ["Compression" section](#module-compression) in the module documentation. By default
      this option is not present, which means no compression is used.
      """
    ],
    configure: [
      type: {:or, [:mfa, {:fun, 1}]},
      type_doc: "`t:configure_fun/0`",
      doc: """
      A function to run before every connect attempt to dynamically configure the options. It's
      either a 1-arity fun which gets called with the options and must return options, or a
      `{module, function, args}` tuple with the options prepended to `args` (the return value
      of the function must still be options). By default, options are not modified. This
      function is called in the connection process. *Available since v0.18.0*.
      """
    ],
    connect_timeout: [
      type: :timeout,
      default: 5000,
      doc: """
      Time to wait (in milliseconds) for Xandra to establish a network connection.
      If the timeout is reached, the connection considers it as a failed connection attempt
      and behaves according to the `:backoff_type` option. *Available since v0.18.0*.
      """
    ],
    default_consistency: [
      type: {:in, @valid_consistencies},
      type_doc: "`t:consistency/0`",
      default: :one,
      doc: """
      The default consistency to set for all queries. Can be overridden on a per-query
      basis through the `:consistency` option in `execute/4`.
      """
    ],
    debug: [type: :any],
    encryption: [
      type: :boolean,
      default: false,
      doc: """
      Whether to connect to Cassandra using SSL. If you want to set up SSL
      options, see the `:transport_options` option.
      """
    ],
    keyspace: [
      type: :string,
      doc: """
      `USE` this keyspace right after establishing a connection to the server. This effectively
      replaces most uses of the `:after_connect` option that was available before v0.18.0.
      *Available since v0.18.0*.
      """
    ],
    max_concurrent_requests_per_connection: [
      type: :pos_integer,
      default: 100,
      doc: """
      The maximum number of requests that can be in flight at any given time on a single
      connection. Xandra "multiplexes" requests on a single connection, since that is allowed
      by the Cassandra protocol (via the use of stream IDs to identify in-flight requests on
      a particular connection). Increasing this option means that a single connection will
      handle more requests, so you can potentially lower the number of total connections in
      your connection pool. However, the more requests are in flight on a single connection,
      the more work that connection will have to do to decode and route requests and responses.
      *Available since 0.19.0*.
      """
    ],
    name: [
      type: :any,
      doc: """
      Name registration, just like `GenServer`.
      """
    ],
    nodes: [
      type: {:list, {:custom, Xandra.OptionsValidators, :validate_node, []}},
      default: ["127.0.0.1"],
      type_doc: "list of `t:String.t/0`",
      doc: """
      The Cassandra node to connect to. This option is a list for consistency with
      `Xandra.Cluster`, but if using `Xandra` directly, it can only contain a single node.
      Such node can have the form `"ADDRESS:PORT"`, or `"ADDRESS"` (port defaults to
      `9042`). See the documentation for `Xandra.Cluster` for more information on
      connecting to multiple nodes.
      """
    ],
    protocol_version: [
      type: {:in, Frame.supported_protocols()},
      type_doc: "`t:native_protocol_version/0`",
      doc: """
      The enforced version of the Cassandra native protocol to use. If this option
      is not present, Xandra will negotiate the protocol with the server, starting
      with the most recent one and falling back to older ones if needed. Must be one
      of #{Enum.map_join(Frame.supported_protocols(), ", ", &"`#{inspect(&1)}`")}. See
      the [relevant section](#module-native-protocol) in the module documentation.
      """
    ],
    show_sensitive_data_on_connection_error: [
      type: :boolean,
      default: false,
      doc: """
      Is it ok to show sensitive data on connection errors? Useful for
      debugging and in tests.
      """
    ],
    transport_options: [
      type: {:or, [:keyword_list, {:list, :any}]},
      doc: """
      Options to forward to the socket transport. If the `:encryption` option is `true`,
      then the transport is SSL (see the Erlang `:ssl` module) otherwise it's
      TCP (see the `:gen_tcp` Erlang module). The `:buffer` option (`t:pos_integer/0`), 
      which controls the size of the user level buffer in use by the active mode
      socket, defaults to `1_000_000` bytes.
      """
    ],

    # Internal options, used by Xandra.Cluster.
    cluster_pid: [doc: false, type: :pid]
  ]

  @typedoc "Options for `start_link/1`."
  @type start_option() :: unquote(NimbleOptions.option_typespec(@start_link_opts_schema))

  @doc """
  Returns the `NimbleOptions` schema used for validating the options for `start_link/1`.

  This function is meant to be used by other libraries that want to extend Xandra's
  functionality, and that rely on `start_link/1` at some point. The keys in the returned
  schema are not all public, so you should always refer to the documentation for
  `start_link/1`.

  ## Examples

      iex> schema = Xandra.start_link_opts_schema()
      iex> schema[:encryption][:type]
      :boolean

  """
  @doc since: "0.15.0"
  @spec start_link_opts_schema() :: keyword()
  def start_link_opts_schema do
    @start_link_opts_schema
  end

  @doc """
  Starts a new pool of connections to Cassandra.

  This function starts a new connection to the provided Cassandra node.

  ## Options

  These are the options supported by this function:

  #{NimbleOptions.docs(@start_link_opts_schema)}

  ## Examples

      # Start a connection:
      {:ok, conn} = Xandra.start_link()

      # Start a connection and register it under a name:
      {:ok, _conn} = Xandra.start_link(name: :xandra)

  If you're using Xandra under a supervisor, see `child_spec/1`.
  """
  @spec start_link([start_option()]) :: GenServer.on_start()
  def start_link(options \\ []) when is_list(options) do
    options = NimbleOptions.validate!(options, @start_link_opts_schema)

    {node, options} =
      case Keyword.pop(options, :nodes) do
        {[], _opts} ->
          raise ArgumentError, "the :nodes option can't be an empty list"

        {[{_address, _port} = node], opts} ->
          {node, opts}

        {nodes, _opts} ->
          raise ArgumentError, """
          cannot use multiple nodes in the :nodes option with a single Xandra connection.
          Use Xandra.Cluster for that. The :nodes option was set to:

            #{inspect(nodes)}

          """
      end

    options =
      options
      |> Keyword.put(:node, node)
      |> Keyword.put(:prepared_cache, Prepared.Cache.new())

    Connection.start_link(options)
  end

  @doc """
  Returns a child spec to use Xandra in supervision trees.

  To use Xandra without passing any options you can just do:

      children = [
        Xandra,
        # ...
      ]

  If you want to pass options, use a two-element tuple like
  usual when using child specs:

      children = [
        {Xandra, name: :xandra_connection}
      ]

  """
  @spec child_spec([start_option()]) :: Supervisor.child_spec()
  def child_spec(options) do
    %{
      id: __MODULE__,
      type: :worker,
      start: {__MODULE__, :start_link, [options]}
    }
  end

  @doc """
  Streams the results of a simple query or a prepared query with the given `params`.

  This function can be used to stream the results of `query` so as not to load
  them entirely in memory. This function doesn't send any query to Cassandra
  right away: it will only execute queries as necessary when results are
  requested out of the returned stream.

  The returned value is a stream of `Xandra.Page` structs, where each of such
  structs contains at most as many rows as specified by the `:page_size`
  option. Every time an element is requested from the stream, `query` will be
  executed with `params` to get that result.

  In order to get each result from Cassandra, `execute!/4` is used: this means
  that if there is an error (such as a network error) when executing the
  queries, that error will be raised.

  ### Simple or prepared queries

  Regardless of `query` being a simple query or a prepared query, this function
  will execute it every time a result is needed from the returned stream. For
  this reason, it is usually a good idea to use prepared queries when streaming.

  ## Options

  `options` supports all the options supported by `execute/4`, with the same
  default values.

  ## Examples

      prepared = Xandra.prepare!(conn, "SELECT * FROM users")
      users_stream = Xandra.stream_pages!(conn, prepared, _params = [], page_size: 2)

      [%Xandra.Page{} = _page1, %Xandra.Page{} = _page2] = Enum.take(users_stream, 2)

  """
  @spec stream_pages!(conn, statement | Prepared.t(), values, keyword) :: Enumerable.t()
  def stream_pages!(conn, query, params, options \\ [])

  def stream_pages!(conn, statement, params, options) when is_binary(statement) do
    %PageStream{conn: conn, query: statement, params: params, options: options}
  end

  def stream_pages!(conn, %Prepared{} = prepared, params, options) do
    %PageStream{conn: conn, query: prepared, params: params, options: options}
  end

  @prepare_opts_schema [
    compressor: [
      type: {:custom, Xandra.OptionsValidators, :validate_module, ["compressor"]},
      doc: """
      The compressor module to use for compressing and decompressing data.
      See the "Compression" section in the module documentation. By default
      this option is not present, which means no compression is used.
      """
    ],
    force: [
      type: :boolean,
      default: false,
      doc: """
      When `true`, forces the preparation of the query on
      the server instead of trying to read the prepared query from cache. See
      the "Prepared queries cache" section below.
      """
    ],
    tracing: [
      type: :boolean,
      default: false,
      doc: """
      Turn on tracing for the preparation of the
      given query and sets the `tracing_id` field in the returned prepared
      query. See the "Tracing" option in `execute/4`.
      """
    ],
    custom_payload: [
      type: {:custom, Xandra.OptionsValidators, :validate_custom_payload, []},
      doc: """
      A custom payload to send to the Cassandra server alongside the request. Only
      supported in `QUERY`, `PREPARE`, `EXECUTE`, and `BATCH` requests. The custom
      payload must be of type `t:custom_payload/0`. See the
      ["Custom payloads"](#module-custom-payloads) section in the module documentation.
      """
    ],
    telemetry_metadata: [
      type: :map,
      default: %{},
      doc: """
      Custom metadata to be added to the metadata of `[:xandra, :prepare_query, :start]`
      and `[:xandra, :prepare_query, :stop]` telemetry events as `extra_metadata` field.
      """
    ],
    timeout: [
      type: :timeout,
      default: :infinity,
      doc: """
      The timeout for this call, in milliseconds.
      """
    ]
  ]

  @doc """
  Prepares the given query.

  This function prepares the given statement on the Cassandra server. If
  preparation is successful and there are no network errors while talking to the
  server, `{:ok, prepared}` is returned, otherwise `{:error, error}` is
  returned.

  The returned prepared query can be run through `execute/4`, or used inside a
  batch (see `Xandra.Batch`).

  Errors returned by this function can be either `Xandra.Error` or
  `Xandra.ConnectionError` structs. See the module documentation for more
  information about errors.

  ## Options

  #{NimbleOptions.docs(@prepare_opts_schema)}

  ## Prepared queries cache

  Since Cassandra prepares queries on a per-node basis (and not on a
  per-connection basis), Xandra internally caches prepared queries for each
  connection or pool of connections. This means that if you prepare a query that
  was already prepared, no action will be executed on the Cassandra server and
  the prepared query will be returned from the cache.

  If the Cassandra node goes down, however, the prepared query will be
  invalidated and trying to use the one from cache will result in a
  `Xandra.Error`. However, this is automatically handled by Xandra: when such an
  error is returned, Xandra will first retry to prepare the query and only
  return an error if the preparation fails.

  If you want to ensure a query is prepared on the server, you can set the
  `:force` option to `true`.

  ## Examples

      {:ok, prepared} = Xandra.prepare(conn, "SELECT * FROM users WHERE id = ?")
      {:ok, _page} = Xandra.execute(conn, prepared, [_id = 1])

      {:error, %Xandra.Error{reason: :invalid_syntax}} = Xandra.prepare(conn, "bad syntax")

      # Force a query to be prepared on the server and not be read from cache:
      Xandra.prepare!(conn, "SELECT * FROM users WHERE ID = ?", force: true)

  """
  @spec prepare(conn, statement, keyword) :: {:ok, Prepared.t()} | {:error, error}
  def prepare(conn, statement, options \\ []) when is_binary(statement) do
    options = NimbleOptions.validate!(options, @prepare_opts_schema)
    prepared = %Prepared{statement: statement}
    Connection.prepare(conn, prepared, options)
  end

  @doc """
  Prepares the given query, raising if there's an error.

  This function works exactly like `prepare/3`, except it returns the prepared
  query directly if preparation succeeds, otherwise raises the returned error.

  ## Examples

      prepared = Xandra.prepare!(conn, "SELECT * FROM users WHERE id = ?")
      {:ok, _page} = Xandra.execute(conn, prepared, [_id = 1])

  """
  @spec prepare!(conn, statement, keyword) :: Prepared.t() | no_return
  def prepare!(conn, statement, options \\ []) do
    case prepare(conn, statement, options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Executes the given simple query, prepared query, or batch query.

  Returns `{:ok, result}` if executing the query was successful, or `{:error,
  error}` otherwise. The meaning of the `params_or_options` argument depends on
  what `query` is:

    * if `query` is a batch query, than `params_or_options` has to be a list of
      options that will be used to run the batch query (since batch queries
      don't use parameters as parameters are attached to each query in the
      batch).

    * if `query` is a simple query (a string) or a prepared query, then
      `params_or_opts` is a list or map of parameters, and this function is
      exactly the same as calling `execute(conn, query, params_or_options, [])`.

  When `query` is a batch query, successful results will always be `Xandra.Void`
  structs. See `execute/4` for full documentation on all supported options.

  When `{:error, error}` is returned, `error` can be either a `Xandra.Error` or
  a `Xandra.ConnectionError` struct. See the module documentation for more
  information on errors.

  ## Options for batch queries

  When `query` is a batch query, `params_or_options` is a list of options. The following
  options are supported:

    * `:consistency` - same as the `:consistency` option described in the
      documentation for `execute/4`.

    * `:serial_consistency` - same as the `:serial_consistency` option described
      in the documentation for `execute/4`.

    * `:timestamp` - using this option means that the provided
      timestamp will apply to all the statements in the batch that do not
      explicitly specify a timestamp.

  See `execute/4` for full documentation on all supported options if `query` is not a batch
  query.

  ## Examples

  For examples on executing simple queries or prepared queries, see the
  documentation for `execute/4`. Examples below specifically refer to batch
  queries. See the documentation for `Xandra.Batch` for more information about
  batch queries and how to construct them.

      prepared_insert = Xandra.prepare!(conn, "INSERT (email, name) INTO users VALUES (?, ?)")

      batch =
        Xandra.Batch.new()
        |> Xandra.Batch.add(prepared_insert, ["abed@community.com", "Abed Nadir"])
        |> Xandra.Batch.add(prepared_insert, ["troy@community.com", "Troy Barnes"])
        |> Xandra.Batch.add(prepared_insert, ["britta@community.com", "Britta Perry"])

      # Execute the batch:
      Xandra.execute(conn, batch)
      #=> {:ok, %Xandra.Void{}}

      # Execute the batch with a default timestamp for all statements:
      Xandra.execute(conn, batch, timestamp: System.system_time(:millisecond) - 1_000)
      #=> {:ok, %Xandra.Void{}}

  """
  @spec execute(conn, statement | Prepared.t(), values) :: {:ok, result} | {:error, error}
  @spec execute(conn, Batch.t(), keyword) :: {:ok, Xandra.Void.t()} | {:error, error}
  def execute(conn, query, params_or_options \\ [])

  def execute(conn, statement, params) when is_binary(statement) do
    execute(conn, statement, params, _options = [])
  end

  def execute(conn, %Prepared{} = prepared, params) do
    execute(conn, prepared, params, _options = [])
  end

  def execute(conn, %Batch{} = batch, options) when is_list(options) do
    execute_with_retrying(conn, batch, nil, options)
  end

  @execute_opts_schema [
    consistency: [
      type: {:in, @valid_consistencies},
      doc: """
      Specifies the consistency level for the given
      query. See the Cassandra documentation for more information on consistency
      levels. If not present, defaults to the value of the `:default_consistency` option
      used when starting the connection (see `start_link/1`).
      The value of this option can be one of:
      #{Enum.map_join(@valid_consistencies, "\n", &"  * `#{inspect(&1)}`")}
      """
    ],
    page_size: [
      type: :non_neg_integer,
      default: 10_000,
      doc: """
      The size of a page of results. If `query` returns a
      `Xandra.Page` struct, that struct will contain at most `:page_size` rows in it.
      """
    ],
    paging_state: [
      type:
        {:or,
         [{:custom, Xandra.OptionsValidators, :validate_binary, [:paging_state]}, {:in, [nil]}]},
      doc: """
      The offset where rows should be returned from. By default this option is not
      present and paging starts from the beginning. See the "Paging" section
      below for more information on how to page queries.
      """
    ],
    timestamp: [
      type: :integer,
      doc: """
      The default timestamp for the query, expressed in microseconds. If provided,
      overrides the server-side assigned timestamp. However, a timestamp in
      the query itself will still override this timestamp.
      """
    ],
    serial_consistency: [
      type: {:in, [:serial, :local_serial]},
      doc: """
      Specifies the serial consistency to use for executing the given query. Can
      be one of `:serial` or `:local_serial`. By default this option is not present.
      """
    ],
    compressor: [
      type: {:custom, Xandra.OptionsValidators, :validate_module, ["compressor"]},
      doc: """
      The compressor module to use for compressing and decompressing data.
      See the "Compression" section in the module documentation. By default
      this option is not present, which means no compression is used.
      """
    ],
    retry_strategy: [
      type: {:custom, Xandra.OptionsValidators, :validate_module, ["retry strategy"]},
      doc: """
      The module implementing the `Xandra.RetryStrategy` behaviour that is used in case
      the query fails to determine whether to retry it or not. See the
      "Retrying failed queries" section in the module documentation.
      By default, this option is not present, which means no retries are attempted.
      """
    ],
    tracing: [
      type: :boolean,
      default: false,
      doc: """
      Turn on tracing for the preparation of the
      given query and sets the `tracing_id` field in the returned prepared
      query. See the "Tracing" option in `execute/4`.
      """
    ],
    custom_payload: [
      type: {:custom, Xandra.OptionsValidators, :validate_custom_payload, []},
      doc: """
      A custom payload to send to the Cassandra server alongside the request. Only
      supported in `QUERY`, `PREPARE`, `EXECUTE`, and `BATCH` requests. The custom
      payload must be of type `t:custom_payload/0`. See the
      ["Custom payloads"](#module-custom-payloads) section in the module documentation.
      """
    ],
    date_format: [
      type: {:in, [:date, :integer]},
      default: :date,
      doc: """
      Controls the format in which dates are returned. When set to `:integer`, the
      returned value is a number of days from the Unix epoch. When set to
      `:date`, the returned value is a date struct.
      """
    ],
    time_format: [
      type: {:in, [:time, :integer]},
      default: :time,
      doc: """
      Controls the format in which times are returned. When set to `:integer`, the
      returned value is a number of nanoseconds from midnight. When set to
      `:time`, the returned value is a time struct.
      """
    ],
    timestamp_format: [
      type: {:in, [:datetime, :integer]},
      default: :datetime,
      doc: """
      Controls the format in which timestamps are returned. When set to `:integer`, the
      returned value is a number of milliseconds from the Unix epoch. When set to
      `:datetime`, the returned value is a datetime struct.
      """
    ],
    decimal_format: [
      type: {:in, [:decimal, :tuple]},
      default: :tuple,
      doc: """
      Controls the format in which decimals are returned. When set to `:decimal`, a
      `Decimal` struct from the [decimal](https://hex.pm/packages/decimal) package is
      returned. When set to `:tuple`, a `{value, scale}` is returned such that
      the returned number is `value * 10^(-1 * scale)`. If you use `:decimal`,
      you'll have to add the `:decimal` dependency to your application explicitly.
      """
    ],
    uuid_format: [
      type: {:in, [:binary, :string]},
      default: :string,
      doc: """
      Controls the format in which UUIDs are returned. When set to `:binary`, UUIDs are
      returned as raw 16-byte binaries, such as: `<<0, 182, 145, 128, 208, 225, 17, 226,
      139, 139, 8, 0, 32, 12, 154, 102>>`. When set to `:string`, UUIDs are returned in
      their human-readable format, such as: `"fe2b4360-28c6-11e2-81c1-0800200c9a66"`.
      """
    ],
    timeuuid_format: [
      type: {:in, [:binary, :string]},
      default: :string,
      doc: """
      Same as the `:uuid_format` option, but for values of the *timeuuid* type.
      """
    ],
    telemetry_metadata: [
      type: :map,
      default: %{},
      doc: """
      Custom metadata to be added to the metadata of `[:xandra, :execute_query, :start]`
      and `[:xandra, :execute_query, :stop]` telemetry events as `extra_metadata` field.
      """
    ],
    timeout: [
      type: :timeout,
      default: :infinity,
      doc: """
      The timeout for this call, in milliseconds.
      """
    ]
  ]

  @execute_opts_keys Keyword.keys(@execute_opts_schema)

  @doc """
  Executes the given simple query or prepared query with the given parameters.

  Returns `{:ok, result}` where `result` is the result of executing `query` if
  the execution is successful (there are no network errors or semantic errors
  with the query), or `{:error, error}` otherwise.

  `result` can be one of the following:

    * a `Xandra.Void` struct - returned for queries such as `INSERT`, `UPDATE`,
      or `DELETE`.

    * a `Xandra.SchemaChange` struct - returned for queries that perform changes
      on the schema (such as creating tables).

    * a `Xandra.SetKeyspace` struct - returned for `USE` queries.

    * a `Xandra.Page` struct - returned for queries that return rows (such as
      `SELECT` queries).

  The properties of each of the results listed above are described in each
  result's module.

  ## Options

  This function supports any arbitrary option, since Xandra passes those down
  to the `Xandra.RetryStrategy` module passed in `:retry_strategy`. However, below
  is a list of the options that are specific to Xandra and that Xandra uses when executing
  the query. Note that we might *add* options to this list in the future, which could
  potentially change the meaning of custom options you use to implement your own retry
  strategy, and we wouldn't consider this a breaking change. Because of this, we recommend
  *scoping* custom options in your retry strategy module (for example, by prefixing them
  with `<my_module>_<option_name`).

  Here are the Xandra-specific options:

  #{NimbleOptions.docs(@execute_opts_schema)}

  ## Parameters

  The `params` argument specifies parameters to use when executing the query; it
  can be either a list of positional parameters (specified via `?` in the query)
  or a map of named parameters (specified as `:named_parameter` in the
  query). When `query` is a simple query, the value of each parameter must be a
  two-element tuple specifying the type used to encode the value and the value
  itself; when `query` is a prepared query, this is not necessary (and values
  can just be values) as the type information is encoded in the prepared
  query. See the module documentation for more information about query
  parameters, types, and encoding values.

  ## Examples

  Executing a simple query (which is just a string):

      statement = "INSERT INTO users (first_name, last_name) VALUES (:first_name, :last_name)"

      {:ok, %Xandra.Void{}} =
        Xandra.execute(conn, statement, %{
          "first_name" => {"text", "Chandler"},
          "last_name" => {"text", "Bing"},
        })

  Executing the query when `atom_keys: true` has been specified in `Xandra.start_link/1`:

      Xandra.execute(conn, statement, %{
        first_name: {"text", "Chandler"},
        last_name: {"text", "Bing"}
      })

  Executing a prepared query:

      prepared = Xandra.prepare!(conn, "INSERT INTO users (first_name, last_name) VALUES (?, ?)")

      {:ok, %Xandra.Void{}} = Xandra.execute(conn, prepared, ["Monica", "Geller"])

  Performing a `SELECT` query and using `Enum.to_list/1` to convert the
  `Xandra.Page` result to a list of rows:

      statement = "SELECT * FROM users"
      {:ok, %Xandra.Page{} = page} = Xandra.execute(conn, statement, _params = [])
      Enum.to_list(page)
      #=> [%{"first_name" => "Chandler", "last_name" => "Bing"},
      #=>  %{"first_name" => "Monica", "last_name" => "Geller"}]

  Performing the query when `atom_keys: true` has been specified in `Xandra.start_link/1`:

      {:ok, page} = Xandra.execute(conn, statement, _params = [])
      Enum.to_list(page)
      #=> [%{first_name:  "Chandler", last_name: "Bing"},
      #=>  %{first_name: "Monica", last_name: "Geller"}]

  Ensuring the write is written to the commit log and memtable of at least three replica nodes:

      statement = "INSERT INTO users (first_name, last_name) VALUES ('Chandler', 'Bing')"
      {:ok, %Xandra.Void{}} = Xandra.execute(conn, statement, _params = [], consistency: :three)

  ## Paging

  Since `execute/4` supports the `:paging_state` option, it is possible to manually
  implement paging. For example, given the following prepared query:

      prepared = Xandra.prepare!(conn, "SELECT first_name FROM users")

  We can now execute such query with a specific page size using the `:page_size`
  option:

      {:ok, %Xandra.Page{} = page} = Xandra.execute(conn, prepared, [], page_size: 2)

  Since `:page_size` is `2`, `page` will contain at most `2` rows:

      Enum.to_list(page)
      #=> [%{"first_name" => "Ross"}, %{"first_name" => "Rachel"}]

  Now, we can pass `page.paging_state` as the value of the `:paging_state`
  option to let the paging start from where we left off:

      {:ok, %Xandra.Page{} = new_page} =
        Xandra.execute(conn, prepared, [], page_size: 2, paging_state: page.paging_state)

      Enum.to_list(page)
      #=> [%{"first_name" => "Joey"}, %{"first_name" => "Phoebe"}]

  However, using `:paging_state` and `:page_size` directly with `execute/4` is not
  recommended when the intent is to "stream" a query. For that, it's recommended
  to use `stream_pages!/4`. Also note that if the `:paging_state` option is set to `nil`,
  meaning there are no more pages to fetch, an `ArgumentError` exception will be raised;
  be sure to check for this with `page.paging_state != nil`.

  ## Tracing

  Cassandra supports [tracing queries](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlshTracing.html).
  If you set the `:tracing` option to `true`, the executed query will be traced.
  This means that a tracing ID (a binary UUID) will be set in the response of the query
  and that Cassandra will write relevant tracing events to tracing-related tables in the
  `system_traces` keyspace.

  In Xandra, all response structs contain an accessible `tracing_id` field that is set
  to `nil` except for when tracing is enabled. In those cases, `tracing_id` is a binary
  UUID that you can use to select events from the traces tables.

  For example:

      {:ok, page} = Xandra.execute(conn, "SELECT * FROM users", [], tracing: true)

      statement = "SELECT * FROM system_traces.events WHERE session_id = ?"
      {:ok, trace_events_page} = Xandra.execute(conn, statement, [{"uuid", page.tracing_id}])

  Note that tracing is an expensive operation for Cassandra that puts load on
  executing queries. This is why this option is only supported *per-query* in
  `execute/4` instead of connection-wide.
  """
  @spec execute(conn, statement | Prepared.t(), values, keyword) ::
          {:ok, result} | {:error, error}
  def execute(conn, query, params, options)

  def execute(conn, statement, params, options) when is_binary(statement) do
    query = %Simple{statement: statement}
    assert_valid_paging_state(options)
    execute_with_retrying(conn, query, params, options)
  end

  def execute(conn, %Prepared{} = prepared, params, options) do
    assert_valid_paging_state(options)
    execute_with_retrying(conn, prepared, params, options)
  end

  @doc """
  Executes the given simple query, prepared query, or batch query, raising if
  there's an error.

  This function behaves exactly like `execute/3`, except that it returns
  successful results directly and raises on errors.

  ## Examples

      Xandra.execute!(conn, "INSERT INTO users (name, age) VALUES ('Jane', 29)")
      #=> %Xandra.Void{}

  """
  @spec execute!(conn, statement | Prepared.t(), values) :: result | no_return
  @spec execute!(conn, Batch.t(), keyword) :: Xandra.Void.t() | no_return
  def execute!(conn, query, params_or_options \\ []) do
    case execute(conn, query, params_or_options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Executes the given simple query, prepared query, or batch query, raising if
  there's an error.

  This function behaves exactly like `execute/4`, except that it returns
  successful results directly and raises on errors.

  ## Examples

      statement = "INSERT INTO users (name, age) VALUES ('John', 43)"
      Xandra.execute!(conn, statement, _params = [], consistency: :quorum)
      #=> %Xandra.Void{}

  """
  @spec execute!(conn, statement | Prepared.t(), values, keyword) :: result | no_return
  def execute!(conn, query, params, options) do
    case execute(conn, query, params, options) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc false
  @spec run(conn, (conn -> result)) :: no_return()
  @spec run(conn, keyword, (conn -> result)) :: no_return()
  def run(_conn, _options \\ [], fun) when is_function(fun, 1) do
    raise "not available since v0.18.0"
  end

  @doc """
  Synchronously stops the given connection with the given reason.

  Waits `timeout` milliseconds for the connection to stop before aborting and exiting.
  """
  @doc since: "0.15.0"
  @spec stop(conn, term, timeout) :: :ok
  def stop(conn, reason \\ :normal, timeout \\ :infinity)
      when timeout == :infinity or (is_integer(timeout) and timeout >= 0) do
    GenServer.stop(conn, reason, timeout)
  end

  ## Helpers

  defp reprepare_queries(conn, queries, options) do
    prepare_options =
      options
      |> Keyword.take(Keyword.keys(@prepare_opts_schema))
      |> Keyword.put(:force, true)

    {:ok, Enum.map(queries, &reprepare_query(conn, &1, prepare_options))}
  catch
    {:reprepare_error, error} ->
      error
  end

  defp reprepare_query(_conn, %Simple{} = query, _prepare_options) do
    query
  end

  defp reprepare_query(conn, %Prepared{statement: statement, values: values}, prepare_options) do
    case prepare(conn, statement, prepare_options) do
      {:ok, reprepared} -> %Prepared{reprepared | values: values}
      other -> throw({:reprepare_error, other})
    end
  end

  defp assert_valid_paging_state(options) do
    case Keyword.fetch(options, :paging_state) do
      {:ok, nil} ->
        raise ArgumentError, "no more pages are available"

      {:ok, value} when not is_binary(value) ->
        raise ArgumentError,
              "expected a binary as the value of the :paging_state option, " <>
                "got: #{inspect(value)}"

      _other ->
        :ok
    end
  end

  defp execute_with_retrying(conn, query, params, options) do
    {xandra_opts, other_opts} = Keyword.split(options, @execute_opts_keys)
    options = NimbleOptions.validate!(xandra_opts, @execute_opts_schema) ++ other_opts

    RetryStrategy.run_on_single_conn(options, fn ->
      execute_without_retrying(conn, query, params, options)
    end)
  end

  defp execute_without_retrying(conn, %Batch{} = batch, nil, options) do
    case Connection.execute(conn, batch, nil, options) do
      {:error, %Error{reason: :unprepared}} ->
        with {:ok, queries} <- reprepare_queries(conn, batch.queries, options) do
          batch = %Batch{batch | queries: queries}
          execute(conn, batch, options)
        end

      {:error, %Error{} = error} ->
        {:error, error}

      {:ok, result} ->
        {:ok, result}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp execute_without_retrying(conn, %Simple{} = query, params, options) do
    Connection.execute(conn, query, params, options)
  end

  defp execute_without_retrying(conn, %Prepared{} = prepared, params, options) do
    case Connection.execute(conn, prepared, params, options) do
      {:error, %Error{reason: :unprepared}} ->
        with {:ok, reprepared} <-
               Connection.prepare(
                 conn,
                 prepared,
                 Keyword.put(options, :force, true)
               ) do
          Connection.execute(conn, reprepared, params, options)
        end

      {:ok, result} ->
        {:ok, result}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
