defmodule Xandra.Cluster.ControlConnectionTest do
  use ExUnit.Case

  alias Xandra.Cluster.ControlConnection

  @protocol_module (case System.get_env("CASSANDRA_NATIVE_PROTOCOL") || "v3" do
                      "v3" -> Xandra.Protocol.V3
                      "v4" -> Xandra.Protocol.V4
                    end)

  test "reporting data upon successful connection" do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)

    node_ref = make_ref()

    args = [
      _cluster = mirror,
      node_ref,
      'localhost',
      9042,
      _options = [protocol_module: @protocol_module],
      _autodiscovery? = true
    ]

    assert {:ok, _ctrl_conn} =
             start_supervised(%{
               id: ControlConnection,
               start: {ControlConnection, :start_link, args},
               type: :worker
             })

    assert_receive {^mirror_ref,
                    {:"$gen_cast", {:activate, _ref, {127, 0, 0, 1}, 9042, "datacenter1"}}}

    assert_receive {^mirror_ref, {:"$gen_cast", {:discovered_peers, [], "datacenter1"}}}
  end

  test "reconnecting after a disconnection" do
    parent = self()
    mirror_ref = make_ref()
    mirror = spawn_link(fn -> mirror(parent, mirror_ref) end)

    node_ref = make_ref()

    args = [
      _cluster = mirror,
      node_ref,
      'localhost',
      9042,
      _options = [protocol_module: @protocol_module],
      _autodiscovery? = false
    ]

    assert {:ok, ctrl_conn} =
             start_supervised(%{
               id: ControlConnection,
               start: {ControlConnection, :start_link, args},
               type: :worker
             })

    assert_receive {^mirror_ref,
                    {:"$gen_cast", {:activate, _ref, {127, 0, 0, 1}, 9042, "datacenter1"}}}

    assert {:connected, data} = :sys.get_state(ctrl_conn)
    send(ctrl_conn, {:tcp_closed, data.socket})

    assert_receive {^mirror_ref,
                    {:"$gen_cast",
                     {:update, {:control_connection_established, {127, 0, 0, 1}}, "datacenter1"}}}
  end

  defp mirror(parent, ref) do
    receive do
      message -> send(parent, {ref, message})
    end

    mirror(parent, ref)
  end
end
