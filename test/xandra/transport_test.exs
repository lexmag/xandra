defmodule Xandra.TransportTest do
  use ExUnit.Case, async: true

  alias Xandra.Transport

  require Xandra.Transport

  describe "close/1" do
    test "returns an updated transport" do
      assert {:ok, listen_socket} = :gen_tcp.listen(0, [])
      assert {:ok, port} = :inet.port(listen_socket)

      transport = %Transport{module: :gen_tcp, options: []}
      assert {:ok, transport} = Transport.connect(transport, ~c"localhost", port, 5000)
      assert transport.socket != nil

      assert %Transport{} = transport = Transport.close(transport)
      assert transport.socket == nil
    end

    test "returns an IPv6-compatible transport" do
      assert {:ok, listen_socket} = :gen_tcp.listen(0, [:inet6])
      assert {:ok, port} = :inet.port(listen_socket)

      transport = %Transport{module: :gen_tcp, options: [:inet6]}
      assert {:ok, transport} = Transport.connect(transport, ~c"::1", port, 5000)
      assert transport.socket != nil

      assert %Transport{} = transport = Transport.close(transport)
      assert transport.socket == nil
    end
  end

  describe "is_* macros" do
    test "returns an updated transport" do
      assert {:ok, listen_socket} = :gen_tcp.listen(0, [])
      assert {:ok, port} = :inet.port(listen_socket)

      transport = %Transport{module: :gen_tcp, options: []}
      assert {:ok, transport} = Transport.connect(transport, ~c"localhost", port, 5000)

      assert Transport.is_data_message(transport, {:tcp, transport.socket, "data"}) == true
      assert Transport.is_data_message(transport, {:tcp, nil, "data"}) == false

      assert Transport.is_closed_message(transport, {:tcp_closed, transport.socket}) == true
      assert Transport.is_closed_message(transport, {:tcp_closed, nil}) == false

      assert Transport.is_error_message(transport, {:tcp_error, transport.socket, :econnrefused}) ==
               true
    end
  end
end
