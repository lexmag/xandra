defmodule Xandra.Protocol do
  def encode(statement) do
    <<byte_size(statement)::32>> <>
      statement <>
      encode_consistency_level(:one) <>
      encode_query_flags()
  end

  @consistency_levels %{
    0x0000 => :any,
    0x0001 => :one,
    0x0002 => :two,
    0x0003 => :three,
    0x0004 => :quorum,
    0x0005 => :all,
    0x0006 => :local_quorum,
    0x0007 => :each_quorum,
    0x0008 => :serial,
    0x0009 => :local_serial,
    0x000A => :local_one,
  }

  for {spec, level} <- @consistency_levels do
    defp encode_consistency_level(unquote(level)) do
      <<unquote(spec)::16>>
    end
  end

  defp encode_query_flags() do
    <<0x00>>
  end

  # ERROR
  def decode_response(<<_cql_version, _flags, _stream_id::16, 0x00>>, body) do
    <<code::32-signed>> <> rest = body
    {message, ""} = decode_string(rest)
    {code, message}
  end

  # READY
  def decode_response(<<_cql_version, _flags, _stream_id::16, 0x02>>, "") do
    :ok
  end

  # SUPPORTED
  def decode_response(<<_cql_version, _flags, _stream_id::16, 0x06>>, body) do
    {content, ""} = decode_string_multimap(body)
    content
  end

  # RESULT
  def decode_response(<<_cql_version, _flags, _stream_id::16, 0x08>>, body) do
    decode_result_response(body)
  end

  defp decode_result_response(<<0x0001::32-signed>>) do
    :ok
  end

  defp decode_result_response(<<0x0002::32-signed>> <> rest) do
    {nil, column_specs, rest} = decode_metadata(rest)
    decode_rows(rest, column_specs)
  end

  defp decode_result_response(<<0x0003::32-signed>> <> rest) do
    {_keyspace, ""} = decode_string(rest)
  end

  defp decode_metadata(<<flags::4-bytes, column_count::32-signed>> <> rest) do
    <<_::29, no_metadata::1, has_more_pages::1, global_tables_spec::1>> = flags
    0 = has_more_pages
    cond do
      no_metadata == 1 ->
        {nil, nil, rest}
      global_tables_spec == 1 ->
        {keyspace_name, rest} = decode_string(rest)
        {table_name, rest} = decode_string(rest)
        {column_specs, rest} = decode_column_specs(rest, column_count, {keyspace_name, table_name}, [])
        {nil, column_specs, rest}
      true ->
        {column_specs, rest} = decode_column_specs(rest, column_count, nil, [])
        {nil, column_specs, rest}
    end
  end

  defp decode_rows(<<row_count::32-signed>> <> buffer, column_specs) do
    {rows, ""} = decode_rows(row_count, buffer, column_specs, column_specs, [%{}])
    rows
  end

  def decode_rows(0, buffer, column_specs, column_specs, [_ | acc]) do
    {Enum.reverse(acc), buffer}
  end

  def decode_rows(row_count, buffer, column_specs, [], acc) do
    decode_rows(row_count - 1, buffer, column_specs, column_specs, [%{} | acc])
  end

  def decode_rows(row_count, <<size::32-signed>> <> buffer, column_specs, [{_, _, name, type} | rest], [row | acc]) do
    {value, buffer} = decode_value(size, buffer, type)
    row = Map.put(row, name, value)
    decode_rows(row_count, buffer, column_specs, rest, [row | acc])
  end

  defp decode_value(<<size::32-signed>> <> buffer, type) do
    decode_value(size, buffer, type)
  end

  defp decode_value(value_size, buffer, :ascii) do
    <<value::size(value_size), buffer::bytes>> = buffer
    {value, buffer}
  end

  defp decode_value(8, <<value::64-signed>> <> buffer, :bigint) do
    {value, buffer}
  end

  defp decode_value(1, <<value::8>> <> buffer, :boolean) do
    {value == 1, buffer}
  end

  # TODO: Decimal

  defp decode_value(8, <<value::64-float>> <> buffer, :double) do
    {value, buffer}
  end

  defp decode_value(4, <<value::32-float>> <> buffer, :float) do
    {value, buffer}
  end

  defp decode_value(4, <<address::4-bytes>> <> buffer, :inet) do
    <<n1, n2, n3, n4>> = address
    {{n1, n2, n3, n4}, buffer}
  end

  defp decode_value(16, <<address::16-bytes>> <> buffer, :inet) do
    <<n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16>> = address
    {{n1, n2, n3, n4, n5, n6, n7, n8, n9, n10, n11, n12, n13, n14, n15, n16}, buffer}
  end

  defp decode_value(4, <<value::32-signed>> <> buffer, :int) do
    {value, buffer}
  end

  defp decode_value(length, buffer, {:list, type}) do
    decode_list(length, buffer, type, [])
  end

  defp decode_value(size, buffer, {:map, key_type, value_type}) do
    decode_map(size, buffer, key_type, value_type, [])
  end

  defp decode_value(length, buffer, {:set, type}) do
    {list, buffer} = decode_list(length, buffer, type, [])
    {MapSet.new(list), buffer}
  end

  defp decode_value(length, buffer, :varchar) do
     <<text::size(length)-bytes>> <> buffer = buffer
    {text, buffer}
  end

  defp decode_value(8, <<value::64-signed>> <> buffer, :timestamp) do
    {value, buffer}
  end

  defp decode_list(0, buffer, _type, acc) do
    {Enum.reverse(acc), buffer}
  end

  defp decode_list(length, buffer, type, acc) do
    {elem, buffer} = decode_value(buffer, type)
    decode_list(length - 1, buffer, type, [elem | acc])
  end

  defp decode_map(0, buffer, _key_type, _value_type, acc) do
    {Map.new(acc), buffer}
  end

  defp decode_map(size, buffer, key_type, value_type, acc) do
    {key, buffer} = decode_value(buffer, key_type)
    {value, buffer} = decode_value(buffer, value_type)
    decode_map(size - 1, buffer, key_type, value_type, [{key, value} | acc])
  end

  defp decode_column_specs(rest, 0, _global_tables_spec, acc) do
    {Enum.reverse(acc), rest}
  end

  defp decode_column_specs(rest, column_count, nil, acc) do
    {keyspace_name, rest} = decode_string(rest)
    {table_name, rest} = decode_string(rest)
    {name, rest} = decode_string(rest)
    {type, rest} = decode_type(rest)
    entry = {keyspace_name, table_name, name, type}
    decode_column_specs(rest, column_count - 1, nil, [entry | acc])
  end

  defp decode_column_specs(rest, column_count, global_tables_spec, acc) do
    {keyspace_name, table_name} = global_tables_spec
    {name, rest} = decode_string(rest)
    {type, rest} = decode_type(rest)
    entry = {keyspace_name, table_name, name, type}
    decode_column_specs(rest, column_count - 1, global_tables_spec, [entry | acc])
  end

  defp decode_type(<<0x0000::16>> <> rest) do
    {name, rest} = decode_string(rest)
    {{:custom, name}, rest}
  end

  defp decode_type(<<0x0001::16>> <> rest) do
    {:ascii, rest}
  end

  defp decode_type(<<0x0002::16>> <> rest) do
    {:bigint, rest}
  end

  defp decode_type(<<0x0003::16>> <> rest) do
    {:blob, rest}
  end

  defp decode_type(<<0x0004::16>> <> rest) do
    {:boolean, rest}
  end

  defp decode_type(<<0x0005::16>> <> rest) do
    {:counter, rest}
  end

  defp decode_type(<<0x0006::16>> <> rest) do
    {:decimal, rest}
  end

  defp decode_type(<<0x0007::16>> <> rest) do
    {:double, rest}
  end

  defp decode_type(<<0x0008::16>> <> rest) do
    {:float, rest}
  end

  defp decode_type(<<0x0009::16>> <> rest) do
    {:int, rest}
  end

  defp decode_type(<<0x000B::16>> <> rest) do
    {:timestamp, rest}
  end

  defp decode_type(<<0x000C::16>> <> rest) do
    {:uuid, rest}
  end

  defp decode_type(<<0x000D::16>> <> rest) do
    {:varchar, rest}
  end

  defp decode_type(<<0x000E::16>> <> rest) do
    {:varint, rest}
  end

  defp decode_type(<<0x000F::16>> <> rest) do
    {:timeuuid, rest}
  end

  defp decode_type(<<0x0010::16>> <> rest) do
    {:inet, rest}
  end

  defp decode_type(<<0x0020::16>> <> rest) do
    {type, rest} = decode_type(rest)
    {{:list, type}, rest}
  end

  defp decode_type(<<0x0021::16>> <> rest) do
    {key_type, rest} = decode_type(rest)
    {value_type, rest} = decode_type(rest)
    {{:map, key_type, value_type}, rest}
  end

  defp decode_type(<<0x0022::16>> <> rest) do
    {type, rest} = decode_type(rest)
    {{:set, type}, rest}
  end

  # TODO: UDT

  defp decode_string_multimap(<<size::16>> <> rest) do
    decode_string_multimap(rest, size, %{})
  end

  defp decode_string_multimap(rest, 0, acc) do
    {acc, rest}
  end

  defp decode_string_multimap(rest, size, acc) do
    {key, rest} = decode_string(rest)
    {value, rest} = decode_string_list(rest)
    decode_string_multimap(rest, size - 1, Map.put(acc, key, value))
  end

  defp decode_string(<<size::16, content::size(size)-bytes>> <> rest) do
    {content, rest}
  end

  defp decode_string_list(<<size::16>> <> rest) do
    decode_string_list(rest, size, [])
  end

  defp decode_string_list(rest, 0, acc) do
    {Enum.reverse(acc), rest}
  end

  defp decode_string_list(rest, size, acc) do
    {elem, rest} = decode_string(rest)
    decode_string_list(rest, size - 1, [elem | acc])
  end
end
