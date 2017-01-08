defmodule Xandra.Query do
  defstruct [:statement, :values, :id, :bound_columns, :result_columns]

  defimpl DBConnection.Query do
    alias Xandra.{Frame, Protocol}

    def parse(query, _opts) do
      query
    end

    def encode(query, values, opts) do
      query = %{query | values: values}
      kind = if query.id, do: :execute, else: :query
      Frame.new(kind)
      |> Protocol.encode_request(query, opts)
      |> Frame.encode()
    end

    def decode(query, %Frame{} = frame, _opts) do
      Protocol.decode_response(frame, query)
    end

    def describe(query, _opts) do
      query
    end
  end
end
