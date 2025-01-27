defmodule Xandra.OptionsValidators do
  # Validator functions used by nimble_options schema definitions all throughout Xandra.
  @moduledoc false

  alias Xandra.Cluster.Host

  @spec validate_module(term(), String.t()) :: {:ok, module()} | {:error, String.t()}
  def validate_module(value, name) when is_binary(name) do
    cond do
      is_atom(value) and Code.ensure_loaded?(value) ->
        {:ok, value}

      is_atom(value) ->
        {:error, "#{name} module #{inspect(value)} is not loaded"}

      true ->
        {:error, "expected #{name} module to be a module, got: #{inspect(value)}"}
    end
  end

  @spec validate_authentication(term()) :: {:ok, module()} | {:error, String.t()}
  def validate_authentication({module, keyword} = value)
      when is_atom(module) and is_list(keyword) do
    with {:ok, _module} <- validate_module(module, "authentication"), do: {:ok, value}
  end

  def validate_authentication(other) do
    {:error, "expected :authentication to be a {module, options} tuple, got: #{inspect(other)}"}
  end

  @spec validate_node(term()) :: {:ok, {String.t(), integer()}} | {:error, String.t()}
  def validate_node(value) when is_binary(value) do
    # Remove surrounding square brackets from IPv6 values.
    value = value |> String.replace("[", "") |> String.replace("]", "")

    {address, port} =
      case String.split(value, ":") do
        # FQDN or IPv4.
        [address] ->
          {address, "9042"}

        # FQDN:PORT or IPv4:PORT. IPv6 addresses have to have at least 2 colons.
        [address, port] ->
          {address, port}

        # IPv6.
        splits ->
          # Our IPv6 address can either parse as a valid address or have the
          # additional port suffix.
          with {:ok, {_, _, _, _, _, _, _, _}} <-
                 :inet.parse_address(value |> String.to_charlist()) do
            {value, "9042"}
          else
            _ -> {splits |> Enum.drop(-1) |> Enum.join(":"), List.last(splits)}
          end
      end

    case Integer.parse(port) do
      {port, ""} when port in 0..65535 -> {:ok, {address, port}}
      {port, ""} -> {:error, "invalid port (outside of the 0..65535 range): #{port}"}
      _ -> {:error, "invalid node: #{inspect(value)}"}
    end
  end

  def validate_node(other) do
    {:error, "expected node to be a \"<host>:<port>\" string, got: #{inspect(other)}"}
  end

  @spec validate_contact_node(term()) :: {:ok, Host.t()} | {:error, String.t()}
  def validate_contact_node(address_and_port)

  def validate_contact_node({ip, port}) when is_tuple(ip) do
    case :inet.ntoa(ip) do
      {:error, :einval} ->
        {:error,
         "expected address in contact node tuple to be a valid IPv4 or IPv6 tuple, got: #{inspect(ip)}"}

      _string_address ->
        {:ok, {ip, port}}
    end
  end

  def validate_contact_node({hostname, port}) when is_binary(hostname) do
    case hostname |> String.to_charlist() |> :inet.parse_address() do
      {:ok, ip} -> {:ok, {ip, port}}
      {:error, :einval} -> {:ok, {hostname, port}}
    end
  end

  def validate_contact_node({other, _port}) do
    {:error,
     "expected address in Host to be an IP tuple or a hostname string, got: #{inspect(other)}"}
  end

  def validate_contact_node(other) do
    {:error, "expected :contact_node to be a {address_or_ip, port} tuple, got: #{inspect(other)}"}
  end

  @spec validate_binary(term(), atom()) :: {:ok, binary()} | {:error, String.t()}
  def validate_binary(value, key) when is_atom(key) do
    if is_binary(value) do
      {:ok, value}
    else
      {:error, "expected #{inspect(key)} to be a binary, got: #{inspect(value)}"}
    end
  end

  @spec validate_custom_payload(term()) :: {:ok, Xandra.custom_payload()} | {:error, String.t()}
  def validate_custom_payload(term) do
    if is_map(term) and Enum.all?(term, fn {key, val} -> is_binary(key) and is_binary(val) end) do
      {:ok, term}
    else
      {:error, "expected custom payload to be a map of string to binary, got: #{inspect(term)}"}
    end
  end
end
