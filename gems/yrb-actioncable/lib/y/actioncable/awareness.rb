# frozen_string_literal: true

module Y
  module Actioncable
    # Server-side awareness encoding utilities.
    #
    # Awareness in Y.js tracks ephemeral state like cursor position, user name,
    # and online status. This module provides helpers to encode awareness updates
    # from the server side (e.g. for an AI agent cursor) and broadcast them
    # through ActionCable channels.
    #
    # The awareness protocol format (from y-protocols/awareness):
    #   - varint: number of client updates
    #   - for each client:
    #     - varint: client_id
    #     - varint: clock (monotonically increasing version)
    #     - var-length string: JSON-encoded state (or empty string for removal)
    #
    # @example Broadcast AI agent presence
    #   awareness_data = Y::Actioncable::Awareness.encode(
    #     client_id: 1,
    #     state: { user: { name: "Sagaly AI", color: "#8b5cf6" }, cursor: nil }
    #   )
    #   Blog::PostChannel.broadcast_to(post, 
    #     Y::Actioncable::Sync.broadcast_awareness(post, awareness_data, origin: "ai-agent")
    #   )
    module Awareness
      module_function

      # Encode an awareness update for a single client.
      #
      # @param client_id [Integer] Unique client identifier
      # @param clock [Integer] Monotonically increasing version counter
      # @param state [Hash, nil] The awareness state (user info, cursor position, etc.)
      #   Pass nil or empty hash to signal client removal.
      # @return [Array<Integer>] The encoded awareness update bytes
      #
      # @example Encode presence with cursor
      #   Y::Actioncable::Awareness.encode(
      #     client_id: 42,
      #     state: {
      #       user: { name: "AI Assistant", color: "#8b5cf6" },
      #       cursor: { anchor: { type: "content", offset: 5 }, head: { type: "content", offset: 5 } }
      #     }
      #   )
      def encode(client_id:, clock: 1, state: nil)
        encoder = Y::Lib0::Encoding.create_encoder

        # Number of updated clients
        Y::Lib0::Encoding.write_var_uint(encoder, 1)

        # Client ID
        Y::Lib0::Encoding.write_var_uint(encoder, client_id)

        # Clock
        Y::Lib0::Encoding.write_var_uint(encoder, clock)

        # State as JSON string (empty string = client removed)
        state_json = state ? state.to_json : "null"
        state_bytes = state_json.encode("UTF-8").bytes
        Y::Lib0::Encoding.write_var_uint8_array(encoder, state_bytes)

        Y::Lib0::Encoding.to_uint8_array(encoder)
      end

      # Encode a removal update for a client (signals disconnect).
      #
      # @param client_id [Integer] The client to remove
      # @param clock [Integer] Must be >= the client's last known clock
      # @return [Array<Integer>] The encoded awareness removal bytes
      def encode_removal(client_id:, clock: 1)
        encode(client_id: client_id, clock: clock, state: nil)
      end

      # Broadcast an awareness update through an ActionCable channel.
      #
      # @param channel_class [Class] The ActionCable channel class (e.g. Blog::PostChannel)
      # @param to [Object] The model to broadcast to
      # @param awareness_data [Array<Integer>] The encoded awareness bytes
      # @param origin [String] Origin identifier to prevent echo
      def broadcast(channel_class, to, awareness_data, origin: "server")
        encoder = Y::Lib0::Encoding.create_encoder
        Y::Lib0::Encoding.write_var_uint(encoder, 1) # MESSAGE_AWARENESS
        Y::Lib0::Encoding.write_var_uint8_array(encoder, awareness_data)
        update = Y::Lib0::Encoding.to_uint8_array(encoder)
        encoded = Y::Lib0::Encoding.encode_uint8_array_to_base64(update)

        channel_class.broadcast_to(to, {
          "update" => encoded,
          "origin" => origin
        })
      end
    end
  end
end
