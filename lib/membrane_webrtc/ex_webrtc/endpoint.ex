defmodule Membrane.WebRTC.ExWebRTCEndpoint do
  @moduledoc false

  use Membrane.Endpoint

  require Membrane.Logger

  alias Membrane.Pad
  alias ExRTCP.Packet.PayloadFeedback.PLI
  alias ExWebRTC.{ICECandidate, PeerConnection, SessionDescription}
  alias Membrane.WebRTC.{ExWebRTCUtils, Signaling, SimpleWebSocketServer, WhipServer}

  def_options signaling: [],
              allowed_video_codecs: [],
              preferred_video_codec: [],
              ice_servers: [],
              ice_port_range: [],
              ice_ip_filter: [],
              keyframe_interval: [],
              sdp_candidates_timeout: []

  def_input_pad :input,
    accepted_format: Membrane.RTP,
    availability: :on_request,
    options: [kind: [], codec: [default: nil]]

  def_output_pad :output,
    accepted_format: Membrane.RTP,
    availability: :on_request,
    flow_control: :push,
    options: [kind: [default: nil]]

  @max_rtp_timestamp 2 ** 32 - 1
  @max_rtp_seq_num 2 ** 16 - 1
  @keyframe_request_throttle_time Membrane.Time.milliseconds(500)

  defmodule State do
    @moduledoc false

    @type output_track :: %{
            status: :awaiting | :connected,
            pad: Membrane.Pad.ref() | nil,
            params: %{
              kind: :audio | :video,
              clock_rate: non_neg_integer(),
              seq_num: non_neg_integer(),
              last_keyframe_request_ts: Membrane.Time.t()
            },
            track: ExWebRTC.MediaStreamTrack.t(),
            first_packet_received: boolean()
          }

    @type t :: %__MODULE__{
            pc: pid() | nil,
            output_tracks: %{(track_or_pad_id :: term()) => output_track()},
            awaiting_outputs: [{:video | :audio, Membrane.Pad.ref()}],
            awaiting_candidates: [ExWebRTC.ICECandidate.t()],
            signaling: Signaling.t() | {:websocket, SimpleWebSocketServer.options()},
            status: :init | :connecting | :connected | :closed,
            audio_params: [ExWebRTC.RTPCodecParameters.t()],
            video_params: [ExWebRTC.RTPCodecParameters.t()],
            allowed_video_codecs: [:h264 | :vp8],
            preferred_video_codec: :h264 | :vp8,
            ice_servers: [ExWebRTC.PeerConnection.Configuration.ice_server()],
            ice_port_range: Enumerable.t(non_neg_integer()),
            ice_ip_filter: (:inet.ip_address() -> boolean()),
            keyframe_interval: Membrane.Time.t() | nil,
            sdp_candidates_timeout: Membrane.Time.t() | nil
          }

    @enforce_keys [
      :signaling,
      :audio_params,
      :allowed_video_codecs,
      :preferred_video_codec,
      :ice_servers,
      :ice_port_range,
      :ice_ip_filter,
      :keyframe_interval,
      :sdp_candidates_timeout
    ]

    defstruct @enforce_keys ++
                [
                  video_params: nil,
                  pc: nil,
                  output_tracks: %{},
                  awaiting_outputs: [],
                  awaiting_candidates: [],
                  status: :init,
                  candidates_in_sdp: false
                ]
  end

  @impl true
  def handle_init(_ctx, opts) do
    {[],
     %State{
       signaling: opts.signaling,
       audio_params: ExWebRTCUtils.codec_params(:opus),
       allowed_video_codecs: opts.allowed_video_codecs |> Enum.uniq(),
       preferred_video_codec: opts.preferred_video_codec,
       ice_servers: opts.ice_servers,
       ice_port_range: opts.ice_port_range,
       ice_ip_filter: opts.ice_ip_filter,
       keyframe_interval: opts.keyframe_interval,
       sdp_candidates_timeout: opts.sdp_candidates_timeout
     }}
  end

  @impl true
  def handle_setup(ctx, state) do
    signaling =
      case state.signaling do
        {:whip, opts} ->
          setup_whip(ctx, opts)

        {:websocket, opts} ->
          SimpleWebSocketServer.start_link_supervised(ctx.utility_supervisor, opts)

        signaling ->
          signaling
      end

    {[], %{state | signaling: signaling}}
  end

  @impl true
  def handle_playing(_ctx, state) do
    Process.monitor(state.signaling.pid)
    Signaling.register_element(state.signaling)

    {[], %{state | status: :connecting}}
  end

  @impl true
  def handle_pad_added(Pad.ref(:input, _id), _ctx, state) do
    {[], state}
  end

  def handle_pad_added(Pad.ref(:output, _id) = pad, %{playback: :stopped} = ctx, state) do
    %{kind: kind} = ctx.pad_options
    state = %{state | awaiting_outputs: state.awaiting_outputs ++ [{kind, pad}]}
    {[], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(:output, track_id) = pad, _ctx, state) do
    state =
      state
      |> Bunch.Struct.update_in([:output_tracks, track_id], fn output_track ->
        %{output_track | status: :connected, pad: pad}
      end)
      |> maybe_answer()

    {[stream_format: {pad, %Membrane.RTP{}}], state}
  end

  @impl true
  def handle_event(pad, event, _ctx, state) do
    Membrane.Logger.debug("Ignoring event #{inspect(event)} that arrived on pad #{inspect(pad)}")
    {[], state}
  end

  @impl true
  def handle_buffer(Pad.ref(:input, _) = pad, buffer, %{pads: pads} = ctx, state) do
    kind = pads[pad].options.kind
    case get_output_track_by_kind(kind, state.output_tracks) do
        {_, output_track} ->
          {[], send_buffer(output_track, buffer, state)}
        nil ->
          Membrane.Logger.warning("Dropping packet that arrived on pad #{inspect(pad)} with kind #{inspect(kind)} that has no corresponding output track: #{inspect(state.output_tracks)}")
          {[], state}
    end
  end

  defp get_output_track_by_kind(kind, output_tracks) do
    state.output_tracks
    |> Enum.find(fn {_id, %{params: %{kind: ^kind}}} -> true; _ -> false end)
  end

  @impl true
  def handle_info({:ex_webrtc, _from, _msg}, _ctx, %{status: :closed} = state) do
    {[], state}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {:rtcp, rtcp_packets}}, _ctx, state) do
    time = Membrane.Time.monotonic_time()

    output_tracks =
      rtcp_packets
      |> Enum.filter(fn
        {_track_id, %PLI{} = packet} ->
          Membrane.Logger.debug("Keyframe request received: #{inspect(packet)}")
          true

        packet ->
          Membrane.Logger.debug_verbose("Ignoring RTCP packet: #{inspect(packet)}")
          false
      end)
      |> Enum.reduce(state.output_tracks, fn {track_id, _pli}, output_tracks ->
        %{params: params} = output_tracks[track_id]

        if params.kind == :video and
             time - params.last_keyframe_request_ts > @keyframe_request_throttle_time do
          Membrane.Logger.debug("Sending PLI on track #{inspect(track_id)}")
          :ok = PeerConnection.send_pli(state.pc, track_id)
          put_in(output_tracks[track_id][:params], %{params | last_keyframe_request_ts: time})
        else
          output_tracks
        end
      end)

    {[], %{state | output_tracks: output_tracks}}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {:rtp, id, _rid, packet}}, _ctx, state) do
    buffer = %Membrane.Buffer{
      payload: packet.payload,
      metadata: %{rtp: packet |> Map.from_struct() |> Map.delete(:payload)}
    }

    case state.output_tracks[id] do
      %{status: :awaiting, track: track} ->
        Membrane.Logger.warning("""
        Dropping packet of track #{inspect(id)}, kind #{inspect(track.kind)} \
        that arrived before the SDP answer was sent.
        """)

        {[], state}

      %{status: :connected, pad: pad, track: %{kind: kind}, first_packet_received: false} ->
        timer_action =
          if kind == :video and state.keyframe_interval != nil do
            [start_timer: {{:request_keyframe, id}, state.keyframe_interval}]
          else
            []
          end

        state =
          Bunch.Struct.update_in(state, [:output_tracks, id], fn output_track ->
            %{output_track | first_packet_received: true}
          end)

        {[buffer: {pad, buffer}] ++ timer_action, state}

      %{status: :connected, pad: pad} ->
        {[buffer: {pad, buffer}], state}
    end
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {:ice_candidate, candidate}}, _ctx, state) do
    Signaling.signal(state.signaling, candidate)
    {[], state}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, {:connection_state_change, :connected}}, _ctx, state) do
    {[], %{state | status: :connected}}
  end

  @impl true
  def handle_info({:ex_webrtc, _from, message}, _ctx, state) do
    Membrane.Logger.debug("Ignoring ex_webrtc message: #{inspect(message)}")
    {[], state}
  end

  @impl true
  def handle_info(
        {:membrane_webrtc_signaling, _pid, %SessionDescription{type: :offer} = sdp, metadata},
        _ctx,
        state
      ) do
    Membrane.Logger.debug("Received SDP offer")

    {codecs_notification, state} = ensure_peer_connection_started(sdp, state)
    :ok = PeerConnection.set_remote_description(state.pc, sdp)

    {new_tracks, awaiting_outputs} =
      receive_new_tracks()
      |> Enum.map_reduce(state.awaiting_outputs, fn track, awaiting_outputs ->
        params = mk_track_params(track)
        case List.keytake(awaiting_outputs, track.kind, 0) do
          nil ->
            {{track.id,
              %{status: :awaiting, track: track, pad: nil, params: params, first_packet_received: false}},
             awaiting_outputs}

          {{_kind, pad}, awaiting_outputs} ->
            {{track.id,
              %{status: :connected, track: track, pad: pad, params: params, first_packet_received: false}},
             awaiting_outputs}
        end
      end)

    output_tracks = Map.merge(state.output_tracks, Map.new(new_tracks))

    state =
      %{
        state
        | awaiting_outputs: awaiting_outputs,
          output_tracks: output_tracks,
          candidates_in_sdp: state.candidates_in_sdp or metadata[:candidates_in_sdp] == true
      }
      |> maybe_answer()

    tracks_notification =
      Enum.flat_map(new_tracks, fn
        {_id, %{status: :awaiting, track: track}} -> [track]
        _connected_track -> []
      end)
      |> case do
        [] -> []
        tracks -> [notify_parent: {:new_tracks, tracks}]
      end

    stream_formats =
      Enum.flat_map(new_tracks, fn
        {_id, %{status: :connected, pad: pad}} ->
          [stream_format: {pad, %Membrane.RTP{}}]

        _other ->
          []
      end)

    {codecs_notification ++ tracks_notification ++ stream_formats, state}
  end

  def handle_info(
        {:membrane_webrtc_signaling, _pid, %SessionDescription{type: :answer} = _sdp, _metadata},
        _ctx,
        _state
      ) do
    raise "WebRTC endpoint received SDP Answer, while it only expects Offer and sends Answer"
  end

  @impl true
  def handle_info(
        {:membrane_webrtc_signaling, _pid, %ICECandidate{} = candidate, _metadata},
        _ctx,
        state
      ) do
    case PeerConnection.add_ice_candidate(state.pc, candidate) do
      :ok ->
        {[], state}

      # Workaround for a bug in ex_webrtc that should be fixed in 0.2.0
      {:error, :no_remote_description} ->
        {[], %{state | awaiting_candidates: [candidate | state.awaiting_candidates]}}
    end
  end

  @impl true
  def handle_info(
        {:DOWN, _monitor, :process, signaling_pid, _reason},
        ctx,
        %{signaling: %{pid: signaling_pid}} = state
      ) do
    handle_close(ctx, state)
  end

  @impl true
  def handle_info(
        {:DOWN, _monitor, :process, pc, _reason},
        ctx,
        %{pc: pc} = state
      ) do
    handle_close(ctx, state)
  end

  @impl true
  def handle_tick({:request_keyframe, track_id}, _ctx, state) do
    :ok = PeerConnection.send_pli(state.pc, track_id)
    {[], state}
  end

  defp ensure_peer_connection_started(sdp, %{pc: nil} = state) do
    video_codecs_in_sdp = ExWebRTCUtils.get_video_codecs_from_sdp(sdp)

    negotiated_video_codecs =
      state.allowed_video_codecs
      |> Enum.filter(&(&1 in video_codecs_in_sdp))
      |> case do
        [] -> []
        [codec] -> [codec]
        _both -> [state.preferred_video_codec]
      end

    video_params = ExWebRTCUtils.codec_params(negotiated_video_codecs)

    {:ok, pc} =
      PeerConnection.start(
        ice_servers: state.ice_servers,
        ice_port_range: state.ice_port_range,
        ice_ip_filter: state.ice_ip_filter,
        video_codecs: video_params,
        audio_codecs: state.audio_params
      )

    Process.monitor(pc)

    notify_parent = [notify_parent: {:negotiated_video_codecs, negotiated_video_codecs}]
    {notify_parent, %{state | pc: pc, video_params: video_params}}
  end

  defp ensure_peer_connection_started(_sdp, state), do: {[], state}

  defp maybe_answer(state) do
    if Enum.all?(state.output_tracks, fn {_id, %{status: status}} -> status == :connected end) do
      %{pc: pc} = state
      {:ok, answer} = PeerConnection.create_answer(pc)
      :ok = PeerConnection.set_local_description(pc, answer)

      state.awaiting_candidates
      |> Enum.reverse()
      |> Enum.each(&(:ok = PeerConnection.add_ice_candidate(pc, &1)))

      answer =
        if state.candidates_in_sdp do
          receive do
            {:ex_webrtc, ^pc, {:ice_gathering_state_change, :complete}} -> :ok
          after
            Membrane.Time.as_milliseconds(state.sdp_candidates_timeout, :round) -> :ok
          end

          PeerConnection.get_local_description(pc)
        else
          answer
        end

      Signaling.signal(state.signaling, answer)
      %{state | awaiting_candidates: [], candidates_in_sdp: false}
    else
      state
    end
  end

  @spec receive_new_tracks() :: [ExWebRTC.MediaStreamTrack.t()]
  defp receive_new_tracks(), do: do_receive_new_tracks([])

  defp do_receive_new_tracks(acc) do
    receive do
      {:ex_webrtc, _pc, {:track, track}} -> do_receive_new_tracks([track | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp handle_close(_ctx, %{status: :connecting}) do
    raise "Connection failed"
  end

  defp handle_close(%{playback: :playing} = ctx, %{status: status} = state)
       when status != :closed do
    actions =
      ctx.pads
      |> Map.values()
      |> Enum.filter(&(&1.direction == :output))
      |> Enum.map(&{:end_of_stream, &1.ref})

    {actions, %{state | status: :closed}}
  end

  defp handle_close(_ctx, state) do
    {[], %{state | status: :closed}}
  end

  defp setup_whip(ctx, opts) do
    signaling = Signaling.new()
    clients_cnt = :atomics.new(1, [])
    {token, opts} = Keyword.pop(opts, :token, fn _token -> true end)
    validate_token = if is_function(token), do: token, else: &(&1 == token)

    handle_new_client = fn token ->
      cond do
        !validate_token.(token) -> {:error, :invalid_token}
        :atomics.add_get(clients_cnt, 1, 1) > 1 -> {:error, :already_connected}
        true -> {:ok, signaling}
      end
    end

    Membrane.UtilitySupervisor.start_child(ctx.utility_supervisor, {
      WhipServer,
      [handle_new_client: handle_new_client] ++ opts
    })

    signaling
  end

  defp send_buffer(output_track, buffer, state) do
    %{params: params, track: %{id: track_id}} = output_track
    timestamp =
      Membrane.Time.divide_by_timebase(
        buffer.pts,
        Ratio.new(Membrane.Time.second(), params.clock_rate)
      )
      |> rem(@max_rtp_timestamp + 1)

    packet =
      ExRTP.Packet.new(buffer.payload,
        timestamp: timestamp,
        marker: buffer.metadata[:rtp][:marker] || false,
        sequence_number: params.seq_num
      )

    PeerConnection.send_rtp(state.pc, track_id, packet)
    seq_num = rem(params.seq_num + 1, @max_rtp_seq_num + 1)
    put_in(state[:output_tracks][track_id][:params], %{params | seq_num: seq_num})
  end

  defp mk_track_params(track) do
    %{
      kind: track.kind,
      clock_rate:
        case track.kind do
          :audio -> ExWebRTCUtils.codec_clock_rate(:opus)
          :video -> 90_000
        end,
      seq_num: Enum.random(0..@max_rtp_seq_num),
      last_keyframe_request_ts: Membrane.Time.monotonic_time() - @keyframe_request_throttle_time
    }
  end
end
