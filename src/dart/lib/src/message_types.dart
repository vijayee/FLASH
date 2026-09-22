/// DataChannel message-type vocabulary, exactly matching the JavaScript
/// implementation's `MESSAGE_TYPES` (src/js/src/message-types.js).
///
/// String values must stay byte-compatible across the Dart and JS
/// implementations for wire-level interop.
abstract final class MeridianMessageTypes {
  // RTT measurement.
  static const String ping = 'ping';
  static const String pong = 'pong';

  // Gossip.
  static const String gossip = 'gossip';

  // Query routing.
  static const String queryForward = 'query_forward';
  static const String leaderQueryForward = 'leader_query_forward';
  static const String constraintQueryForward = 'constraint_query_forward';
  static const String probeRequest = 'probe_request';
  static const String probeResult = 'probe_result';
  static const String probeRequestAvg = 'probe_request_avg';
  static const String probeResultAvg = 'probe_result_avg';
  static const String probeRequestConstraints = 'probe_request_constraints';
  static const String probeResultConstraints = 'probe_result_constraints';
  static const String queryResult = 'query_result';

  // Media.
  static const String mediaOffer = 'media_offer';
  static const String mediaAnswer = 'media_answer';
  static const String forwardedStream = 'forwarded_stream';
  static const String mediaClose = 'media_close';

  // Supernode / Raft.
  static const String supernodeElected = 'supernode_elected';
  static const String raftAppendEntries = 'raft_append_entries';
  static const String raftAppendEntriesResponse =
      'raft_append_entries_response';
  static const String raftRequestVote = 'raft_request_vote';
  static const String raftRequestVoteResponse = 'raft_request_vote_response';

  // Peer management.
  static const String peerLeaving = 'peer_leaving';
  static const String peerStatus = 'peer_status';

  /// Every message type in the shared wire vocabulary.
  static const Set<String> all = {
    ping,
    pong,
    gossip,
    queryForward,
    leaderQueryForward,
    constraintQueryForward,
    probeRequest,
    probeResult,
    probeRequestAvg,
    probeResultAvg,
    probeRequestConstraints,
    probeResultConstraints,
    queryResult,
    mediaOffer,
    mediaAnswer,
    forwardedStream,
    mediaClose,
    supernodeElected,
    raftAppendEntries,
    raftAppendEntriesResponse,
    raftRequestVote,
    raftRequestVoteResponse,
    peerLeaving,
    peerStatus,
  };
}
