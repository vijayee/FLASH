// DataChannel message types, exactly per spec §7.1. Field names must stay
// byte-compatible with the Dart implementation.
export const MESSAGE_TYPES = {
  // RTT measurement
  PING: 'ping',
  PONG: 'pong',

  // Gossip
  GOSSIP: 'gossip',

  // Query routing
  QUERY_FORWARD: 'query_forward',
  LEADER_QUERY_FORWARD: 'leader_query_forward',
  CONSTRAINT_QUERY_FORWARD: 'constraint_query_forward',
  PROBE_REQUEST: 'probe_request',
  PROBE_RESULT: 'probe_result',
  PROBE_REQUEST_AVG: 'probe_request_avg',
  PROBE_RESULT_AVG: 'probe_result_avg',
  PROBE_REQUEST_CONSTRAINTS: 'probe_request_constraints',
  PROBE_RESULT_CONSTRAINTS: 'probe_result_constraints',
  QUERY_RESULT: 'query_result',

  // Media
  MEDIA_OFFER: 'media_offer',
  MEDIA_ANSWER: 'media_answer',
  FORWARDED_STREAM: 'forwarded_stream',
  MEDIA_CLOSE: 'media_close',

  // Supernode / Raft
  SUPERNODE_ELECTED: 'supernode_elected',
  RAFT_APPEND_ENTRIES: 'raft_append_entries',
  RAFT_APPEND_ENTRIES_RESPONSE: 'raft_append_entries_response',
  RAFT_REQUEST_VOTE: 'raft_request_vote',
  RAFT_REQUEST_VOTE_RESPONSE: 'raft_request_vote_response',

  // Peer management
  PEER_LEAVING: 'peer_leaving',
  PEER_STATUS: 'peer_status'
};