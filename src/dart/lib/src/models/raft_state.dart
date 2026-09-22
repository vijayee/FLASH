/// Raft consensus role of this node within the supernode cluster.
enum RaftState { follower, candidate, leader }

/// One replicated command in the Raft log. Log indices are 0-based.
class RaftLogEntry {
  final int term;
  final int index;
  final Map<String, dynamic> command;

  const RaftLogEntry({
    required this.term,
    required this.index,
    required this.command,
  });
}

/// Raft cluster state for the supernode control plane (spec §4.6).
///
/// Invariants: log/array indices are 0-based and "nothing yet" is -1, so
/// [commitIndex], [lastApplied] and every [matchIndex] entry start at -1
/// (0 would pin the first entry out of reach), while every [nextIndex] entry
/// starts at 0 — the start of the (empty) log.
class SupernodeCluster {
  final String clusterId;
  List<String> members;
  String? leaderId;

  // Persistent state.
  int currentTerm;
  String? votedFor;
  List<RaftLogEntry> log;

  // Volatile state.
  int commitIndex;
  int lastApplied;

  // Leader state, per follower.
  final Map<String, int> nextIndex;
  final Map<String, int> matchIndex;

  // Timers.
  Duration electionTimeout;
  Duration heartbeatInterval;

  // Internal.
  RaftState raftState;

  SupernodeCluster({
    required this.clusterId,
    required this.members,
    this.leaderId,
    this.currentTerm = 0,
    this.votedFor,
    List<RaftLogEntry>? log,
    this.commitIndex = -1,
    this.lastApplied = -1,
    this.raftState = RaftState.follower,
    Duration? electionTimeout,
    this.heartbeatInterval = const Duration(milliseconds: 50),
  })  : log = log ?? [],
        nextIndex = {},
        matchIndex = {},
        electionTimeout = electionTimeout ??
            Duration(
              milliseconds: 150 + (DateTime.now().millisecondsSinceEpoch % 150),
            ) {
    // Replication state starts before the first log entry: nothing sent
    // (nextIndex 0 = start of log), nothing replicated (matchIndex -1).
    for (final member in members) {
      nextIndex[member] = 0;
      matchIndex[member] = -1;
    }
  }
}
