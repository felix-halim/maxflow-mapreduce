package mf;

public enum MFCounter {
	N,						// Total number of vertices
	E,						// Total number of edges
	ACCEPTED_FLOWS,
	ACCEPTED_AUGPATHS,
	AUGPATH_CANDIDATES,
	SOURCE_E_DROPPED,
	SINK_E_DROPPED,
	SOURCE_E_TRUNCATED,
	SINK_E_TRUNCATED,
	SOURCE_E_SATURATED,
	SINK_E_SATURATED,
	EXTEND_SOURCE_E,
	EXTEND_SINK_E,
	SOURCE_EPATH_COUNT,
	SOURCE_EPATH_TOTAL,
	SINK_EPATH_COUNT,
	SINK_EPATH_TOTAL,
	SOURCE_MOVE,			// how many source-nodes becomes "alive"
	SINK_MOVE,				// how many sink-nodes becomes "alive"
	BROADCAST,
	LOSE_EXCESS,

	TIME_MAPPER,
	TIME_COMBINER,
	TIME_REDUCER,
	TIME_MERGE_NODES,
	TIME_AUGMENTING,
	TIME_OPEN_HDFS,

	// Push-Relabel Counters
	OVERFLOW_TOTAL,
	OVERFLOW_COUNT,
	SINK_EXCESS,
	MEET_IN_THE_MIDDLE,
	VERSION,
	VERSION_COUNT,
	TIME_PUSH,
	TIME_RELABEL,
	TIME_DISCHARGE,

	// Ford-Fulkerson Counters
	TIME_MEET_IN_THE_MIDDLE,
	TIME_EXTEND_SOURCE_E,
	TIME_EXTEND_SINK_E,
	TIME_UPDATE_EDGES	// how long to change the edges when there are shared edges with aug edges
}
