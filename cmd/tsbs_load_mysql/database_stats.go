package main

type ReplicationStats struct {
	ReplicaName string `db:"application_name"`
	ReplayLag   string `db:"replay_lag"`
}

func (rs ReplicationStats) ToSlice() []string {
	return []string{rs.ReplicaName, rs.ReplayLag}
}

