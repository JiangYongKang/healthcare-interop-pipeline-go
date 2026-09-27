package pipeline

import (
	"context"
	"fmt"
	"sort"
)

// ReplayReport summarizes one consistency replay: the chain state was
// rebuilt from the append-only journals and compared against the live
// in-memory state. Reprocessing history must yield the same effective
// result as incremental processing.
type ReplayReport struct {
	Seq            int64    `json:"seq"`
	Chains         int      `json:"chains"`
	Deliveries     int      `json:"deliveries"`
	Conflicts      int      `json:"conflicts"`
	PendingReviews int      `json:"pendingReviews"`
	Consistent     bool     `json:"consistent"`
	Mismatches     []string `json:"mismatches,omitempty"`
}

// buildReplayActions pairs committed revisions with their decisions and
// merges review conclusions by sequence, so replayed history
// interleaves exactly as it happened live. It also returns the end
// offset of the last committed revision line (for orphan healing).
func buildReplayActions(j *journalData) (actions []replayAction, lastCommittedRevOff int64) {
	commitDecs := map[string][]*Decision{}
	for _, d := range j.decisions {
		if committingOutcome(d.Outcome) {
			commitDecs[d.EventID] = append(commitDecs[d.EventID], d)
		}
	}
	applied := map[string]int{}
	for i, rec := range j.revisions {
		id := rec.EventID
		if applied[id] >= len(commitDecs[id]) {
			continue // orphan revision (crash before its decision)
		}
		dec := commitDecs[id][applied[id]]
		applied[id]++
		actions = append(actions, replayAction{seq: dec.Seq, rev: rec, dec: dec})
		lastCommittedRevOff = j.revOffsets[i]
	}
	for _, rw := range j.reviews {
		actions = append(actions, replayAction{seq: rw.Seq, rw: rw})
	}
	sort.SliceStable(actions, func(i, k int) bool { return actions[i].seq < actions[k].seq })
	return actions, lastCommittedRevOff
}

// applyReplayActions folds ordered history into a fresh model.
func applyReplayActions(m *chainModel, actions []replayAction) {
	for _, a := range actions {
		if a.rw != nil {
			m.applyReview(a.rw)
		} else {
			m.applyCommitted(a.rev, a.dec.Seq, a.dec.Outcome, a.dec.Reason)
		}
	}
}

// compareModels diffs the live model against a rebuilt one. The
// effective version of every chain and every conflict's status must
// match exactly; deliveries are reconstructed from the same journals
// and cannot diverge.
func compareModels(live, rebuilt *chainModel) *ReplayReport {
	rep := &ReplayReport{Consistent: true}
	rep.Chains = len(rebuilt.chains)
	for _, c := range rebuilt.chains {
		rep.Deliveries += len(c.revisions)
	}
	rep.Conflicts = len(rebuilt.conflicts)
	rep.PendingReviews = rebuilt.pending

	effectiveDigest := func(c *chainState) string {
		if c == nil || c.effective == nil {
			return ""
		}
		return businessDigest(c.effective)
	}
	for id, lc := range live.chains {
		rc := rebuilt.chains[id]
		if rc == nil {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("chain %s missing in replay", lc.id))
			continue
		}
		if ld, rd := effectiveDigest(lc), effectiveDigest(rc); ld != rd {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("chain %s effective digest live=%s replay=%s", lc.id, shortDigest(ld), shortDigest(rd)))
		}
	}
	for id, rc := range rebuilt.chains {
		if live.chains[id] == nil {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("chain %s missing in live state", rc.id))
		}
	}
	for cid, lc := range live.conflicts {
		rc := rebuilt.conflicts[cid]
		if rc == nil {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("conflict %s missing in replay", cid))
			continue
		}
		if rc.Status != lc.Status {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("conflict %s status live=%s replay=%s", cid, lc.Status, rc.Status))
		}
		if rc.Reason != lc.Reason {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("conflict %s reason live=%s replay=%s", cid, lc.Reason, rc.Reason))
		}
		if (lc.Resolution == nil) != (rc.Resolution == nil) {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("conflict %s resolution presence live=%v replay=%v", cid, lc.Resolution != nil, rc.Resolution != nil))
		} else if lc.Resolution != nil && (lc.Resolution.Choice != rc.Resolution.Choice ||
			lc.Resolution.WinnerDigest != rc.Resolution.WinnerDigest) {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("conflict %s resolution live=%+v replay=%+v", cid, lc.Resolution, rc.Resolution))
		}
	}
	for cid := range rebuilt.conflicts {
		if live.conflicts[cid] == nil {
			rep.Mismatches = append(rep.Mismatches, fmt.Sprintf("conflict %s missing in live state", cid))
		}
	}
	if len(rep.Mismatches) > 0 {
		rep.Consistent = false
	}
	return rep
}

// Replay rebuilds the chain state from the journals and verifies it
// equals the live state, then journals the report to replay.jsonl so
// every replay (including refused ones) stays queryable. Concurrent
// replays are bounded by MaxReplayTasks; excess requests are refused
// with UNAVAILABLE/REPLAY_LIMIT and the refusal is journaled.
func (s *fileStore) Replay(ctx context.Context) (*ReplayReport, error) {
	if s.cfg.MaxReplayTasks > 0 {
		select {
		case s.replaySem <- struct{}{}:
			defer func() { <-s.replaySem }()
		default:
			rep := &ReplayReport{
				Consistent: false,
				Mismatches: []string{fmt.Sprintf("replay refused: concurrent replay limit %d reached", s.cfg.MaxReplayTasks)},
			}
			s.mu.Lock()
			if !s.closed {
				s.seq++
				rep.Seq = s.seq
				_ = s.appendJSON(s.rplFile, rep)
			}
			s.mu.Unlock()
			return rep, &IngestionError{
				Type: ErrUnavailable, Message: "replay backlog limit reached",
				Reason: "REPLAY_LIMIT",
				Detail: fmt.Sprintf("max concurrent replay tasks %d reached", s.cfg.MaxReplayTasks),
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, &IngestionError{Type: ErrUnavailable, Message: "store closed", Reason: "CLOSED"}
	}
	j, err := s.readJournals()
	if err != nil {
		return nil, err
	}
	actions, _ := buildReplayActions(j)
	rebuilt := newChainModel()
	applyReplayActions(rebuilt, actions)

	rep := compareModels(s.model, rebuilt)
	s.seq++
	rep.Seq = s.seq
	if err := s.appendJSON(s.rplFile, rep); err != nil {
		return nil, err
	}
	return rep, nil
}
