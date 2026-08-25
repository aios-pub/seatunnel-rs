/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.
 */

//! Leader election using a simplified Raft-based approach.
//!
//! In production, this would use the `raft` crate for full consensus.
//! This implementation uses a timeout-based leader election suitable for
//! development and testing.

use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Node role in the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

/// Leader election state.
pub struct LeaderElection {
    my_id: String,
    role: Role,
    term: u64,
    voted_for: Option<String>,
    heartbeat_interval: Duration,
    election_timeout: Duration,
    last_heartbeat: Instant,
    peers: Vec<String>,
}

impl LeaderElection {
    pub fn new(
        my_id: String,
        peers: Vec<String>,
        heartbeat_interval_ms: u64,
        election_timeout_ms: u64,
    ) -> Self {
        LeaderElection {
            my_id,
            role: Role::Follower,
            term: 0,
            voted_for: None,
            heartbeat_interval: Duration::from_millis(heartbeat_interval_ms),
            election_timeout: Duration::from_millis(election_timeout_ms),
            last_heartbeat: Instant::now(),
            peers,
        }
    }

    /// Returns the current role.
    pub fn role(&self) -> Role {
        self.role.clone()
    }

    /// Returns the current leader id (if known).
    pub fn leader_id(&self) -> Option<String> {
        if self.role == Role::Leader {
            Some(self.my_id.clone())
        } else {
            None
        }
    }

    /// Returns the current term.
    pub fn term(&self) -> u64 {
        self.term
    }

    /// Simulate receiving a heartbeat from the leader.
    pub fn receive_heartbeat(&mut self, from_leader: String) -> bool {
        if self.role == Role::Leader {
            return false;
        }
        self.last_heartbeat = Instant::now();
        self.role = Role::Follower;
        let leader_id = from_leader.clone();
        self.voted_for = Some(from_leader);
        info!("Received heartbeat from leader: {}", leader_id);
        true
    }

    /// Simulate receiving a vote request from a candidate.
    pub fn request_vote(&mut self, candidate_id: String, candidate_term: u64) -> bool {
        if candidate_term <= self.term {
            return false;
        }
        // Accept if we haven't voted yet or voted for this candidate
        if let Some(ref voted) = self.voted_for {
            if voted != &candidate_id {
                return false;
            }
        }
        self.term = candidate_term;
        self.voted_for = Some(candidate_id.clone());
        info!(
            "Voted for candidate: {} at term {}",
            candidate_id, candidate_term
        );
        true
    }

    /// Check if election timeout has expired (should start an election).
    pub fn should_start_election(&self) -> bool {
        if self.role == Role::Leader {
            return false;
        }
        self.last_heartbeat.elapsed() > self.election_timeout
    }

    /// Become leader (after winning an election).
    pub fn become_leader(&mut self) {
        self.term += 1;
        self.role = Role::Leader;
        self.voted_for = Some(self.my_id.clone());
        self.last_heartbeat = Instant::now();
        info!("Became leader at term {}", self.term);
    }

    /// Become candidate.
    pub fn become_candidate(&mut self) {
        self.term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.my_id.clone());
        info!("Became candidate at term {}", self.term);
    }
}

/// Simple cluster membership manager.
pub struct Membership {
    members: Vec<String>,
    quorum_size: usize,
}

impl Membership {
    pub fn new(members: Vec<String>) -> Self {
        let quorum_size = members.len() / 2 + 1;
        Membership {
            members,
            quorum_size,
        }
    }

    pub fn quorum_size(&self) -> usize {
        self.quorum_size
    }

    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    pub fn is_quorum(&self, votes: usize) -> bool {
        votes >= self.quorum_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_election() {
        let mut election =
            LeaderElection::new("node1".to_string(), vec!["node2".to_string()], 1000, 3000);
        assert_eq!(election.role(), Role::Follower);

        election.receive_heartbeat("node1".to_string());
        assert_eq!(election.role(), Role::Follower);

        election.become_candidate();
        assert_eq!(election.role(), Role::Candidate);

        election.become_leader();
        assert_eq!(election.role(), Role::Leader);
        assert_eq!(election.leader_id(), Some("node1".to_string()));
    }

    #[test]
    fn test_membership_quorum() {
        let membership = Membership::new(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        assert_eq!(membership.quorum_size(), 2);
        assert!(!membership.is_quorum(1));
        assert!(membership.is_quorum(2));
        assert!(membership.is_quorum(3));
    }

    #[test]
    fn test_vote_request() {
        let mut election = LeaderElection::new("node1".to_string(), vec![], 1000, 3000);
        assert!(election.request_vote("candidate".to_string(), 1));
        assert!(!election.request_vote("other".to_string(), 2)); // already voted
    }

    #[test]
    fn test_election_timeout() {
        let election = LeaderElection::new("node1".to_string(), vec![], 1000, 1);
        // With 1ms timeout and no heartbeat, election should be due
        std::thread::sleep(Duration::from_millis(5));
        assert!(election.should_start_election());
    }
}
