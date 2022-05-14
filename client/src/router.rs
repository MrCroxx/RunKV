use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use runkv_proto::meta::{KeyRange, KeyRangeInfo};

fn _is_overlap(r1: &KeyRange, r2: &KeyRange) -> bool {
    !(r1.start_key > r2.end_key || r1.end_key < r2.start_key)
}

fn in_range(key: &[u8], range: &KeyRange) -> bool {
    key >= &range.start_key[..] && key < &range.end_key[..]
}

pub struct LeaderInfo {
    pub node: u64,
    pub group: u64,
    pub raft_node: u64,
}

struct RouterCore {
    /// { key range -> raft group id }
    key_range_groups: BTreeMap<KeyRange, u64>,
    /// { raft group id -> [raft node id] }
    group_raft_nodes: HashMap<u64, Vec<u64>>,
    /// { raft group id -> leader raft node id }
    group_leader: HashMap<u64, u64>,
    /// { raft node id -> node id }
    raft_nodes: HashMap<u64, u64>,
}

#[derive(Clone)]
pub struct Router {
    core: Arc<RwLock<RouterCore>>,
}

impl Default for Router {
    fn default() -> Self {
        Self {
            core: Arc::new(RwLock::new(RouterCore {
                key_range_groups: BTreeMap::default(),
                group_raft_nodes: HashMap::default(),
                group_leader: HashMap::default(),
                raft_nodes: HashMap::default(),
            })),
        }
    }
}

impl Router {
    pub fn leader(&self, key: &[u8]) -> Option<LeaderInfo> {
        let core = self.core.read();
        for (key_range, &group) in core.key_range_groups.iter() {
            if in_range(key, key_range) {
                if let Some(&leader) = core.group_leader.get(&group) {
                    let node = core.raft_nodes.get(&leader).copied().unwrap();
                    return Some(LeaderInfo {
                        node,
                        group,
                        raft_node: leader,
                    });
                }
                return None;
            }
        }
        None
    }

    pub fn update_key_ranges(&self, key_range_infos: Vec<KeyRangeInfo>) {
        let mut updated = RouterCore {
            key_range_groups: BTreeMap::default(),
            group_raft_nodes: HashMap::default(),
            group_leader: HashMap::default(),
            raft_nodes: HashMap::default(),
        };
        for KeyRangeInfo {
            group,
            key_range,
            raft_nodes,
            leader,
        } in key_range_infos
        {
            let key_range = key_range.unwrap();

            updated.key_range_groups.insert(key_range, group);
            updated
                .group_raft_nodes
                .insert(group, raft_nodes.keys().copied().collect_vec());
            updated.group_leader.insert(group, leader);
            for (raft_node, node) in raft_nodes {
                updated.raft_nodes.insert(raft_node, node);
            }
        }
        let mut core = self.core.write();
        *core = updated;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync_clone<T: Send + Sync + Clone + 'static>() {}

    #[test]
    fn ensure_send_sync_clone() {
        is_send_sync_clone::<Router>();
    }
}
