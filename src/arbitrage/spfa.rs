use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::arbitrage::graph::{EdgeKind, Graph};
use crate::domain::{CycleInfo, Trade};

#[derive(Default)]
pub struct SpfaWorkspace {
    dist: Vec<f64>,
    pred: Vec<usize>,
    enqueue_count: Vec<usize>,
    in_queue: Vec<bool>,

    queue: Vec<usize>,
    queue_head: usize,
    candidate_nodes: Vec<usize>,

    found_signatures: HashSet<u64>,
    node_in_cycle: Vec<bool>,

    visited_stamp: Vec<u32>,
    visited_pos: Vec<i32>,
    stamp: u32,

    path_rev: Vec<usize>,
    cycle_node_indices: Vec<usize>,
    signature_scratch: Vec<usize>,
}

impl SpfaWorkspace {
    pub fn new() -> Self {
        Self::default()
    }

    fn prepare(&mut self, n: usize) {
        self.dist.resize(n, 0.0);
        self.dist.fill(0.0);

        self.pred.resize(n, usize::MAX);
        self.pred.fill(usize::MAX);

        self.enqueue_count.resize(n, 0);
        self.enqueue_count.fill(0);

        self.in_queue.resize(n, false);
        self.in_queue.fill(false);

        self.node_in_cycle.resize(n, false);
        self.node_in_cycle.fill(false);

        self.visited_stamp.resize(n, 0);
        self.visited_pos.resize(n, 0);

        self.queue.clear();
        self.queue_head = 0;
        self.candidate_nodes.clear();
        self.found_signatures.clear();
        self.path_rev.clear();
        self.cycle_node_indices.clear();
        self.signature_scratch.clear();
    }

    fn next_stamp(&mut self) -> u32 {
        self.stamp = self.stamp.wrapping_add(1);
        if self.stamp == 0 {
            self.visited_stamp.fill(0);
            self.stamp = 1;
        }
        self.stamp
    }
}

pub fn find_negative_cycles_spfa(
    graph: &Graph,
    pair_names: &[String],
    max_depth: usize,
) -> anyhow::Result<Vec<CycleInfo>> {
    let mut ws = SpfaWorkspace::new();
    find_negative_cycles_spfa_with_workspace(graph, pair_names, max_depth, &mut ws)
}

pub fn find_negative_cycles_spfa_with_workspace(
    graph: &Graph,
    pair_names: &[String],
    max_depth: usize,
    ws: &mut SpfaWorkspace,
) -> anyhow::Result<Vec<CycleInfo>> {
    if graph.nodes.is_empty() || graph.edges.is_empty() {
        return Ok(Vec::new());
    }
    if max_depth == 0 {
        return Ok(Vec::new());
    }

    let n = graph.nodes.len();
    let m = graph.edges.len();

    ws.prepare(n);

    for i in 0..n {
        ws.queue.push(i);
        ws.in_queue[i] = true;
        ws.enqueue_count[i] = 1;
    }

    let iteration_limit: u64 = (n as u64) * (m as u64) * 2 + (n as u64);
    let mut loop_iters: u64 = 0;

    while ws.queue_head < ws.queue.len() {
        if loop_iters > iteration_limit {
            return Err(anyhow::anyhow!("SPFA 超过安全迭代上限"));
        }
        loop_iters += 1;

        let u = ws.queue[ws.queue_head];
        ws.queue_head += 1;

        ws.in_queue[u] = false;
        let du = ws.dist[u];
        if !du.is_finite() {
            continue;
        }

        for &edge_idx in &graph.adjacency[u] {
            let v = graph.edges[edge_idx].to;
            let w = graph.edge_weight(edge_idx);
            if !w.is_finite() {
                continue;
            }
            let nd = du + w;
            if nd < ws.dist[v] {
                ws.dist[v] = nd;
                ws.pred[v] = u;
                if !ws.in_queue[v] && ws.enqueue_count[v] < n {
                    ws.queue.push(v);
                    ws.in_queue[v] = true;
                    ws.enqueue_count[v] += 1;
                    if ws.enqueue_count[v] >= n {
                        ws.candidate_nodes.push(v);
                    }
                }
            }
        }
    }

    if ws.candidate_nodes.is_empty() {
        return Ok(Vec::new());
    }

    ws.candidate_nodes.sort_unstable();
    ws.candidate_nodes.dedup();

    let mut cycles = Vec::new();

    for i in 0..ws.candidate_nodes.len() {
        let node_updated = ws.candidate_nodes[i];
        if ws.node_in_cycle[node_updated] {
            continue;
        }

        let mut backtrack = node_updated;
        for _ in 0..n {
            if backtrack == usize::MAX || ws.pred[backtrack] == usize::MAX {
                backtrack = usize::MAX;
                break;
            }
            backtrack = ws.pred[backtrack];
        }
        if backtrack == usize::MAX {
            continue;
        }

        let stamp = ws.next_stamp();
        ws.path_rev.clear();
        let mut cur = backtrack;
        let mut pos: i32 = 0;
        while cur != usize::MAX && ws.visited_stamp[cur] != stamp && (pos as usize) <= n + 1 {
            ws.visited_stamp[cur] = stamp;
            ws.visited_pos[cur] = pos;
            ws.path_rev.push(cur);
            pos += 1;
            cur = ws.pred[cur];
        }
        if cur == usize::MAX || ws.visited_stamp[cur] != stamp || (pos as usize) > n + 1 {
            continue;
        }

        let start_pos = ws.visited_pos[cur];
        if start_pos < 0 {
            continue;
        }
        let start_pos = start_pos as usize;
        if start_pos >= ws.path_rev.len() {
            continue;
        }

        ws.cycle_node_indices.clear();
        ws.cycle_node_indices
            .extend_from_slice(&ws.path_rev[start_pos..]);
        ws.cycle_node_indices.reverse();
        if ws.cycle_node_indices.is_empty() {
            continue;
        }
        let first = ws.cycle_node_indices[0];
        ws.cycle_node_indices.push(first);

        let depth = ws.cycle_node_indices.len().saturating_sub(1);
        if depth == 0 || depth > max_depth {
            continue;
        }

        ws.signature_scratch.clear();
        ws.signature_scratch
            .extend_from_slice(&ws.cycle_node_indices[..depth]);
        ws.signature_scratch.sort_unstable();
        let sig = signature_hash(&ws.signature_scratch);
        if ws.found_signatures.contains(&sig) {
            continue;
        }

        if let Some(cycle) = reconstruct_cycle(graph, pair_names, &ws.cycle_node_indices, depth) {
            ws.found_signatures.insert(sig);
            for &node_idx in ws.cycle_node_indices.iter().take(depth) {
                if node_idx < ws.node_in_cycle.len() {
                    ws.node_in_cycle[node_idx] = true;
                }
            }
            cycles.push(cycle);
        }
    }

    Ok(cycles)
}

fn reconstruct_cycle(
    graph: &Graph,
    pair_names: &[String],
    cycle_nodes: &[usize],
    depth: usize,
) -> Option<CycleInfo> {
    let mut trades = Vec::with_capacity(depth);
    let mut nodes = Vec::with_capacity(depth + 1);

    for &idx in cycle_nodes {
        nodes.push(graph.nodes.get(idx)?.clone());
    }

    for i in 0..depth {
        let u = cycle_nodes[i];
        let v = cycle_nodes[i + 1];
        let edge_idx = *graph.edge_lookup.get(&(u, v))?;
        let e = &graph.edges[edge_idx];
        let pair = pair_names.get(e.pair_id)?.clone();
        let kind = match e.kind {
            EdgeKind::Buy => "BUY",
            EdgeKind::Sell => "SELL",
        }
        .to_string();
        trades.push(Trade {
            from: graph.nodes[u].clone(),
            to: graph.nodes[v].clone(),
            pair,
            kind,
        });
    }

    Some(CycleInfo {
        nodes,
        trades,
        depth,
    })
}

fn signature_hash(sorted_nodes: &[usize]) -> u64 {
    let mut hasher = DefaultHasher::new();
    sorted_nodes.hash(&mut hasher);
    hasher.finish()
}
