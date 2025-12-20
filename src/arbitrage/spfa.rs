use std::collections::{HashSet, VecDeque};

use crate::arbitrage::graph::{EdgeKind, Graph};
use crate::domain::{CycleInfo, Trade};

pub fn find_negative_cycles_spfa(
    graph: &Graph,
    pair_names: &[String],
    max_depth: usize,
) -> anyhow::Result<Vec<CycleInfo>> {
    if graph.nodes.is_empty() || graph.edges.is_empty() {
        return Ok(Vec::new());
    }
    if max_depth == 0 {
        return Ok(Vec::new());
    }

    let n = graph.nodes.len();
    let m = graph.edges.len();

    let mut dist = vec![0.0f64; n];
    let mut pred = vec![usize::MAX; n];
    let mut enqueue_count = vec![0usize; n];
    let mut in_queue = vec![false; n];
    let mut q = VecDeque::new();

    for i in 0..n {
        q.push_back(i);
        in_queue[i] = true;
        enqueue_count[i] = 1;
    }

    let mut relaxation_checks: u64 = 0;
    let iteration_limit: u64 = (n as u64) * (m as u64) * 2 + (n as u64);
    let mut loop_iters: u64 = 0;
    let mut candidate_nodes: Vec<usize> = Vec::new();

    while let Some(u) = q.pop_front() {
        if loop_iters > iteration_limit {
            return Err(anyhow::anyhow!("SPFA 超过安全迭代上限"));
        }
        loop_iters += 1;

        in_queue[u] = false;
        for &edge_idx in &graph.adjacency[u] {
            relaxation_checks += 1;
            let e = &graph.edges[edge_idx];
            let v = e.to;
            let w = graph.edge_weight(edge_idx);
            if !w.is_finite() || !dist[u].is_finite() {
                continue;
            }
            let nd = dist[u] + w;
            if nd < dist[v] {
                dist[v] = nd;
                pred[v] = u;
                if !in_queue[v] && enqueue_count[v] < n {
                    q.push_back(v);
                    in_queue[v] = true;
                    enqueue_count[v] += 1;
                    if enqueue_count[v] >= n {
                        candidate_nodes.push(v);
                    }
                }
            }
        }
    }

    if candidate_nodes.is_empty() {
        let _ = relaxation_checks;
        return Ok(Vec::new());
    }

    candidate_nodes.sort_unstable();
    candidate_nodes.dedup();

    let mut found_signatures: HashSet<Vec<usize>> = HashSet::new();
    let mut node_in_cycle = vec![false; n];
    let mut cycles = Vec::new();

    for node_updated in candidate_nodes {
        if node_in_cycle[node_updated] {
            continue;
        }

        let mut backtrack = node_updated;
        for _ in 0..n {
            if backtrack == usize::MAX || pred[backtrack] == usize::MAX {
                backtrack = usize::MAX;
                break;
            }
            backtrack = pred[backtrack];
        }
        if backtrack == usize::MAX {
            continue;
        }

        let mut visited = vec![isize::MIN; n];
        let mut path_rev: Vec<usize> = Vec::with_capacity(n);
        let mut cur = backtrack;
        let mut pos: isize = 0;
        while cur != usize::MAX && visited[cur] == isize::MIN && (pos as usize) <= n + 1 {
            visited[cur] = pos;
            path_rev.push(cur);
            pos += 1;
            cur = pred[cur];
        }
        if cur == usize::MAX || visited[cur] == isize::MIN || (pos as usize) > n + 1 {
            continue;
        }

        let start_pos = visited[cur] as usize;
        if start_pos >= path_rev.len() {
            continue;
        }

        let mut cycle_node_indices = path_rev[start_pos..].to_vec();
        cycle_node_indices.reverse();
        if cycle_node_indices.is_empty() {
            continue;
        }
        let first = cycle_node_indices[0];
        cycle_node_indices.push(first);

        let depth = cycle_node_indices.len().saturating_sub(1);
        if depth == 0 || depth > max_depth {
            continue;
        }

        let mut signature = cycle_node_indices[..depth].to_vec();
        signature.sort_unstable();
        if found_signatures.contains(&signature) {
            continue;
        }

        if let Some(cycle) =
            reconstruct_cycle(graph, pair_names, &cycle_node_indices, depth)
        {
            found_signatures.insert(signature);
            for &node_idx in cycle_node_indices.iter().take(depth) {
                if node_idx < node_in_cycle.len() {
                    node_in_cycle[node_idx] = true;
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
