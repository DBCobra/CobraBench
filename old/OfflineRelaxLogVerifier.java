package verifier;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import algo.DFSCycleDetection;
import chengTxn.Profiler;
import graph.AdjacentListGraph;
import graph.GEdge;
import graph.GNode;
import verifier.txngraph.EdgeLabel;
import verifier.txngraph.EdgeLabel.LABEL;
import verifier.txngraph.OpNode;
import verifier.txngraph.Pair;
import verifier.txngraph.PrecedenceGraph;
import verifier.txngraph.TxnNode;
import verifier.txngraph.WriteGraph;
import verifier.txngraph.WriteNode;

public class OfflineRelaxLogVerifier extends AbstractLogVerifier{
	
	// for permutate(). It's own states.
	ArrayList<LinkedList<WriteNode>> perkey_list = null;
	Map<Long,Long> perkey_topohash = null;
	PruneCondition prune_cond = new PruneCondition();
	
	public OfflineRelaxLogVerifier(String logfd) {
		super(logfd);
	}

	// NOTE: pg should be read-only!!!!
	public static PrecedenceGraph concentrateTxnGraph(PrecedenceGraph pg) {
		// clone the known graph
		PrecedenceGraph wg = new PrecedenceGraph(pg);

		// remove the RO-txn by connecting all predecessors and successors
		// NOTE: the original wg.allNodes() might be modified during the following process
		ArrayList<TxnNode> txns = new ArrayList<TxnNode>(wg.allNodes());
		for (TxnNode n : txns) {
			// FIXME: OPTIMIZATION: can be faster by adding a flag in TxnNode
			// check if RO
			boolean isRO = true;
			for (OpNode op : n.getOps()) {
				if (!op.isRead) {
					isRO = false;
					break;
				}
			}
			
			// if RO, (1) connect predecessors and successors and (2) remove the node
			if (isRO) {				
				for (GEdge inedge : wg.inEdges(n)) {
					for (GEdge outedge : wg.outEdges(n)) {
						assert inedge.getDst() == outedge.getSrc();
						// construct the new edge with the labels
						EdgeLabel new_label = EdgeLabel.concatEdges(
								(EdgeLabel)inedge.getLabel(), (EdgeLabel)outedge.getLabel());
						wg.addEdge(inedge.getSrc().id(), outedge.getDst().id(), new_label);
					}
				}	
				wg.deleteNode(n);
			}
		}
		
		return wg;
	}
	
	// recent-map: key=>{wid,wid,...}
	private static void absorbRecentMap(HashMap<Long, HashSet<Long>> fr, HashMap<Long, HashSet<Long>> to) {
		for (long key : fr.keySet()) {
			if (!to.containsKey(key)) {
				to.put(key, new HashSet<Long>());
			}
			HashSet<Long> to_set = to.get(key);
			for (long wid : fr.get(key)) {
				to_set.add(wid);
			}
		}
	}
	
	/*
	 * (1) each node has a recent-map, with key=>{most recent writes of this key}
   (2) do BFS,
    for each node reached,
    -- pull and merge the recent-map from all its predecessor
    -- add this node to its own key graph
    -- create edges from the most-recent-writes in recent-map to this write
    -- update the recent-map of its own key to this write itself
	 */
	
	public static HashMap<Long,WriteGraph> concentrateWriteGraph(WriteGraph wg) {
		// key => write graph
		HashMap<Long,WriteGraph> perkey_graph = new HashMap<Long, WriteGraph>();
		// wid => recent-map {key, [wid, wid]}
		HashMap<Long, HashMap<Long, HashSet<Long>>> recent_maps = new HashMap<Long, HashMap<Long, HashSet<Long>>>();
		// keep track of indgrees
		HashMap<Long,Integer> node_indegree = new HashMap<Long, Integer>();
		
		// find all the zero-in-degree nodes
		LinkedList<WriteNode> to_explore = new LinkedList<WriteNode>();
		for (WriteNode n : wg.allNodes()) {
			if (n.numPredecessor() == 0) {
				to_explore.add(n);
			} else {
				node_indegree.put(n.getWid(), n.numPredecessor());
			}
		}
		
		// do BFS
		while(to_explore.size() > 0) {
			WriteNode cur_node = to_explore.poll();
			long key = cur_node.getKey();
			long wid = cur_node.getWid();
			
			// (1) add itself to the relevant graphs
			if (!perkey_graph.containsKey(key)) {
				perkey_graph.put(key, new WriteGraph());
			}
			// need to clone a new WriteNode, without any edge information
			perkey_graph.get(key).addWriteNode(new WriteNode(cur_node));
			
			// (2) add all the edges if applicable
			HashMap<Long, HashSet<Long>> cur_recent_map = null;
			if (recent_maps.containsKey(wid)) {
				cur_recent_map = recent_maps.get(wid);
				if (cur_recent_map.containsKey(key)) {
					for (long predecessor_wid : cur_recent_map.get(key)) {
						// make sure they are the same key
						assert key == wg.getNode(predecessor_wid).getKey();
						perkey_graph.get(key).addEdge(predecessor_wid, wid);
					}
				}
			} else {
				cur_recent_map = new HashMap<Long, HashSet<Long>>();
			}
			
			// (3) update the current recent-map
			if (cur_recent_map.containsKey(key)) {
				cur_recent_map.get(key).clear();
				cur_recent_map.get(key).add(wid);
			} else {
				HashSet<Long> tmp = new HashSet<Long>();
				tmp.add(wid);
				cur_recent_map.put(key, tmp);
			}
			
			// (4) find out all the successors and push&merge the recent-maps
			// (5) update the in-degree, add to list if 0
			for (WriteNode succ_node : wg.successors(cur_node)) {
				long succ_wid = succ_node.getWid();
				
				// A short-cut: if it is one-on-one, just transfer the recent-map
				if (cur_node.numSuccessor() == 1 && succ_node.numPredecessor() == 1) {
					// safely transfer the recent-map
					recent_maps.put(succ_wid, cur_recent_map);
					node_indegree.put(succ_wid, 0);
					to_explore.add(succ_node);
					recent_maps.remove(wid);
					continue;
				}
				
				if (!recent_maps.containsKey(succ_wid)) {
					recent_maps.put(succ_wid, new HashMap<Long, HashSet<Long>>());
				}
				
				// push&merge the recent-maps
				absorbRecentMap(cur_recent_map, recent_maps.get(succ_wid));
				
				// update in-degree
				int num_in = node_indegree.get(succ_wid);
				num_in--;
				if (num_in == 0) {
					to_explore.add(succ_node);
				}
				node_indegree.put(succ_wid, num_in);	
			}
			
			// (6) release the recent-map
			recent_maps.remove(wid);
		}
	
	  // validation phase
		// -- all the in-degree should be 0
		for (int indegree : node_indegree.values()) {
			assert indegree == 0;
		}
		
		return perkey_graph;
	}
	
	// the input pg should be the concentrated graph which doesn't contain RO txns
	public static WriteGraph extractWriteGraph(PrecedenceGraph pg) {
		WriteGraph wg = new WriteGraph();
		
		// txnid=>wid
		HashMap<Long, Long> firstWriteOfTxn = new HashMap<Long,Long>();
		HashMap<Long, Long> lastWriteOfTxn = new HashMap<Long,Long>();
		
		// (1) build all the write nodes
		// (2) record the start and end write for one txn
		// (3) add all edges within txn
		for (TxnNode txn : pg.allNodes()) {
			// for one txn
			WriteNode first = null, last = null;
			long delta_trick = 0;
			long commit_ts = txn.getCommitTimestamp();
			assert commit_ts != -1;
			
			for (OpNode op : txn.getOps()) {
				if (!op.isRead) {
					// create a WriteNode and add to write graph
					// the timestamp for this write is (commit_ts + delta_trick)
					WriteNode cur_node = new WriteNode(op, (commit_ts + delta_trick++));
					wg.addWriteNode(cur_node);
					// check if the first write
					if (first == null) {
						first = cur_node;
					} else {
						// if not the first write, add edge within txn
						wg.addEdge(last.getWid(), cur_node.getWid());
					}
					last = cur_node;
				}
			}
			// collect the firstWrite and lastWrite
			assert first!=null && last !=null; // there should be no RO txn
			firstWriteOfTxn.put(txn.getTxnid(), first.getWid());
			lastWriteOfTxn.put(txn.getTxnid(), last.getWid());
		}
		
		// (4) construct all the edges:
		// for each txn, add an edge from the last write of current txn
		// to the first write of the successor txns
		for (TxnNode txn : pg.allNodes()) {
			long cur_last_write = lastWriteOfTxn.get(txn.getTxnid());
			// FIXME: is this OK? or we don't care?
			// that the last and first write inherent the relationship
			for (GEdge outedge : pg.outEdges(txn)) {
				GNode succ = outedge.getDst();
				long succ_first_write = firstWriteOfTxn.get(succ.id());
				// add an edge
				wg.addEdge(cur_last_write, succ_first_write);
			}
		}
		
		return wg;
	}
	
	// add WW-edges and RW-edges for the precedence graph
	private void AddWOEdges(ArrayList<LinkedList<WriteNode>> write_orders,
			PrecedenceGraph g, HashMap<Long,ArrayList<OpNode>> readFromMapping)
	{
		for (LinkedList<WriteNode> wlist : write_orders) {
			long key = wlist.get(0).getKey();
			WriteNode prev = null, cur = null;
			for (WriteNode wnode : wlist) {
				assert key == wnode.getKey();
				if (prev == null) {
					prev = wnode;
					continue;
				}
				cur = wnode;
				
				long prev_txnid = prev.getTxnid();
				long prev_wid = prev.getWid();
				long cur_txnid = cur.getTxnid();
				
				// add WW-edge for the txns
				if (prev_txnid != cur_txnid) {
					g.addEdge(prev_txnid, cur_txnid, EdgeLabel.getLabel(EdgeLabel.LABEL.WW));
				}
				// if there are R-op reads-from prev write, should add RW-edge
				if (readFromMapping.containsKey(prev_wid)) {
					for (OpNode op : readFromMapping.get(prev_wid)) {
						if (op.txnid != cur_txnid) {
							g.addEdge(op.txnid, cur_txnid, EdgeLabel.getLabel(EdgeLabel.LABEL.RW));
						}
					}
				}
				
				prev = cur;
			}
		}
	}
	
	private int resetAndReturnMinSorter(PruneCondition prune, Map<Long,Long> hit_cond, ArrayList<TopologicalSorter> perkey_sorter) {
		
		/*
		System.out.println("  hit prune.");
		System.out.println(prune);
		System.out.print("hit conditions: {");
		for (long key : hit_cond.keySet()) {
			System.out.print(Long.toHexString(key) + "=>" + Long.toHexString(hit_cond.get(key)) + ", ");
		}
		System.out.println("}");
		*/
		
		// ----
		// find the smallest index of the hit condition
		int min_indx = perkey_sorter.size();
		for (int i = 0; i < perkey_sorter.size(); i++) {
			long key = perkey_sorter.get(i).getKey();
			if (hit_cond.containsKey(key)) {
				if (i < min_indx) {
					min_indx = i;
					//break;
				}
				//System.out.println("[" + i + "]="+ Long.toHexString(perkey_sorter.get(i).getKey()));
			}
		}
		//System.out.println(" the smallest key[" + min_indx + "]="+  Long.toHexString(perkey_sorter.get(min_indx).getKey()));
		assert min_indx != perkey_sorter.size();
		// ----

		// we can skip cases by choosing the next case for the [min_indx] topological
		// sorter
		// (1) set the cur_indx to min_indx (which will be increased at the header of this loop)
		// (2) reset all the previous sorters, and try again
		for (int i=0; i<min_indx; i++) {
			TopologicalSorter tmp_sorter = perkey_sorter.get(i);
			tmp_sorter.refresh();
			LinkedList<WriteNode> first_sort = tmp_sorter.getOneTopologicalSort();
			assert first_sort != null;
			perkey_list.set(i, first_sort);
			perkey_topohash.put(tmp_sorter.getKey(), tmp_sorter.getTopoHash());
		}
		
		return min_indx;
	}
	
	private String solution2str(ArrayList<TopologicalSorter> perkey_sorter) {
		StringBuilder sb = new StringBuilder();
		sb.append("current solution: [" + perkey_sorter.hashCode() + "]\n  ");
		for (int i=0; i<perkey_sorter.size(); i++) {
			sb.append(String.format("|%2d",i));
		}
		sb.append("\n  ");
		for (TopologicalSorter sorter : perkey_sorter) {
			sb.append(String.format("|%2d",sorter.getCurrentTrail()));
		}
		sb.append("\n");
		return sb.toString();
	}
	
	// FIXME: may have better exploration solutions
	/*
	 * Some possible optimizations:
	 *  -- Sort the keys by the contention. The heavier the contention, the more likely there is a cycle.
	 */
	private ArrayList<LinkedList<WriteNode>> permutate(ArrayList<TopologicalSorter> perkey_sorter, PruneCondition prune) {
		int size = perkey_sorter.size();
		
		if (perkey_list == null) {
			// init
			perkey_list = new ArrayList<LinkedList<WriteNode>>();
			perkey_topohash = new HashMap<Long,Long>();
			for (int i=0; i<size; i++) {
				perkey_list.add(null);
			}
		}
		
		// (1) choose one slot to permutate
		// (2) keep track of running out of choices (then return null)
		int cur_indx = 0;
		while (cur_indx < size) {
			TopologicalSorter s = perkey_sorter.get(cur_indx);
			LinkedList<WriteNode> sort = s.getOneTopologicalSort();
			
			// if slot is null:
			// done exploring this slot, refresh, get the original one and move to next slot
			if (sort == null) {
				s.refresh();
				sort = s.getOneTopologicalSort();
				assert sort != null;
				perkey_list.set(cur_indx, sort);
				perkey_topohash.put(s.getKey(), s.getTopoHash());
				cur_indx++;
				// Here we've run out of choices
				if (cur_indx >= size) {
					return null;
				}
				continue;
			}

			// if sort is a valid list
			// then we found a valid sort for this slot,
			// now we have one candidate
			perkey_list.set(cur_indx, sort);
			perkey_topohash.put(s.getKey(), s.getTopoHash());

			// check if can be pruned
			Map<Long, Long> hit_cond = prune.hitConditionOtherwiseNull(perkey_topohash);	
			// if can be pruned
			if (hit_cond != null) {
				cur_indx = resetAndReturnMinSorter(prune, hit_cond, perkey_sorter);
				//System.out.println("[INFO] hit the prune condition");
				//System.out.println(solution2str(perkey_sorter));
				// give another try
				continue;
			}

			// if we successfully finish filling all the slots, we found an answer
			if (perkey_topohash.size() == size) {
				System.out.println("[INFO] found a candidate");
				System.out.println(solution2str(perkey_sorter));
				return perkey_list;
			} else {
				// otherwise, we're still during init
				cur_indx++;
			}
		}

		// should not be here
		assert false;
		return null;
	}
	
	private static HashMap<Long,ArrayList<Pair<OpNode,OpNode>>>
	ParseWOLog(File log, HashMap<Long,ArrayList<Pair<OpNode,OpNode>>> ww_pairs,
			PrecedenceGraph g) throws IOException {
		DataInputStream in = new DataInputStream(new FileInputStream(log));
		// =================================
		// FIXME: copy-paste
		// | 1 | 7B | 8B | 1 | 7B | 8B |
		// | 0 | txnid_prev | write_id_prev | 0 | txnid | write_id |
		byte[] c_prev_txnid = new byte[8];
		byte[] c_prev_wid = new byte[8];
		byte[] c_txnid = new byte[8];
		byte[] c_wid = new byte[8];

		long prev_txnid;
		long prev_wid;
		long txnid;
		long wid;

		while (true) {
			int ret = in.read(c_prev_txnid);
			if (ret == -1)
				break;
			assert ret == 8;
			ret = in.read(c_prev_wid);
			assert ret == 8;
			ret = in.read(c_txnid);
			assert ret == 8;
			ret = in.read(c_wid);
			assert ret == 8;

			prev_txnid = bytes2long(c_prev_txnid, 8);
			prev_wid = bytes2long(c_prev_wid, 8);
			txnid = bytes2long(c_txnid, 8);
			wid = bytes2long(c_wid, 8);

			assert prev_txnid != txnid;

			// TODO: check if wid within txnid
			// TODO: check if prev_wid within prev_txnid

			// write sampling: skip this entry if we found it missing
			if (prev_txnid == VeriConstants.MISSING_TXN_ID || prev_txnid == VeriConstants.NOP_TXN_ID) {
				assert prev_wid == VeriConstants.MISSING_WRITE_ID ||
						prev_wid == VeriConstants.NOP_WRITE_ID;
				num_missing_ww++;
				continue;
			}
			num_ww++;

			// construct wwpairs
			OpNode prev = null, cur = null;
			// get prev_node
			TxnNode prev_txn = g.getNode(prev_txnid);
			for (OpNode n : prev_txn.getOps()) {
				if (!n.isRead && n.wid == prev_wid) {
					prev = n;
					break;
				}
			}
			assert prev != null; // must find one
			// get cur_node
			TxnNode cur_txn = g.getNode(txnid);
			for (OpNode n : cur_txn.getOps()) {
				if (!n.isRead && n.wid == wid) {
					cur = n;
					break;
				}
			}
			assert cur != null;
			// add to ww_pairs
			if (!ww_pairs.containsKey(prev.key_hash)) {
				ww_pairs.put(prev.key_hash, new ArrayList<Pair<OpNode,OpNode>>());
			}
			ww_pairs.get(prev.key_hash).add(new Pair<OpNode,OpNode>(prev,cur));
		}
		// =================================
		return ww_pairs;
	}
	
	private void pushBackAndResetTopo(ArrayList<TopologicalSorter> ordered_perkey_sorter, TopologicalSorter sorter) {
		assert ordered_perkey_sorter.contains(sorter);
		// set the sorter at the end
		ordered_perkey_sorter.remove(sorter);
		ordered_perkey_sorter.add(sorter);
		// reset all the sorters
		for (TopologicalSorter s : ordered_perkey_sorter) {
			s.refresh();
		}
		// reset the results
		perkey_list = null;
		//perkey_topohash = null;
	}
	
	// return one topological sort

	// helper class for AddTimeOrderEdges(...)
	static class TxnBeginEndEvent {
		public boolean begin;
		public long timestamp;
		public TxnNode txn;
		
		public TxnBeginEndEvent(boolean begin, long ts, TxnNode txn) {
			this.begin = begin;
			this.timestamp = ts;
			this.txn = txn;
		}
	}
	
	// add the time order edges between txns
	// (1) create two event for one txn, the start and end event
	// (2) sort the events by time
	// (3) use orochi's algorithm to add time order edges
	private void AddTimeOrderEdges(PrecedenceGraph g) {
		// (1)
		Map<Long,TxnBeginEndEvent> all_events = new HashMap<Long,TxnBeginEndEvent>();
		for (TxnNode txn : g.allNodes()) {
			all_events.put(txn.getBeginTimestamp(), new TxnBeginEndEvent(true, txn.getBeginTimestamp(), txn));
			all_events.put(txn.getCommitTimestamp(), new TxnBeginEndEvent(false, txn.getCommitTimestamp(), txn));
		}
		// (2)
		SortedMap<Long,TxnBeginEndEvent> sorted_events = new TreeMap<Long,TxnBeginEndEvent>(all_events);
		// (3)
		Set<TxnBeginEndEvent> frontier = new HashSet<TxnBeginEndEvent>();
		
		for (TxnBeginEndEvent cur_event : sorted_events.values()) {
			if (cur_event.begin) {
				// add time-order edges
				for (TxnBeginEndEvent e : frontier) {
					// if txn ends before current_event starting, then add an edge
					if (e.txn.getCommitTimestamp() < cur_event.timestamp) {
						g.addEdge(e.txn.getTxnid(), cur_event.txn.getTxnid(), EdgeLabel.getLabel(EdgeLabel.LABEL.TO));
					}
				}
			} else {
				Set<TxnBeginEndEvent> rm_events = new HashSet<TxnBeginEndEvent>();
				for (TxnBeginEndEvent e : frontier) {
					if (e.txn.getCommitTimestamp() < cur_event.timestamp) {
						rm_events.add(e);
					}
				}
				// remove the ones which can be represented from frontier
				frontier.removeAll(rm_events);
			}
		}
	}
	
	/*
	1. Construct a graph with client-order, WR order and partial WW order
  2. Build concentrate graph: remove all the RO txn, but keep their connectivities
    -- absorb the connectivity of the next un-selected node
  3. extract a write graph with concentrated graph's  connectivities
  4. generate a topological sort graph for the write order
  5. Loop 
     5.1 Keep track of which possibilities have been tracked, if run out, return fail.
        -- https://www.geeksforgeeks.org/all-topological-sorts-of-a-directed-acyclic-graph/
     5.2 Use the generated writes order to construct the precedence graph to see
     if there is cycle.
     If there is, continue
     Else return good
	 */
	
	public boolean audit() {
		ArrayList<File> opfiles = findOpLogInDir(log_dir);
		ArrayList<File> wofiles = findWOLogInDir(log_dir);
		
		HashMap<Long,ArrayList<OpNode>> readFromMapping = new HashMap<Long,ArrayList<OpNode>>();
		HashMap<Long,ArrayList<Pair<OpNode,OpNode>>> ww_pairs = new HashMap<Long,ArrayList<Pair<OpNode,OpNode>>>();

		// 1. construct a graph with client-order, WR order and *partial WW order*
		try {
			for (File f : opfiles) {
				ExtractClientLogFromLog(f, m_g);
			}
			AddReadFromEdges(m_g, readFromMapping);
			AddTimeOrderEdges(m_g);
			
			for (File f : wofiles) {
				ExtractWWFromLog(f, m_g, readFromMapping); // NOTE: [WHY?] if this is commented out, then there are too many options to check?
				ww_pairs = ParseWOLog(f, ww_pairs, m_g);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
//		GraphVisual visual = new GraphVisual();
//		System.out.println("===orig graph====");
//		System.out.println(g.dumpGraph());
//		visual.display(g, "/tmp/cobra/fig/orig.dot");
		System.out.println("WW percent: " + ((double)num_ww)/(num_missing_ww+num_ww)*100 + "% (" + num_ww + "/" + (num_ww + num_missing_ww) + ")");
		
		// make sure the graph is acyclic
		boolean hasCycle = DFSCycleDetection.CycleDetection(m_g.getGraph());
		if (hasCycle) {
			return false;
		}

		// 2. build concentrated graph
		PrecedenceGraph congraph = concentrateTxnGraph(m_g);
		
//		System.out.println("===concentrate graph====");
//		System.out.println(congraph.dumpGraph());
//		visual.display(congraph, "/tmp/cobra/fig/conc.dot",false);
		
		// 3. extract a write graph with concentrated graph's connectivities
		WriteGraph wgraph = extractWriteGraph(congraph);
		assert !DFSCycleDetection.CycleDetection(wgraph.getGraph());
		
//		System.out.println("===write-graph graph====");
//		System.out.println(wgraph.dumpGraph());
//		GraphVisual visual2 = new GraphVisual();
//		visual2.display(wgraph, "/tmp/cobra/fig/wgraph.dot", true);

		// 4. generate perkey write graph
		HashMap<Long,WriteGraph> perkey_graphs = concentrateWriteGraph(wgraph);
		// construct sorter
		ArrayList<TopologicalSorter> ordered_perkey_sorter = new ArrayList<TopologicalSorter>();
		// init perkey_sorter & perkey_list
		
		
		
		
		
		// XXX: try to see how many possibilites for each key's sorter
		
		HashMap<Long,Integer> perkey_num_cases = new HashMap<Long,Integer>();
		for (Long key : perkey_graphs.keySet()) {
			TopologicalSorter s = new TopologicalSorter(key, perkey_graphs.get(key), ww_pairs.get(key));
			int counter = 0;
			do {
				counter++;
			} while(s.getOneTopologicalSort()!=null);
			perkey_num_cases.put(key, counter);
		}
		System.out.println("#keys=" + perkey_num_cases.size());
		
		BigInteger total_possibilities = BigInteger.valueOf(1);
		for (Long key : perkey_num_cases.keySet()) {
			System.out.println(Long.toHexString(key)+"=>"+perkey_num_cases.get(key));
			long cur_num = perkey_num_cases.get(key);
			total_possibilities = total_possibilities.multiply(BigInteger.valueOf(cur_num));
			System.out.println(total_possibilities);
		}
		System.out.println(total_possibilities);
		
		System.out.println("XXXX");
		System.exit(1);
		
		
		// an optimization: sorted the keys by contention for more an efficient pruning
		Set<Long> seen_key_set = new HashSet<Long>();
		Map<Long, TopologicalSorter> key2sorter = new HashMap<Long, TopologicalSorter>();
		for (Long key : perkey_graphs.keySet()) {
			TopologicalSorter s = new TopologicalSorter(key, perkey_graphs.get(key), ww_pairs.get(key));
			ordered_perkey_sorter.add(s);
			key2sorter.put(key, s);
		}

//GraphVisual visual3 = new GraphVisual();

		// 5. permute all the possible WW orders for each key
		int try_counter = 0;
		while(true) {
			System.out.println("Try candidate[" + (try_counter++) + "]");
			// FIXME: this can be a performance issue, it copies whole graph every time
			PrecedenceGraph guess_graph = new PrecedenceGraph(m_g);
/*		
	System.out.println("===guess-graph graph====");
	System.out.println(guess_graph.dumpGraph());
	visual3.display(guess_graph, "/tmp/cobra/fig/guess_graph.dot", true);
*/
	
			
			// 5.1 generate one set of possible WW orders
			ArrayList<LinkedList<WriteNode>> write_orders = permutate(ordered_perkey_sorter, prune_cond);
			// if we run out of choices, then audit failed.
			if (write_orders == null) {
				return false;
			}
			
			/*
			// print the guessed WW orders
			for (LinkedList<WriteNode> wlist : write_orders) {
				long key = wlist.get(0).getKey();
				System.out.println("[" + Long.toHexString(key) + "]");
				for(WriteNode w : wlist) {
					System.out.print("T["+Long.toHexString(w.getTxnid()) + "] -> ");
				}
				System.out.println();
			}
			*/
			
			// 5.2 incorporate this WW orders in precedence graph
			AddWOEdges(write_orders, guess_graph, readFromMapping);
			
			// 5.3 test if there is a cycle
			hasCycle = DFSCycleDetection.CycleDetection(guess_graph.getGraph());
			// get feedback from the cycle detector
			if (hasCycle) {
				// fetch the key and their hash from the perkey_topohash
				ArrayList<Long> found_cycle = DFSCycleDetection.getLastCycle();
				assert found_cycle != null;
				assert found_cycle.get(0).equals(found_cycle.get(found_cycle.size()-1)); // make sure it is a cycle
				// (1) find all keys from the cycle
				// FIXME: XXX: is the following correct?
				// "
				//  The relavant keys are those who shared between two adjunct txns
				// "
				Set<Long> keys = new HashSet<Long>();
				Set<Long> prev_txn_keys = new HashSet<Long>();
				for (int i=0; i<found_cycle.size()-1; i++) {
					TxnNode prev = guess_graph.getNode(found_cycle.get(i));
					TxnNode next = guess_graph.getNode(found_cycle.get(i+1));
					// add all the keys seen by prev txn
					for (OpNode op : prev.getOps()) {
						prev_txn_keys.add(op.key_hash);
					}
					// check if there are shared keys
					for (OpNode op : next.getOps()) {
						if (prev_txn_keys.contains(op.key_hash)) {
							keys.add(op.key_hash);
						}
					}
					// clear the set
					prev_txn_keys.clear();
				}
				// (2) get the topohash from these keys
				Map<Long,Long> fail_cond = new HashMap<Long,Long>();
				for (long k : keys) {
					long topohash = perkey_topohash.get(k);
					fail_cond.put(k,topohash);
					// victim-lay-back optimization
					if (!seen_key_set.contains(k)) {
						seen_key_set.add(k);
						TopologicalSorter s = key2sorter.get(k);
						pushBackAndResetTopo(ordered_perkey_sorter, s);
					}
				}
				prune_cond.addCondition(fail_cond);
			}
			
			// DONE!
			if (!hasCycle) return true;
		
/*			
	System.out.println("===after completion guess-graph graph====");
	System.out.println(guess_graph.dumpGraph());
	visual3.display(guess_graph, "/tmp/cobra/fig/after_completion_guess_graph.dot", true);

	System.out.println("Press any key to continue...");
	try {
		System.in.read();
	} catch (IOException e) {
		e.printStackTrace();
	}
	*/
	
	
		}
	}
	
	public static void main(String args[]) {
		long start = System.currentTimeMillis();
		OfflineRelaxLogVerifier xx = new OfflineRelaxLogVerifier("/tmp/cobra/log");
		boolean pass = xx.audit();
		
		if (pass) {
			System.out.println("OK");
		} else {
			System.out.println("SHIT");
		}
		
		System.out.println("TIME=>" +
				(System.currentTimeMillis() - start) + "ms"
		);
	}
	
	private void testCode() {
		/*
		int wcounter = 0;
		WriteGraph lalala = null;
		WriteGraph lala = null;
		long keyxxx = 0;
		for (WriteGraph wg : perkey_graphs.values()) {
			visual2.display(wg, "/tmp/cobra/fig/wgraph_"+(wcounter++)+".dot", false);
			if (wcounter == 2) {
				lalala = wg;
			}
			if (wcounter == 3) {
				lala = wg;
			}
		}
		for (WriteNode n : lalala.allNodes()) {
			if (n.numSuccessor()==2) {
				long[] succ_id = new long[2];
				int succ_counter = 0;
				for (WriteNode succ : lalala.successors(n)) {
					succ_id[succ_counter++] = succ.getWid();
				}
				lalala.deleteEdge(n.getWid(), succ_id[0]);
				lalala.deleteEdge(n.getWid(), succ_id[1]);
			}
		}
		for (WriteNode n : lala.allNodes()) {
			if (n.numSuccessor()==2) {
				long[] succ_id = new long[2];
				int succ_counter = 0;
				for (WriteNode succ : lala.successors(n)) {
					succ_id[succ_counter++] = succ.getWid();
				}
				lala.deleteEdge(n.getWid(), succ_id[0]);
				lala.deleteEdge(n.getWid(), succ_id[1]);
			}
		}
		visual2.display(lalala, "/tmp/cobra/fig/wgraph_1x.dot", false);
		TopologicalSorter lalalas = new TopologicalSorter(123,lalala);
		LinkedList<WriteNode> lalalaxx = lalalas.getOneTopologicalSort();
		while(lalalaxx!=null) {
			System.out.println(lalalaxx);
			lalalaxx = lalalas.getOneTopologicalSort();
		}
		
		lalalas.refresh();
		do {
			lalalaxx = lalalas.getOneTopologicalSort();
			System.out.println(lalalaxx);
		} while(lalalaxx!=null);
		
		while (true) {
			ArrayList<LinkedList<WriteNode>> woxxx = permutate(perkey_graphs);
			if (woxxx != null) {
				for (LinkedList<WriteNode> woxx : woxxx) {
					System.out.println("  " + woxx);
				}
				System.out.println("------");
			} else {
				System.out.println("---end---");
				break;
			}
		}
		if (true)
		return false;
		*/
	}
	
}
