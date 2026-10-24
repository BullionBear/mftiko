// Package daemon runs one controller term (DECISIONS E13-D001, E13-D012–E13-D037).
// It campaigns in the home region, writes the AgentRecord once, and queues
// later writes by region. Each region is fenced on its first write (E13-D005).
// One mirror per instance feeds the global view; a region that has not
// finished its first list is not written (E13-D017, E13-D018). Each reconcile
// derives N1 and puts only the keys that differ (E13-D008, E13-D022–E13-D027), fills
// bindings and start, and ends a session whose dependencies failed or timed
// out (N2, N3, E13-D028–E13-D032). A deps deadline wakes that loop without waiting
// for the full resync (E13-D031). It then deletes what N4 and N5 remove (E13-D009,
// E13-D010, E13-D033–E13-D035). A round hands its writes to the region queues and
// does not wait for them, so a region whose write hangs holds neither the
// loop nor the other regions (E13-D006, E13-D037).
//
// Put and Delete are the test write entry (E13-D013). The mftiko-controller
// process does not call them.
package daemon
