// Package daemon runs one controller term (DECISIONS D1301, D1312–D1337).
// It campaigns in the home region, writes the AgentRecord once, and queues
// later writes by region. Each region is fenced on its first write (D1305).
// One mirror per instance feeds the global view; a region that has not
// finished its first list is not written (D1317, D1318). Each reconcile
// derives N1 and puts only the keys that differ (D1308, D1322–D1327), fills
// bindings and start, and ends a session whose dependencies failed or timed
// out (N2, N3, D1328–D1332). A deps deadline wakes that loop without waiting
// for the full resync (D1331). It then deletes what N4 and N5 remove (D1309,
// D1310, D1333–D1335). A round hands its writes to the region queues and
// does not wait for them, so a region whose write hangs holds neither the
// loop nor the other regions (D1306, D1337).
//
// Put and Delete are the test write entry (D1313). The mftiko-controller
// process does not call them.
package daemon
