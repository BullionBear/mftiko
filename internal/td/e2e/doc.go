// Package e2e runs TD end to end against a fake venue (E6-11): the td-agent with its shims and
// account workers, etcd, NATS, and a session that speaks td.order, td.evt and td.view on NATS
// directly. The venue is exchangetest.AccountVenue, served over loopback HTTP from the test process,
// so the test can drop its private stream while the worker runs in another process.
//
// It holds only tests; the epic's completion criteria (#8) each have one here.
package e2e
