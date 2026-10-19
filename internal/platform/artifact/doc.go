// Package artifact is the OCI artifact format shared by mftiko-pack and the STS host: strategy artifacts (D902) and data artifacts (D904), PROTOCOL §8.
//
// PackStrategy and Push build a reproducible single-layer strategy image and upload it (D935–D941).
// The manifest and what a shim Launch takes from it are the same types (PROTOCOL §4.6, §8.2).
package artifact
