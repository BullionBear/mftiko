// Package artifact is the OCI artifact format shared by mftiko-pack and the agents (D902, D006, PROTOCOL §8).
//
// PackStrategy and Push build a reproducible single-layer strategy image and upload it (D935–D941).
// The manifest and what a shim Launch takes from it are the same types (PROTOCOL §4.6, §8.2).
// Pull fetches one worker artifact by digest, checks it, and leaves each verified layer at
// ${DATA}/layers/<digest> (PROTOCOL §8.3 steps 1 and 2, D928–D934). Unpacking and GC are separate (E9-03).
// Resolving a tag to a digest is not done here (E10).
package artifact
