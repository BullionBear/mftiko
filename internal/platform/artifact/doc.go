// Package artifact is the OCI artifact format shared by mftiko-pack and the agents (D902, D006, PROTOCOL §8).
//
// PackStrategy and Push build a reproducible single-layer strategy image and upload it (D935–D941).
// The manifest and what a shim Launch takes from it are the same types (PROTOCOL §4.6, §8.2).
// Pull fetches one worker artifact by digest, checks it, and leaves each verified layer at
// ${DATA}/layers/<digest> (PROTOCOL §8.3 steps 1 and 2, D928–D934).
// PullData fetches one data artifact the same way, checking the data media types (PROTOCOL §8.4, D972).
// Unpack extracts gzip-compressed tar layers into a directory (D951–D953, D957). UnpackImage installs
// them at ${DATA}/artifacts/<digest>/rootfs and records the layer list used for GC (D950, D955).
// GC deletes every other rootfs, then layers no remaining rootfs references (D956). An optional
// second set keeps input layers that are still being pulled or unpacked (D973).
// Resolving a tag to a digest is not done here (E10).
package artifact
