// Package artifact is the OCI artifact format shared by mftiko-pack and the agents (E09-D002, E00-D006, PROTOCOL §8).
//
// PackStrategy and Push build a reproducible single-layer strategy image and upload it (E09-D035–E09-D041).
// The manifest and what a shim Launch takes from it are the same types (PROTOCOL §4.6, §8.2).
// Pull fetches one worker artifact by digest, checks it, and leaves each verified layer at
// ${DATA}/layers/<digest> (PROTOCOL §8.3 steps 1 and 2, E09-D028–E09-D034).
// PullData fetches one data artifact the same way, checking the data media types (PROTOCOL §8.4, E09-D072).
// PackData and Client.PushData pack a session volume and stream it up (E09-D079–E09-D081). Push is unchanged:
// it still uploads a strategy image from memory with Docker config credentials.
// Unpack extracts gzip-compressed tar layers into a directory (E09-D051–E09-D053, E09-D057). UnpackImage installs
// them at ${DATA}/artifacts/<digest>/rootfs and records the layer list used for GC (E09-D050, E09-D055).
// UnpackImageFrom also records the repository beside a rootfs it installs (E12-D011). A rootfs that is already
// there is reused and its record is left as it is.
// GC deletes every other rootfs, then layers no remaining rootfs references (E09-D056). An optional
// second set keeps input layers that are still being pulled or unpacked. HoldLayers keeps digests
// a caller is still using, including strategy layers between Pull and UnpackImage (E09-D073).
// Catalog lists tags, resolves a tag to a digest, and reads a worker or data manifest without
// downloading layers (E10-D056, E10-D012). Pull and PullData are unchanged: STS still fetches by digest.
package artifact
