// Package volume is how a strategy reads and writes its own files (DECISIONS E09-D004, E08-D051, E08-D052).
//
// Write creates a temp file in the destination directory, syncs it, renames it onto the
// name, and syncs the directory. A crash before the rename leaves the previous file, or
// no file. The temp file may remain; the destination is never a partial write.
//
// Read looks in the volume directory first, then the input directory, and reports ok
// false when neither has the file. An empty directory does not fall through to the
// process's current directory. The SDK does not call Read on startup; the strategy does.
package volume
