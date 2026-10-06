package slogbetterstack

// name identifies this library in the User-Agent header and in the "logger.name" field of every
// record. It stays "<GitHub owner>/<repository>", the convention the library has always used.
const name = "BetterStackHQ/slog-betterstack"

// version is the latest released version and is sent in the "logger.version" field of every
// record. The Release workflow bumps it and tags the commit, so edit it only there.
const version = "1.4.4"

// userAgent identifies the library and its version in every request, like the other Better
// Stack clients do.
const userAgent = name + "/" + version
