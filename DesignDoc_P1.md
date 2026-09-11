# DSFS Design Document

## 1) Introduction

In this document, I will describe the design of the gRPC-based file system implementation in fs, and the corresponding non-gRPC variant in fs_wo_grpc.

Basically, I system implemented a small distributed file service with client-side caching, version validation, and write-through-on-close behavior.


## 2) Goals 

### Goals
- The fundamental goal was to provide simple remote file operations: create, open, read, write, close.
- I also wanted to reduce network transfer by using client cache with version checking.
- Also, I wanted to ensure deterministic write commit semantics: writes are committed on `close`.
- Since I had to use this with the later part, the API had to be easy to integrate with worker/coordinator programs.

---

## 3) High-Level Architecture

### 3.1 gRPC version (`fs`)

**Components**
- **Server**: 
	- It serves as the main storage site for the files and owns authoritative file data under `./server_data`.
	- It also tracks open handles, versions, and lock state.
	- It communicates with the clients via gRPC.
- **Client stub**: 
	- It mainly serves as the gRPC transport wrapper which the actual clients use in communicating with server.
	- It maintains local cache under `./temp_cache_files`.
	- Also, it exposes Python methods (`create`, `open`, `read`, `write`, `close`, etc.) so that the actual client can perform these operations with the file system.
- **Proto contract**:
	- Here, I define service RPCs and request/response messages for various supported operations.

**Data flow**
1. Client checks version (`TestVersionNumber`) if cache metadata exists.
2. Client calls `Open(filename, mode, fetch_data)`.
3. If cache is stale/missing, server returns full file bytes; otherwise transfer is skipped.
4. Client reads/writes local cached copy.
5. On `Close` in write mode, client sends full file bytes to server; server overwrites file and bumps version.

### 3.2 Non-gRPC version (`fs_wo_grpc`)

**Components**
- **Server**: 
	- Same core file/version semantics as gRPC server.
- **Custom RPC Transport**: 
	- I perform the communication via TCP socket.
	- The data is Pickle serialized followed by Fernet encryption.
	- Also, I add the length prefix to the framed messages.
- **Client stub**: 
	- Same high-level API as gRPC client stub.
	- Adds reconnect-and-retry behavior on network failures.


## 4) API and Semantics

## 4.1 Core operations (both variants)
- `create(filename)`
	- Creates empty file on server.
	- Initializes version to `1`.
	- Returns file handle (which is an int mapping to file object on server) and error messages if any.
- `test_version_number(filename)`
	- Returns current server version.
- `open(filename, mode, fetch_data)`
	- Valid modes: `r`, `w`.
	- Returns opaque file handle.
	- It also transfers the entire file and the version number if requested by `fetch_data`.
- `read(handle, offset, length)`
	- Reads from local cached file.
	- Since I cache the entire file, the offset and length are handled locally.
- `write(handle, offset, data)`
	- Writes to local cached file.
	- Accepts bytes or UTF-8-encodable string.
	- Again, since I cache the entire file, the offset and length are handled locally.
- `close(handle)`
	- In `w` mode, sends full file content back to server and updates version.
	- In `r` mode, only releases handle.

- I also provide a lock file semantic, which is not very useful here.

### 4.2 Cache consistency model
- Cache is version validated at open time.
- For this, I first check the local version number (stored in filename extension part) and fetch the server version.
- If local version == server version and cache file exists, download is skipped.
- After successful write-close, local cache metadata moves to new version.
- Older cached versions are deleted per file.


## 5) Concurrency and Safety

### 5.1 Server-side synchronization
- Server uses a global lock (`state_lock`) to guard:
	- handle allocation,
	- `open_files` map,
	- `file_versions` map,
	- lock/notification state (gRPC variant).

### 5.2 Server File Handle model
- Handle IDs are server-generated monotonically increasing integers.
- Client stores `handle -> {path, filename}` mapping locally.
- Invalid handles are rejected.

## 6) Locking (gRPC `fs` only)

In order to support multiple writer, I also decided to include file locking. However, it is not ised in my implementayion, I have just included it as an extra feature. So, the gRPC implementation extends baseline FS operations with timed write locks:
- `LockFile(filename, timeout_seconds, client_id)`
- `UnlockFile(filename, client_id)`
- `GetLockNotifications(client_id)`

So, the following properties are ensured:
- Write `open` and write `close` are blocked while lock is active.
- Locks auto-expire via timer thread.
- Owner receives notification messages for timeout/unlock events.
- Unlock allowed only for lock owner.

This mechanism is absent in fs_wo_grpc. Also, since it is an extra feature.

## 7) End-to-End Workflow

Now, in this section, I describe the runtime workflow step-by-step.

### 7.1 Startup workflow

1. Start server.
2. Server initializes state such as `open_files`, `file_versions`, handle counter, and lock structures (gRPC only).
3. Client creates stub and local cache directory.

### 7.2 Read workflow

1. Client calls `open(filename, mode='r')`.
2. If cache metadata exists, client checks server version via `test_version_number`.
3. If cache is valid and cache file exists, client requests `open(..., fetch_data=False)`.
4. Otherwise, client requests `open(..., fetch_data=True)` and receives full bytes.
5. Client stores/updates cached version file and records `handle -> local path`.
6. Client serves `read(handle, offset, length)` from local cache file.
7. Client calls `close(handle)` with no server data overwrite for read mode.

### 7.3 Write workflow

1. Client calls `open(filename, mode='w')` (with cache validation same as read path).
2. Client performs `write(handle, offset, data)` locally on cached copy.
3. On `close(handle)`:
	- client reads full local file bytes,
	- sends bytes to server close RPC,
	- server overwrites authoritative file,
	- server increments version,
	- client updates cache metadata to new version.
4. Any later opener with old cache sees version mismatch and refreshes.

### 7.4 Locking workflow (gRPC only)

1. Client requests `LockFile(filename, timeout_seconds, client_id)`.
2. Server grants lock if currently free; stores owner + expiry + timer.
3. While lock is active, write `open`/`close` on that file are rejected.
4. Lock is released by:
	- explicit `UnlockFile` from owner, or
	- automatic timeout callback.
5. Client polls `GetLockNotifications(client_id)` for timeout/unlock messages.

This is an extra feature and in my later implementation, I am not using these locks. 

### 7.5 Non-gRPC transport workflow

1. Client packages request dictionary: `{method, args}`.
2. Request is serialized (`pickle`), encrypted by Fernet method, length-prefixed, and sent via TCP socket.
3. Server receives frame, decrypts, deserializes, dispatches to logic implementation.
4. Server builds response dict, serializes/encrypts/sends back.
5. Client decrypts/deserializes response and returns result to caller.
6. On timeout/socket/decrypt errors, client reconnects and retries once through the same API path.

### 7.6 Failure workflow

1. Invalid file handle -> operation fails immediately with error.
2. Missing file on open/create conflict -> server returns `success=False` with reason.
3. Non-gRPC connection loss -> client reconnect path attempts recovery.
4. Cache file missing but metadata present -> client treats as cache miss and refetches.

