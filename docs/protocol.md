# Plugin worker protocol, version 1

This document describes the frames a Bazarr host and a plugin worker exchange.
It covers what the SDK implements in version 0.1.0. A host that talks to a
worker must follow the same rules. Plugin authors do not need this page to
write a plugin; see the README for that.

## Processes and descriptors

The host starts the worker as a child process and passes two descriptors:

| Variable | Direction | Meaning |
| --- | --- | --- |
| `BAZARR_PLUGIN_FD_IN` | host to worker | Frames the worker reads. |
| `BAZARR_PLUGIN_FD_OUT` | worker to host | Frames the worker writes. |

Both values are decimal descriptor numbers from 3 to 2147483647. The worker
exits with status 2 when either variable is missing or malformed, when a value
is not an open descriptor, or when a value names a directory.

Standard input is a liveness pipe. The host keeps it open for the life of the
worker. When standard input reaches end of file, the worker exits with status
0, whether or not `hello` has been received. A failed read on standard input
exits with status 2. The worker never reads frames from standard input. It
discards any bytes that arrive there.

Standard output is redirected to standard error from the moment `serve()`
starts. A plugin's `print` output and log records go to standard error, so a
worker writes nothing to standard output once `serve()` has started. Output
written before `serve()` is called is not a frame either. The host must not
parse standard output or standard error. Standard error is free text for
people.

The protocol is defined for POSIX descriptors. Passing descriptors on Windows
is not verified in version 0.1.0.

## Host obligations

The host must always drain the worker's frame stream (`BAZARR_PLUGIN_FD_OUT`),
even when it has nothing to ask. A host that stops reading blocks every write
the worker makes, including the error frame that answers a cancel and the
writes made during a shutdown. The worker cannot send anything until the host
reads again.

## Framing

Every frame is:

1. A four-byte unsigned big-endian length `N`.
2. `N` bytes of UTF-8 encoded JSON. The JSON value is an object.

A length of zero is an error. The JSON must not contain `NaN`, `Infinity` or
`-Infinity`. A payload that the JSON decoder refuses, such as an integer with
more digits than Python converts, is a malformed frame. Every frame object has:

- `t`: the frame type, a string.
- `op`: the operation id. It is text of at most 256 characters that encodes as
  UTF-8 without lone surrogates. Operation frames need a non-empty `op`.
  Connection frames (`hello`, `hello_ack`, `shutdown`) must use the empty
  string. Any other value is a malformed frame.

Request ids (`req`) follow the same rule as `op`: text of at most 256
characters that encodes as UTF-8. A malformed id is a malformed frame.

### Nesting

Arrays and objects may nest at most 64 levels deep, and the frame object is
level 1. A frame that nests deeper is malformed. The worker checks this when it
reads a frame and before it encodes one, so it never sends a frame that it
would refuse to read. A malformed frame from the host ends the worker with
status 76.

### Size limits

| Limit | Default | Applies to |
| --- | --- | --- |
| `control_frame_bytes` | 65,536 (64 KiB) | JSON payload of every frame except `chunk`. A result value is part of its `result` frame. |
| `chunk_bytes` | 1,048,576 (1 MiB) | Decoded bytes in one `chunk` frame. |
| `request_body_bytes` | 16 MiB | Decoded size of a call body. |
| `result_body_bytes` | 16 MiB | Upper bound on a result body. |
| `result_floor_bytes` | 1 MiB | Lower bound of the result limit. |
| `result_factor` | 4 | Multiplier applied to the request body length. |
| `checkpoint_bytes` | 8 MiB | Decoded size of one checkpoint. |

The JSON payload of a `chunk` frame may be as large as the base64 text of
`chunk_bytes` plus `control_frame_bytes`.

The reader checks the length prefix against the channel limit before it reads
the payload. The channel limit is `chunk_frame_bytes`, the largest payload any
frame may have: 1,463,640 bytes with the default limits. A length above it is
refused without reading the body. A control frame whose length lies between its
own 64 KiB limit and the channel limit is read in full and then refused, so its
body is read before the refusal. Either way the reader treats the stream as
broken and the worker exits with status 76.

The host sends the limits it uses in `hello`. A worker may be sent smaller
limits than the defaults. Limits above the defaults make `hello` malformed and
the worker exits with status 76. All seven limit fields must be present and
must be positive integers. No other field is allowed.

The result limit for one call is `min(result_body_bytes, max(result_floor_bytes,
result_factor * request_body_length))`. A result body larger than this limit is
replaced by an `internal` error whose message is exactly:

```text
the result body exceeds the result limit
```

## Bodies

A body that does not fit in one frame travels as chunk frames:

```json
{"t": "chunk", "op": "7", "of": "result", "seq": 0, "last": false, "data": "<base64>"}
```

- `of` names the frame that owns the body: `call`, `checkpoint` or `result`.
- `seq` starts at 0 and counts up by one.
- `last` is true on the final chunk only.
- `data` is standard base64 with padding.
- `req` is present only when the frame that the body belongs to carries a
  request id.

An empty body is one chunk with empty `data` and `last` true. Chunks of one
body follow the frame they belong to, in order. The worker writes that frame
and its chunks under one lock, so no other frame falls between them. A `cancel`
for the same operation may arrive while a call body is still arriving; the
worker holds the cancel until the body is complete.

## Connection

### hello (host to worker)

The first frame on the channel. Any other first frame is a protocol error.

```json
{
  "t": "hello", "op": "", "protocol": 1, "kind_api": "example.kind.v1",
  "limits": {"control_frame_bytes": 65536, "chunk_bytes": 1048576,
             "request_body_bytes": 16777216, "result_body_bytes": 16777216,
             "result_floor_bytes": 1048576, "result_factor": 4,
             "checkpoint_bytes": 8388608},
  "heartbeat_interval": 10,
  "max_in_flight": 1,
  "folders": {"data_dir": "/abs/data", "temp_dir": "/abs/temp", "artifacts_dir": "/abs/artifacts"}
}
```

- `protocol` must be the integer `1`. A missing value, a boolean, a float such
  as `1.0`, or text is a malformed hello (status 76). An integer other than `1`
  is a version the worker does not serve (status 78).
- `kind_api` must equal the `kind_api` the worker class declares.
- `heartbeat_interval` is a finite number of seconds above zero. The worker
  stores it and exposes it to plugin code. The worker never sends heartbeats on
  its own. A value that is not a number, is zero or below, is not finite (such
  as `1e400`), or is too large to convert to a float is a malformed hello
  (status 76).
- `max_in_flight` is required. It is an integer of 1 or more. A hello without
  it is malformed and the worker exits with status 76.
- Each folder value is an absolute path. The folders are fixed for the life
  of the worker.

### hello_ack (worker to host)

Sent after `hello` is accepted and the plugin object is created.

```json
{
  "t": "hello_ack", "op": "", "protocol": 1, "sdk_version": "0.1.0",
  "kind_apis": ["example.kind.v1"], "operations": ["search", "fetch"],
  "folders": {"data_dir": "/abs/data", "temp_dir": "/abs/temp", "artifacts_dir": "/abs/artifacts"},
  "environment": {"TMPDIR": "/abs/tmp"}
}
```

- `operations` lists the method names the worker class declares.
- `environment` contains the values of `TMPDIR`, `TMP` and `TEMP` that the
  worker inherited. Variables that are not set are omitted.

### Exit statuses

| Status | Meaning |
| --- | --- |
| 0 | Orderly shutdown; end of the frame stream, before or after `hello`; end of standard input, before or after `hello`; the host pipe closed while the worker wrote a frame, including `hello_ack`. |
| 2 | The environment is wrong: a descriptor variable is missing, malformed, above 2147483647, not open or a directory; the class sets no `kind_api`; standard output cannot be redirected to standard error; the plugin constructor raised; or a read fails on the frame descriptor or on standard input. |
| 76 | Protocol error: the first frame is not `hello`; `hello` is malformed (including a `protocol` that is not an integer, a `heartbeat_interval` that is not a finite number above zero, or a missing `max_in_flight`); a frame is malformed, nests too deep, has an id that is too long, or is too large; a frame type is not allowed in this direction; an operation id is started twice; or a result cannot be encoded; or the frame reader fails with an unexpected error. |
| 78 | The host asked for a protocol version or a kind this worker does not serve. |

A read error is never a clean exit: a failed read on either descriptor exits
with status 2. The worker writes a short reason to standard error before
exiting with a non-zero status.

## Calls (host to worker)

### call

```json
{"t": "call", "op": "7", "method": "search", "params": {"q": "x"},
 "secrets": {"api_key": "..."}, "budget": {"max_cost": 0.5}, "body": false}
```

- `op` is chosen by the host and is unique among the operations in flight.
  Starting an operation id that is still active is a protocol error (status 76).
- `method` must be one of the names in `hello_ack.operations`. Any other name
  produces an `internal` error for that operation.
- `params` and `budget` are JSON objects. `secrets` maps names to text.
- When `body` is true, the body follows as chunks with `of` set to `call`. The
  worker passes the decoded body to the operation.
- A `call` that arrives while `max_in_flight` operations are active produces
  an `internal` error for that operation. The worker stays up.

### cancel

```json
{"t": "cancel", "op": "7", "abandon": false}
```

- Sets the operation's `cancelled` flag. The plugin observes the flag through
  its context. An `async` operation that is awaiting a context call is
  interrupted. A `def` operation keeps running on its thread after the cancel
  and is never joined. The host receives the `cancelled` error at once, and
  the thread's later context calls are refused (see Ordering). A plain `def`
  function that ignores cancellation keeps running until it returns. The
  worker does not wait for it: a shutdown ends the process at its time bound
  (see shutdown).
- With `abandon` true the worker also sets the abandoned flag. The host must
  then ignore every later frame for that operation id. The worker stops the
  operation at its next context call and sends no further result frame for it.
  Its `error` frame, if any, may still be sent and the host discards it.
- A cancel for an operation that is not active is ignored.
- `abandon` is optional and defaults to false. Any other type is a protocol
  error.

### ack (host to worker)

The reply to an awaited worker frame. It carries the `req` the worker chose.

```json
{"t": "ack", "op": "7", "req": "3", "ok": true, "reservation": "r-1"}
{"t": "ack", "op": "7", "req": "4", "ok": false, "error": {"code": "quota_exhausted", "message": "..."}}
```

- `req` is the string the worker sent in the awaited frame. An ack for an
  unknown `req` is ignored.
- `ok` true means accepted. `reservation` is a string or null and is used
  only with `reserve`.
- When `ok` is false, `error.code` is one of the error codes below. An unknown
  code is treated as `internal`.

### shutdown (host to worker)

```json
{"t": "shutdown", "op": ""}
```

The worker cancels its active operations and waits up to two seconds for them.
The two seconds cover the whole teardown, including the asyncio cleanup that
follows. A watchdog started at shutdown has the same bound: when it runs out,
the worker flushes standard output and standard error and ends the process
with the status recorded so far. After an orderly shutdown that status is 0.
Operations still running at that point, such as a `def` function that ignores
cancellation, are ended with the process.

### Frames not used in version 1

`log` and `host_call` are reserved names. The worker never sends them and the
host must not send them. A worker that receives any frame type not listed in
this document exits with status 76.

## Worker frames

### progress

```json
{"t": "progress", "op": "7", "done": 3, "total": 10, "message": "fetching"}
```

`done` is a non-negative integer. `total` is a non-negative integer or null.
`message` is text. No answer is sent. The worker does not send a `progress`
frame for an operation after that operation's final frame (see Ordering).

### heartbeat

```json
{"t": "heartbeat", "op": "7"}
```

No answer is sent. The host chooses the heartbeat interval in `hello`. The
recommended value is 10 seconds. The worker sends heartbeats only when plugin
code asks for one.

### reserve (awaited)

```json
{"t": "reserve", "op": "7", "req": "3", "amount": 0.25, "model": "m1"}
```

`amount` is a finite non-negative number. The host answers with an `ack`. The
reservation id in the ack is returned to plugin code.

### usage (awaited)

```json
{"t": "usage", "op": "7", "req": "4", "tokens": 1200, "cost": 0.01,
 "model": "m1", "reservations": ["r-1"]}
```

`tokens` is a non-negative integer. `cost` is a finite non-negative number.
`reservations` is a list of text values. The host answers with an `ack`.

### checkpoint (awaited)

```json
{"t": "checkpoint", "op": "7", "req": "5", "body": true}
```

followed by chunks with `of` set to `checkpoint` and the same `req`. A
checkpoint may be at most `checkpoint_bytes`. The host answers with an `ack`
after storing the body.

### result

```json
{"t": "result", "op": "7", "value": {"matches": []}, "body": false}
```

`value` is a JSON object or null. When `body` is true, the body follows as
chunks with `of` set to `result`. A result is sent at most once per operation.
A value too large for one control frame becomes an `internal` error with the
message `the result is too large to send`. A value that cannot be encoded as
JSON ends the worker with status 76, because the host cannot be answered.

### error

```json
{"t": "error", "op": "7", "code": "invalid_config",
 "message": "bad field", "field_errors": {"api_key": "missing"}}
```

- `code` is one of the nine codes below.
- `message` is at most 500 characters.
- `field_errors` is present only with `invalid_config`. It maps field names to
  messages.
- `retry_after` is present only with `rate_limited`. It is a finite
  non-negative number of seconds.
- `reason` is present only with `declined`. It is a short text token.

If the error frame does not fit in one control frame, the worker sends an
`internal` error with the message "the error details are too large to send".

## Error codes

| Code | Meaning |
| --- | --- |
| `auth_required` | The host must obtain or refresh credentials before the call can succeed. |
| `invalid_config` | The configuration is wrong. Carries `field_errors`. |
| `rate_limited` | The upstream service asked for a pause. Carries `retry_after`. |
| `quota_exhausted` | A budget or quota is used up. |
| `dependency_unavailable` | A service the plugin needs is down or unreachable. |
| `declined` | The plugin will not handle this request. Carries `reason`. |
| `cancelled` | The host cancelled the operation. |
| `partial` | Some work completed and some did not. |
| `internal` | Any other failure. The worker sends the exception class name and text, cut to 500 characters. The traceback goes to standard error. |

An exception that is not a plugin error becomes an `internal` error. Plugin code
that wants the host to see a specific code raises the matching error.

The exception text is forwarded to the host, and the traceback, which contains
the same text, goes to standard error. Plugin code must keep secrets out of
exception messages.

## Ordering

- An operation sends at most one `result` or `error` frame. Once that final
  frame is sent, the worker refuses every later frame for the same operation
  id: `progress`, `heartbeat`, `reserve`, `usage` and `checkpoint` raise
  `OperationAbandoned` in plugin code and nothing is written. The worker frees
  the id at that point, so a later call may reuse it.
- Awaited frames (`reserve`, `usage`, `checkpoint`) are answered in any order,
  each matched by `req`.
- Frames for different operation ids may interleave, except that a body's
  chunks stay together.

## Timing values

The SDK does not enforce these values. They are host policy and are listed so
that plugin authors know what to expect:

- Heartbeat interval: set by the host in `hello`. The recommended value is 10
  seconds.
- Cancel grace period: the time a host waits after `cancel` before it sends
  `abandon`. The recommended value is 15 seconds.
- Stall limit: the time without any frame for one operation after which a host
  treats the operation as stalled. The recommended value is 30 seconds.
