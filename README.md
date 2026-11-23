# @hyperttp/transport-node

Native Node.js transport for Hyperttp.

- `FetchTransport` uses global `fetch` for client I/O and `node:http` for HTTP/1.1 listeners.
- `Http2Transport` uses `node:http2` for multiplexed REST and gRPC sessions, TLS and trailers.

```ts
import { Http2Transport } from "@hyperttp/transport-node";

const transport = new Http2Transport({
  baseUrl: "https://api.example.test",
  serverTls: { key, cert },
});
```

`Http2Transport` supports `rest` and `grpc` on one listener. It preserves HTTP/2
trailers as `TransportResponse.trailers`; gRPC framing and Protobuf codecs stay
in `@hyperttp/protocol-grpc`. `GrpcTransport` is exported as a compatibility
alias.

Node.js ≥20 is supported. Bun can use this transport through its `node:http2`
compatibility API; run the integration tests on the exact Bun release used in
production.
