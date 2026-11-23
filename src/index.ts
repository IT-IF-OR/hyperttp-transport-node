declare module "@hyperttp/types" {
  interface HyperClientOptions {
    /** Prefer HTTP/2 transport selection for REST and gRPC without modifying global fetch. */
    http2?: boolean;
  }
}

export { FetchTransport } from "./fetch.js";
export { Http2Transport, Http2Transport as GrpcTransport } from "./http2.js";
export type { FetchTransportOptions } from "./fetch.js";
export type { Http2ServerTlsOptions, Http2TransportConfig } from "./http2-types.js";
