import type { ConnectionOptions } from "node:tls";
/** TLS settings for an HTTP/2 server. Both key and cert are required. */
export interface Http2ServerTlsOptions {
    readonly key: string | Buffer;
    readonly cert: string | Buffer;
    readonly ca?: string | Buffer | (string | Buffer)[];
    readonly passphrase?: string;
    readonly requestCert?: boolean;
    readonly rejectUnauthorized?: boolean;
}
/**
 * Configuration for the native Node.js HTTP/2 gRPC transport.
 *
 * The transport deliberately has no gRPC or Protobuf runtime dependency:
 * framing/serialization stays in the Hyperttp protocol package.
 */
export interface Http2TransportConfig {
    /** Base URL used when TransportRequest.url is a relative path. */
    readonly baseUrl?: string;
    /** Request timeout in milliseconds. Zero disables the timeout. Default: 30 seconds. */
    readonly timeout?: number;
    /** Maximum buffered response size in bytes. Default: 16 MiB. */
    readonly maxResponseBytes?: number;
    /** Maximum buffered REST request body size in bytes. Default: 10 MiB. */
    readonly maxBodyBytes?: number;
    /** Maximum concurrent HTTP/2 streams advertised by the server. */
    readonly maxConcurrentStreams?: number;
    /** Native TLS options forwarded to node:http2 for HTTPS client sessions. */
    readonly clientTls?: ConnectionOptions;
    /** Optional TLS configuration for listen(). Without it, the server uses h2c. */
    readonly serverTls?: Http2ServerTlsOptions;
}
//# sourceMappingURL=http2-types.d.ts.map