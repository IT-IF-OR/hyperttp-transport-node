import type { HyperTransport, TransportListenOptions, TransportRequest, TransportResponse, TransportServer } from "@hyperttp/types";
import type { Http2TransportConfig } from "./http2-types.js";
/**
 * Native HTTP/2 transport for REST and gRPC.
 *
 * It owns HTTP/2 sessions and streams, forwards raw bytes, and preserves HTTP/2
 * response trailers separately from initial headers. Protocol packages retain
 * responsibility for REST semantics and gRPC framing/Protobuf handling.
 */
export declare class Http2Transport implements HyperTransport {
    readonly protocols: readonly ["rest", "grpc"];
    private readonly config;
    private readonly sessions;
    private readonly servers;
    private readonly lifecycle;
    private closed;
    private closePromise?;
    constructor(config?: Http2TransportConfig);
    supports(protocol: string): boolean;
    /** Executes one gRPC HTTP/2 request and returns its complete raw framed body. */
    execute(request: TransportRequest): Promise<TransportResponse>;
    /** Starts an HTTP/2 gRPC server and forwards each raw stream to onRequest. */
    listen(options: TransportListenOptions): Promise<TransportServer>;
    /** Gracefully closes listeners and HTTP/2 client sessions. */
    close(): Promise<void>;
    /** Aborts active client requests and forcefully closes all HTTP/2 resources. */
    destroy(): Promise<void>;
    private getSession;
    private executeOnSession;
    private handleServerStream;
    private sendServerResponse;
    private shutdown;
}
//# sourceMappingURL=http2.d.ts.map