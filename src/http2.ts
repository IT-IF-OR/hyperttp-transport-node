import { connect, createSecureServer, createServer, constants } from "node:http2";
import type {
  ClientHttp2Session,
  ClientHttp2Stream,
  Http2SecureServer,
  Http2Server,
  OutgoingHttpHeaders,
  ServerHttp2Session,
  ServerHttp2Stream,
} from "node:http2";
import type {
  HyperTransport,
  TransportListenOptions,
  TransportRequest,
  TransportResponse,
  TransportServer,
} from "@hyperttp/types";
import type { Http2TransportConfig } from "./http2-types.js";

const DEFAULT_TIMEOUT = 30_000;
const DEFAULT_MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
const DEFAULT_MAX_BODY_BYTES = 10 * 1024 * 1024;
const GRPC_TRAILER_NAMES = new Set(["grpc-status", "grpc-message", "grpc-status-details-bin"]);
const FORBIDDEN_HTTP2_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-connection",
  "transfer-encoding",
  "upgrade",
  "host",
]);

type GrpcHttp2Server = Http2Server | Http2SecureServer;
type Http2ClientStream = ClientHttp2Stream;
type Http2ServerStream = ServerHttp2Stream;
type Http2Session = ClientHttp2Session;
type Http2ServerSession = ServerHttp2Session;
type Http2Writable = Http2ClientStream | Http2ServerStream;
type HeaderRecord = Record<string, string | string[]>;
type RawHeaderValue = string | number | boolean | readonly string[] | undefined;
type RawHeaders = Record<string, RawHeaderValue>;

interface RunningServer {
  readonly server: GrpcHttp2Server;
  readonly sessions: Set<Http2ServerSession>;
  readonly stop: (force?: boolean) => Promise<void>;
}

interface CombinedSignal {
  readonly signal: AbortSignal;
  dispose(): void;
}

function abortError(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  return new DOMException("The operation was aborted", "AbortError");
}

function combineSignals(
  signals: readonly (AbortSignal | undefined)[],
  timeout: number,
): CombinedSignal {
  const controller = new AbortController();
  const cleanups: Array<() => void> = [];

  for (const signal of signals) {
    if (!signal) continue;
    if (signal.aborted) {
      controller.abort(signal.reason);
      break;
    }

    const onAbort = (): void => controller.abort(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    cleanups.push(() => signal.removeEventListener("abort", onAbort));
  }

  if (timeout > 0 && !controller.signal.aborted) {
    const timer = setTimeout(() => {
      const error = new Error(`gRPC request timed out after ${timeout} ms`);
      error.name = "TimeoutError";
      controller.abort(error);
    }, timeout);
    cleanups.push(() => clearTimeout(timer));
  }

  return {
    signal: controller.signal,
    dispose() {
      for (const cleanup of cleanups) cleanup();
    },
  };
}

function isReadableStream(value: unknown): value is ReadableStream<Uint8Array> {
  return (
    typeof value === "object" &&
    value !== null &&
    "getReader" in value &&
    typeof value.getReader === "function"
  );
}

function isAsyncIterable(value: unknown): value is AsyncIterable<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    Symbol.asyncIterator in value &&
    typeof value[Symbol.asyncIterator] === "function"
  );
}

function toBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (typeof value === "string") return Buffer.from(value);
  throw new TypeError("gRPC stream chunks must be strings, ArrayBuffers, or Uint8Arrays.");
}

function toHeaderRecord(headers: RawHeaders): HeaderRecord {
  const result: HeaderRecord = Object.create(null) as HeaderRecord;

  for (const [rawName, rawValue] of Object.entries(headers)) {
    if (rawValue === undefined || rawName.startsWith(":")) continue;
    const name = rawName.toLowerCase();
    if (FORBIDDEN_HTTP2_HEADERS.has(name)) continue;
    if (Array.isArray(rawValue)) {
      result[name] = rawValue.map(String);
    } else if (rawValue !== undefined) {
      result[name] = String(rawValue);
    }
  }

  return result;
}

function assignOutgoingHeaders(
  target: OutgoingHttpHeaders,
  source: Readonly<Record<string, string | string[]>>,
): void {
  for (const [rawName, rawValue] of Object.entries(source)) {
    const name = rawName.toLowerCase();
    if (name.startsWith(":") || FORBIDDEN_HTTP2_HEADERS.has(name)) continue;
    target[name] = Array.isArray(rawValue) ? [...rawValue] : rawValue;
  }
}

function asHeaderString(value: RawHeaderValue): string | undefined {
  if (value === undefined) return undefined;
  return Array.isArray(value) ? value.map(String).join(",") : String(value);
}

async function writeBody(
  stream: Http2Writable,
  body: unknown,
  signal?: AbortSignal,
  endStream = true,
): Promise<void> {
  const writeChunk = async (chunk: Uint8Array): Promise<void> => {
    if (signal?.aborted) throw abortError(signal);
    if (!stream.write(chunk)) {
      await new Promise<void>((resolve, reject) => {
        const onDrain = (): void => {
          cleanup();
          resolve();
        };
        const onError = (error: Error): void => {
          cleanup();
          reject(error);
        };
        const onClose = (): void => {
          cleanup();
          reject(new Error("The gRPC HTTP/2 stream closed before the write completed."));
        };
        const cleanup = (): void => {
          stream.removeListener("drain", onDrain);
          stream.removeListener("error", onError);
          stream.removeListener("close", onClose);
          signal?.removeEventListener("abort", onAbort);
        };
        const onAbort = (): void => {
          cleanup();
          reject(abortError(signal!));
        };
        stream.once("drain", onDrain);
        stream.once("error", onError);
        stream.once("close", onClose);
        signal?.addEventListener("abort", onAbort, { once: true });
        if (signal?.aborted) onAbort();
        else if (stream.destroyed) onClose();
      });
    }
  };

  try {
    if (body === undefined || body === null) {
      if (endStream) stream.end();
      return;
    }

    if (body instanceof Uint8Array || body instanceof ArrayBuffer || ArrayBuffer.isView(body)) {
      await writeChunk(toBytes(body));
    } else if (typeof body === "string") {
      await writeChunk(Buffer.from(body));
    } else if (isReadableStream(body)) {
      const reader = body.getReader();
      try {
        while (true) {
          const result = await reader.read();
          if (result.done) break;
          await writeChunk(toBytes(result.value));
        }
      } finally {
        reader.releaseLock();
      }
    } else if (isAsyncIterable(body)) {
      for await (const chunk of body) await writeChunk(toBytes(chunk));
    } else {
      throw new TypeError(
        "Unsupported gRPC request/response body. Use Uint8Array, Buffer, string, ReadableStream, or AsyncIterable.",
      );
    }

    if (endStream) stream.end();
  } catch (error) {
    if (endStream && !stream.destroyed) stream.close(constants.NGHTTP2_CANCEL);
    throw error;
  }
}

async function readStreamBody(
  stream: AsyncIterable<Uint8Array | string>,
  maxBodyBytes: number,
  signal?: AbortSignal,
): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let total = 0;

  for await (const chunk of stream) {
    if (signal?.aborted) throw abortError(signal);
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += bytes.byteLength;
    if (total > maxBodyBytes) {
      throw new RangeError(`Request body exceeded maxBodyBytes (${maxBodyBytes} bytes).`);
    }
    chunks.push(bytes);
  }

  return Buffer.concat(chunks, total);
}

function resolveRequestUrl(requestUrl: string, baseUrl?: string): URL {
  let url: URL;
  try {
    url = baseUrl ? new URL(requestUrl, baseUrl) : new URL(requestUrl);
  } catch {
    throw new TypeError(
      `Invalid HTTP/2 URL "${requestUrl}". Supply an absolute URL or configure Http2Transport.baseUrl.`,
    );
  }

  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new TypeError(`Unsupported gRPC URL protocol "${url.protocol}". Use http: or https:.`);
  }
  if (url.username || url.password) {
    throw new TypeError("Credentials must be sent as gRPC metadata, not embedded in the URL.");
  }
  return url;
}

function waitForSessionClose(session: Http2Session, force: boolean): Promise<void> {
  if (session.closed || session.destroyed) return Promise.resolve();

  return new Promise<void>((resolve) => {
    session.once("close", resolve);
    if (force) session.destroy();
    else session.close();
  });
}

function waitForServerSessionClose(session: Http2ServerSession, force: boolean): void {
  if (session.closed || session.destroyed) return;
  if (force) session.destroy();
  else session.close();
}

/**
 * Native HTTP/2 transport for REST and gRPC.
 *
 * It owns HTTP/2 sessions and streams, forwards raw bytes, and preserves HTTP/2
 * response trailers separately from initial headers. Protocol packages retain
 * responsibility for REST semantics and gRPC framing/Protobuf handling.
 */
export class Http2Transport implements HyperTransport {
  public readonly protocols = ["rest", "grpc"] as const;

  private readonly config: Http2TransportConfig;
  private readonly sessions = new Map<string, Http2Session>();
  private readonly servers = new Set<RunningServer>();
  private readonly lifecycle = new AbortController();
  private closed = false;
  private closePromise?: Promise<void>;

  public constructor(config: Http2TransportConfig = {}) {
    this.config = config;
  }

  public supports(protocol: string): boolean {
    return protocol === "rest" || protocol === "grpc";
  }

  /** Executes one gRPC HTTP/2 request and returns its complete raw framed body. */
  public async execute(request: TransportRequest): Promise<TransportResponse> {
    if (this.closed) throw new Error("Http2Transport is closed and cannot execute new requests.");
    if (!this.supports(request.protocol)) {
      throw new TypeError(`Http2Transport does not support protocol "${request.protocol}".`);
    }

    const url = resolveRequestUrl(request.url, this.config.baseUrl);
    const configuredTimeout = this.config.timeout ?? DEFAULT_TIMEOUT;
    const timeout = Number.isFinite(configuredTimeout)
      ? Math.max(0, configuredTimeout)
      : DEFAULT_TIMEOUT;
    const configuredMaxResponseBytes = this.config.maxResponseBytes ?? DEFAULT_MAX_RESPONSE_BYTES;
    const maxResponseBytes = Number.isFinite(configuredMaxResponseBytes)
      ? Math.max(1, Math.floor(configuredMaxResponseBytes))
      : DEFAULT_MAX_RESPONSE_BYTES;
    const combined = combineSignals([request.signal, this.lifecycle.signal], timeout);

    try {
      if (combined.signal.aborted) throw abortError(combined.signal);
      const session = this.getSession(url.origin);
      return await this.executeOnSession(session, url, request, combined.signal, maxResponseBytes);
    } finally {
      combined.dispose();
    }
  }

  /** Starts an HTTP/2 gRPC server and forwards each raw stream to onRequest. */
  public async listen(options: TransportListenOptions): Promise<TransportServer> {
    if (this.closed) throw new Error("Http2Transport is closed and cannot start a server.");
    if (options.signal?.aborted) throw abortError(options.signal);

    const serverOptions =
      this.config.maxConcurrentStreams === undefined
        ? undefined
        : { settings: { maxConcurrentStreams: this.config.maxConcurrentStreams } };
    const server: GrpcHttp2Server = this.config.serverTls
      ? createSecureServer({ ...this.config.serverTls, ...serverOptions })
      : serverOptions
        ? createServer(serverOptions)
        : createServer();

    const sessions = new Set<Http2ServerSession>();
    const running = {} as RunningServer;
    let stopPromise: Promise<void> | undefined;
    const stop = (force = false): Promise<void> => {
      if (stopPromise) return stopPromise;

      stopPromise = new Promise<void>((resolve, reject) => {
        const finish = (error?: Error): void => {
          options.signal?.removeEventListener("abort", onAbort);
          this.servers.delete(running);
          if (error) reject(error);
          else resolve();
        };

        if (!server.listening) {
          finish();
          return;
        }

        server.close((error?: Error) => finish(error));
        for (const session of sessions) waitForServerSessionClose(session, force);
      });
      return stopPromise;
    };

    const onAbort = (): void => {
      void stop(true).catch(() => {});
    };

    Object.assign(running, { server, sessions, stop });

    server.on("session", (session) => {
      sessions.add(session);
      session.once("close", () => sessions.delete(session));
    });
    server.on("stream", (stream, headers) => {
      // HTTP/2 emits stream-level errors independently from the server error event.
      // The handler below reports failures through gRPC trailers when possible.
      stream.on("error", () => {});
      void this.handleServerStream(
        stream as Http2ServerStream,
        headers as RawHeaders,
        options,
      ).catch(() => {
        if (!stream.destroyed) stream.close(constants.NGHTTP2_INTERNAL_ERROR);
      });
    });

    try {
      await new Promise<void>((resolve, reject) => {
        const onError = (error: Error): void => {
          server.removeListener("listening", onListening);
          reject(error);
        };
        const onListening = (): void => {
          server.removeListener("error", onError);
          resolve();
        };
        server.once("error", onError);
        server.once("listening", onListening);
        server.listen(options.port ?? 0, options.host ?? "0.0.0.0");
      });
    } catch (error) {
      try {
        server.close();
      } catch {
        // Preserve the original startup error.
      }
      throw error;
    }

    this.servers.add(running);
    options.signal?.addEventListener("abort", onAbort, { once: true });
    if (options.signal?.aborted) {
      await stop(true);
      throw abortError(options.signal);
    }

    return { close: () => stop(false) };
  }

  /** Gracefully closes listeners and HTTP/2 client sessions. */
  public close(): Promise<void> {
    return this.shutdown(false);
  }

  /** Aborts active client requests and forcefully closes all HTTP/2 resources. */
  public destroy(): Promise<void> {
    return this.shutdown(true);
  }

  private getSession(origin: string): Http2Session {
    const existing = this.sessions.get(origin);
    if (existing && !existing.closed && !existing.destroyed) return existing;

    const session = connect(origin, this.config.clientTls);
    // The individual request streams own request errors; this listener prevents
    // an idle connection error from becoming an unhandled EventEmitter error.
    session.on("error", () => {});
    session.on("goaway", () => {
      if (this.sessions.get(origin) === session) this.sessions.delete(origin);
    });
    session.once("close", () => {
      if (this.sessions.get(origin) === session) this.sessions.delete(origin);
    });
    this.sessions.set(origin, session);
    return session;
  }

  private executeOnSession(
    session: Http2Session,
    url: URL,
    request: TransportRequest,
    signal: AbortSignal,
    maxResponseBytes: number,
  ): Promise<TransportResponse> {
    const headers: OutgoingHttpHeaders = {
      ":method": request.method.toUpperCase(),
      ":path": `${url.pathname}${url.search}`,
      ":scheme": url.protocol.slice(0, -1),
      ":authority": url.host,
    };
    assignOutgoingHeaders(headers, request.headers);

    const stream = session.request(headers);

    return new Promise<TransportResponse>((resolve, reject) => {
      let settled = false;
      let status = 0;
      const responseHeaders: HeaderRecord = Object.create(null) as HeaderRecord;
      const responseTrailers: HeaderRecord = Object.create(null) as HeaderRecord;
      const chunks: Buffer[] = [];
      let bodyBytes = 0;

      const cleanup = (): void => {
        signal.removeEventListener("abort", onAbort);
        stream.removeListener("response", onResponse);
        stream.removeListener("trailers", onTrailers);
        stream.removeListener("data", onData);
        stream.removeListener("end", onEnd);
        stream.removeListener("error", onError);
        stream.removeListener("aborted", onAborted);
      };
      const fail = (error: unknown): void => {
        if (settled) return;
        settled = true;
        // Closing an HTTP/2 stream can emit ERR_HTTP2_STREAM_CANCEL on a later
        // tick. Keep a one-shot error sink attached after removing request hooks.
        const absorbCloseError = (): void => {};
        const removeErrorSink = (): void => {
          stream.removeListener("error", absorbCloseError);
        };
        stream.once("error", absorbCloseError);
        stream.once("close", removeErrorSink);
        cleanup();
        if (!stream.destroyed) stream.close(constants.NGHTTP2_CANCEL);
        reject(error);
      };
      const onResponse = (incoming: RawHeaders): void => {
        status = Number(incoming[":status"] ?? 0);
        Object.assign(responseHeaders, toHeaderRecord(incoming));
      };
      const onTrailers = (trailers: RawHeaders): void => {
        Object.assign(responseTrailers, toHeaderRecord(trailers));
      };
      const onData = (chunk: Buffer | Uint8Array): void => {
        const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        bodyBytes += bytes.byteLength;
        if (bodyBytes > maxResponseBytes) {
          fail(
            new RangeError(`gRPC response exceeded maxResponseBytes (${maxResponseBytes} bytes).`),
          );
          return;
        }
        chunks.push(bytes);
      };
      const onEnd = (): void => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve({
          status,
          headers: responseHeaders,
          trailers: Object.keys(responseTrailers).length > 0 ? responseTrailers : undefined,
          url: url.href,
          body: Buffer.concat(chunks, bodyBytes),
        });
      };
      const onError = (error: Error): void => fail(error);
      const onAborted = (): void => fail(new Error("The gRPC HTTP/2 response stream was aborted."));
      const onAbort = (): void => fail(abortError(signal));

      stream.on("response", onResponse);
      stream.on("trailers", onTrailers);
      stream.on("data", onData);
      stream.once("end", onEnd);
      stream.once("error", onError);
      stream.once("aborted", onAborted);
      signal.addEventListener("abort", onAbort, { once: true });

      if (signal.aborted) {
        onAbort();
        return;
      }

      void writeBody(stream, request.body, signal).catch(fail);
    });
  }

  private async handleServerStream(
    stream: Http2ServerStream,
    rawHeaders: RawHeaders,
    options: TransportListenOptions,
  ): Promise<void> {
    if (stream.destroyed) return;

    const method = asHeaderString(rawHeaders[":method"]) ?? "POST";
    const path = asHeaderString(rawHeaders[":path"]) ?? "/";
    const scheme =
      asHeaderString(rawHeaders[":scheme"]) ?? (this.config.serverTls ? "https" : "http");
    const authority = asHeaderString(rawHeaders[":authority"]) ?? "localhost";
    const controller = new AbortController();
    const onAborted = (): void => controller.abort(new Error("The gRPC request was cancelled."));
    stream.once("aborted", onAborted);

    const contentType = asHeaderString(rawHeaders["content-type"]) ?? "";
    const protocol = contentType.startsWith("application/grpc") ? "grpc" : "rest";
    const maxBodyBytes = this.config.maxBodyBytes ?? DEFAULT_MAX_BODY_BYTES;
    const body =
      protocol === "grpc" ? stream : await readStreamBody(stream, maxBodyBytes, controller.signal);
    const request: TransportRequest = {
      method,
      url: `${scheme}://${authority}${path}`,
      headers: toHeaderRecord(rawHeaders),
      body,
      signal: controller.signal,
      protocol,
    };

    if (!options.onRequest) {
      await this.sendServerResponse(
        stream,
        protocol === "grpc"
          ? {
              status: 200,
              headers: { "content-type": "application/grpc" },
              trailers: { "grpc-status": "12", "grpc-message": "Unimplemented" },
              body: null,
            }
          : {
              status: 501,
              headers: { "content-type": "text/plain; charset=utf-8" },
              body: "Not Implemented",
            },
      );
      stream.removeListener("aborted", onAborted);
      return;
    }

    try {
      const response = await options.onRequest(request);
      if (!stream.destroyed) await this.sendServerResponse(stream, response);
    } catch {
      if (!stream.destroyed) {
        await this.sendServerResponse(
          stream,
          protocol === "grpc"
            ? {
                status: 200,
                headers: { "content-type": "application/grpc" },
                trailers: {
                  "grpc-status": "13",
                  "grpc-message": "Internal%20server%20error",
                },
                body: null,
              }
            : {
                status: 500,
                headers: { "content-type": "text/plain; charset=utf-8" },
                body: "Internal Server Error",
              },
        );
      }
    } finally {
      stream.removeListener("aborted", onAborted);
    }
  }

  private async sendServerResponse(
    stream: Http2ServerStream,
    response: TransportResponse,
  ): Promise<void> {
    const initialHeaders: OutgoingHttpHeaders = { ":status": response.status || 200 };
    const trailers: OutgoingHttpHeaders = Object.create(null) as OutgoingHttpHeaders;
    let isGrpc = false;

    for (const [rawName, rawValue] of Object.entries(response.headers)) {
      const name = rawName.toLowerCase();
      const value = Array.isArray(rawValue) ? [...rawValue] : rawValue;
      if (name === "content-type" && String(value).startsWith("application/grpc")) {
        isGrpc = true;
      }
      if (GRPC_TRAILER_NAMES.has(name)) {
        trailers[name] = value;
      } else if (!name.startsWith(":") && !FORBIDDEN_HTTP2_HEADERS.has(name)) {
        initialHeaders[name] = value;
      }
    }

    for (const [rawName, rawValue] of Object.entries(response.trailers ?? {})) {
      const name = rawName.toLowerCase();
      if (!name.startsWith(":") && !FORBIDDEN_HTTP2_HEADERS.has(name)) {
        trailers[name] = Array.isArray(rawValue) ? [...rawValue] : rawValue;
      }
    }

    if (isGrpc) {
      if (initialHeaders["content-type"] === undefined) {
        initialHeaders["content-type"] = "application/grpc";
      }
      if (trailers["grpc-status"] === undefined) {
        trailers["grpc-status"] = String(response.status >= 200 && response.status < 300 ? 0 : 13);
      }
    }

    if (Object.keys(trailers).length > 0) {
      stream.on("wantTrailers", () => {
        if (!stream.destroyed) stream.sendTrailers(trailers);
      });
      stream.respond(initialHeaders, { waitForTrailers: true });
    } else {
      stream.respond(initialHeaders);
    }
    await writeBody(stream, response.body, undefined, false);
    stream.end();
  }

  private shutdown(force: boolean): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closed = true;
    if (force && !this.lifecycle.signal.aborted) this.lifecycle.abort();

    this.closePromise = (async () => {
      const serverStops = Array.from(this.servers, (running) => {
        return running.stop(force);
      });

      const clientCloses = Array.from(this.sessions.values(), (session) =>
        waitForSessionClose(session, force),
      );
      this.sessions.clear();

      await Promise.all([...serverStops, ...clientCloses]);
    })();

    return this.closePromise;
  }
}
