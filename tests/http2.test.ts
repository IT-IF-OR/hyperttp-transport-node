import { connect as connectHttp2, constants } from "node:http2";
import { createServer as createTcpServer } from "node:net";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import type { TransportRequest } from "@hyperttp/types";
import { Http2Transport } from "../src/index.js";

type Transport = Http2Transport;

const transports: Transport[] = [];
const listeners: Array<{ close(): Promise<void> | void }> = [];

function createTransport(config: ConstructorParameters<typeof Http2Transport>[0] = {}): Transport {
  const transport = new Http2Transport(config);
  transports.push(transport);
  return transport;
}

async function getAvailablePort(): Promise<number> {
  const server = createTcpServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address() as AddressInfo;
  await new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
  return address.port;
}

async function readBody(body: unknown): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of body as AsyncIterable<Uint8Array>) {
    chunks.push(Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

function grpcRequest(url: string, body: Uint8Array): TransportRequest {
  return {
    method: "POST",
    url,
    protocol: "grpc",
    headers: {
      "content-type": "application/grpc",
      te: "trailers",
    },
    body,
  };
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve(value: T): void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

afterEach(async () => {
  for (const listener of listeners.splice(0)) await listener.close();
  for (const transport of transports.splice(0)) await transport.destroy();
});

describe("Http2Transport", () => {
  it("supports REST and gRPC", () => {
    const transport = createTransport();
    expect(transport.protocols).toEqual(["rest", "grpc"]);
    expect(transport.supports("grpc")).toBe(true);
    expect(transport.supports("rest")).toBe(true);
  });

  it("sends REST over HTTP/2 without gRPC trailers", async () => {
    const port = await getAvailablePort();
    const serverTransport = createTransport();
    const server = await serverTransport.listen({
      host: "127.0.0.1",
      port,
      onRequest: async (request) => ({
        status: 201,
        headers: { "content-type": "application/json", "x-method": request.method },
        body: JSON.stringify({ path: new URL(request.url).pathname }),
      }),
    });
    listeners.push(server);

    const client = createTransport({ baseUrl: `http://127.0.0.1:${port}` });
    const response = await client.execute({
      method: "POST",
      url: "/items",
      protocol: "rest",
      headers: { "content-type": "application/json" },
      body: "{}",
    });

    expect(response.status).toBe(201);
    expect(response.headers["x-method"]).toBe("POST");
    expect(response.trailers).toBeUndefined();
    expect(response.body).toEqual(Buffer.from('{"path":"/items"}'));
  });

  it("sends raw gRPC frames over HTTP/2 and exposes response trailers", async () => {
    const port = await getAvailablePort();
    const serverTransport = createTransport();
    const server = await serverTransport.listen({
      host: "127.0.0.1",
      port,
      onRequest: async (request) => {
        expect(request.method).toBe("POST");
        expect(request.headers.authorization).toBe("Bearer test");
        const body = await readBody(request.body);
        return {
          status: 200,
          headers: {
            "content-type": "application/grpc",
            "grpc-status": "0",
            "x-server-meta": "echo",
          },
          body,
        };
      },
    });
    listeners.push(server);

    const client = createTransport({ baseUrl: `http://127.0.0.1:${port}` });
    const frame = new Uint8Array([0, 0, 0, 0, 3, 10, 20, 30]);
    const response = await client.execute({
      ...grpcRequest("/example.Echo/Unary", frame),
      headers: {
        "content-type": "application/grpc",
        te: "trailers",
        authorization: "Bearer test",
      },
    });

    expect(response.status).toBe(200);
    expect(response.trailers?.["grpc-status"]).toBe("0");
    expect(response.headers["x-server-meta"]).toBe("echo");
    expect(response.body).toEqual(Buffer.from(frame));
    expect(response.url).toBe(`http://127.0.0.1:${port}/example.Echo/Unary`);
  });

  it("streams an async iterable request body without concatenating it first", async () => {
    const port = await getAvailablePort();
    const serverTransport = createTransport();
    const server = await serverTransport.listen({
      host: "127.0.0.1",
      port,
      onRequest: async (request) => ({
        status: 200,
        headers: { "grpc-status": "0" },
        body: await readBody(request.body),
      }),
    });
    listeners.push(server);

    const client = createTransport({ baseUrl: `http://127.0.0.1:${port}` });
    async function* body(): AsyncIterable<Uint8Array> {
      yield new Uint8Array([0, 0, 0]);
      yield new Uint8Array([0, 0, 2, 1, 2]);
    }

    const response = await client.execute({
      ...grpcRequest("/example.Stream/Unary", new Uint8Array()),
      body: body(),
    });
    expect(response.body).toEqual(Buffer.from([0, 0, 0, 0, 0, 2, 1, 2]));
    expect(response.trailers?.["grpc-status"]).toBe("0");
  });

  it("returns UNIMPLEMENTED trailers when no server handler is registered", async () => {
    const port = await getAvailablePort();
    const serverTransport = createTransport();
    const server = await serverTransport.listen({ host: "127.0.0.1", port });
    listeners.push(server);

    const client = createTransport({ baseUrl: `http://127.0.0.1:${port}` });
    const response = await client.execute(
      grpcRequest("/example.Missing/Call", new Uint8Array([0, 0, 0, 0, 0])),
    );

    expect(response.status).toBe(200);
    expect(response.trailers?.["grpc-status"]).toBe("12");
    expect(response.trailers?.["grpc-message"]).toBe("Unimplemented");
  });

  it("rejects oversized responses instead of buffering without a limit", async () => {
    const port = await getAvailablePort();
    const serverTransport = createTransport();
    const server = await serverTransport.listen({
      host: "127.0.0.1",
      port,
      onRequest: async () => ({
        status: 200,
        headers: { "grpc-status": "0" },
        body: new Uint8Array(128),
      }),
    });
    listeners.push(server);

    const client = createTransport({
      baseUrl: `http://127.0.0.1:${port}`,
      maxResponseBytes: 64,
    });
    await expect(
      client.execute(grpcRequest("/example.Large/Call", new Uint8Array([0, 0, 0, 0, 0]))),
    ).rejects.toThrow(/maxResponseBytes/);
  });

  it("aborts requests when the configured timeout expires", async () => {
    const port = await getAvailablePort();
    const serverTransport = createTransport();
    const server = await serverTransport.listen({
      host: "127.0.0.1",
      port,
      onRequest: async () => {
        await new Promise((resolve) => setTimeout(resolve, 100));
        return { status: 200, headers: { "grpc-status": "0" }, body: new Uint8Array() };
      },
    });
    listeners.push(server);

    const client = createTransport({ baseUrl: `http://127.0.0.1:${port}`, timeout: 10 });
    await expect(
      client.execute(grpcRequest("/example.Timeout/Call", new Uint8Array())),
    ).rejects.toThrow(/timed out/i);
  });

  it("cancels an in-flight request through AbortSignal", async () => {
    const port = await getAvailablePort();
    const serverTransport = createTransport();
    const server = await serverTransport.listen({
      host: "127.0.0.1",
      port,
      onRequest: async () => {
        await new Promise((resolve) => setTimeout(resolve, 50));
        return { status: 200, headers: { "grpc-status": "0" }, body: new Uint8Array() };
      },
    });
    listeners.push(server);

    const client = createTransport({ baseUrl: `http://127.0.0.1:${port}` });
    const controller = new AbortController();
    const pending = client.execute({
      ...grpcRequest("/example.Cancel/Call", new Uint8Array()),
      signal: controller.signal,
    });
    setTimeout(() => controller.abort(new Error("request cancelled")), 5);
    await expect(pending).rejects.toThrow("request cancelled");
  });

  it("keeps parallel RPCs independent when one is cancelled", async () => {
    const port = await getAvailablePort();
    const cancelled = deferred<void>();
    const serverTransport = createTransport();
    const server = await serverTransport.listen({
      host: "127.0.0.1",
      port,
      onRequest: async (request) => {
        if (new URL(request.url).pathname === "/cancel") {
          await new Promise<void>((resolve) => {
            request.signal?.addEventListener(
              "abort",
              () => {
                cancelled.resolve();
                resolve();
              },
              { once: true },
            );
          });
        }
        return { status: 200, headers: { "content-type": "application/grpc" }, body: request.body };
      },
    });
    listeners.push(server);

    const client = createTransport({ baseUrl: `http://127.0.0.1:${port}` });
    const controller = new AbortController();
    const cancelledCall = client.execute({
      ...grpcRequest("/cancel", new Uint8Array([0, 0, 0, 0, 0])),
      signal: controller.signal,
    });
    const completedCall = client.execute(grpcRequest("/complete", new Uint8Array([0, 0, 0, 0, 0])));

    controller.abort(new Error("cancelled only this RPC"));
    await expect(cancelledCall).rejects.toThrow("cancelled only this RPC");
    await expect(cancelled.promise).resolves.toBeUndefined();
    await expect(completedCall).resolves.toMatchObject({ trailers: { "grpc-status": "0" } });
  });

  it("sends GOAWAY and drains an active stream during graceful shutdown", async () => {
    const port = await getAvailablePort();
    const started = deferred<void>();
    const finish = deferred<void>();
    const serverTransport = createTransport();
    const server = await serverTransport.listen({
      host: "127.0.0.1",
      port,
      onRequest: async () => {
        started.resolve();
        await finish.promise;
        return {
          status: 200,
          headers: { "content-type": "application/grpc" },
          body: new Uint8Array(),
        };
      },
    });
    listeners.push(server);

    const session = connectHttp2(`http://127.0.0.1:${port}`);
    const request = session.request({
      ":method": "POST",
      ":path": "/drain",
      "content-type": "application/grpc",
      te: "trailers",
    });
    request.end(new Uint8Array([0, 0, 0, 0, 0]));
    request.resume();
    await started.promise;

    const goaway = new Promise<number>((resolve) =>
      session.once("goaway", (code) => resolve(code)),
    );
    const stopped = server.close();
    await expect(goaway).resolves.toBe(constants.NGHTTP2_NO_ERROR);
    await expect(
      Promise.race([stopped.then(() => true), wait(10).then(() => false)]),
    ).resolves.toBe(false);

    finish.resolve();
    await expect(stopped).resolves.toBeUndefined();
    session.destroy();
  });

  it("recovers with a new session after the peer disconnects", async () => {
    const port = await getAvailablePort();
    const firstServer = createTransport();
    const firstListener = await firstServer.listen({
      host: "127.0.0.1",
      port,
      onRequest: async () => ({ status: 200, headers: {}, body: "first" }),
    });
    listeners.push(firstListener);
    const client = createTransport({ baseUrl: `http://127.0.0.1:${port}` });

    await expect(
      client.execute({ method: "GET", url: "/", protocol: "rest", headers: {} }),
    ).resolves.toMatchObject({
      body: Buffer.from("first"),
    });
    await firstServer.destroy();

    const replacementServer = createTransport();
    const replacementListener = await replacementServer.listen({
      host: "127.0.0.1",
      port,
      onRequest: async () => ({ status: 200, headers: {}, body: "recovered" }),
    });
    listeners.push(replacementListener);

    await expect(
      client.execute({ method: "GET", url: "/", protocol: "rest", headers: {} }),
    ).resolves.toMatchObject({
      body: Buffer.from("recovered"),
    });
  });

  it("advertises and enforces maxConcurrentStreams", async () => {
    const port = await getAvailablePort();
    const firstStarted = deferred<void>();
    const releaseFirst = deferred<void>();
    let active = 0;
    let maxActive = 0;
    const serverTransport = createTransport({ maxConcurrentStreams: 1 });
    const server = await serverTransport.listen({
      host: "127.0.0.1",
      port,
      onRequest: async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        firstStarted.resolve();
        await releaseFirst.promise;
        active -= 1;
        return { status: 200, headers: {}, body: new Uint8Array() };
      },
    });
    listeners.push(server);

    const client = createTransport({ baseUrl: `http://127.0.0.1:${port}` });
    const first = client.execute({ method: "GET", url: "/first", protocol: "rest", headers: {} });
    await firstStarted.promise;
    const second = client.execute({ method: "GET", url: "/second", protocol: "rest", headers: {} });
    await wait(10);
    expect(maxActive).toBe(1);

    releaseFirst.resolve();
    await expect(Promise.all([first, second])).resolves.toHaveLength(2);
    expect(maxActive).toBe(1);
  });

  it("releases server work after a client disconnect", async () => {
    const port = await getAvailablePort();
    const aborted = deferred<void>();
    const serverTransport = createTransport();
    const server = await serverTransport.listen({
      host: "127.0.0.1",
      port,
      onRequest: async (request) => {
        await new Promise<void>((resolve) => {
          request.signal?.addEventListener(
            "abort",
            () => {
              aborted.resolve();
              resolve();
            },
            { once: true },
          );
        });
        return { status: 200, headers: { "content-type": "application/grpc" }, body: null };
      },
    });
    listeners.push(server);

    const client = createTransport({ baseUrl: `http://127.0.0.1:${port}` });
    const controller = new AbortController();
    const pending = client.execute({
      ...grpcRequest("/disconnect", new Uint8Array([0, 0, 0, 0, 0])),
      signal: controller.signal,
    });
    controller.abort(new Error("client disconnected"));

    await expect(pending).rejects.toThrow("client disconnected");
    await expect(aborted.promise).resolves.toBeUndefined();
    await expect(server.close()).resolves.toBeUndefined();
  });

  it("rejects relative URLs when no baseUrl is configured", async () => {
    const transport = createTransport();
    await expect(
      transport.execute(grpcRequest("/example.Echo/Unary", new Uint8Array())),
    ).rejects.toThrow(/absolute URL or configure/);
  });

  it("rejects requests after close", async () => {
    const transport = createTransport();
    await transport.close();
    await expect(
      transport.execute(grpcRequest("http://127.0.0.1:1/example.Echo/Unary", new Uint8Array())),
    ).rejects.toThrow(/closed/);
  });
});
