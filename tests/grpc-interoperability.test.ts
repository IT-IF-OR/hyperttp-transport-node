import { execFile as execFileCallback } from "node:child_process";
import { readFile } from "node:fs/promises";
import { createServer as createTcpServer } from "node:net";
import type { AddressInfo } from "node:net";
import { connect as connectTls } from "node:tls";
import { promisify } from "node:util";
import * as grpc from "@grpc/grpc-js";
import { afterEach, describe, expect, it } from "vitest";
import type { TransportRequest, TransportResponse } from "@hyperttp/types";
import { Http2Transport } from "../src/index.js";

const execFile = promisify(execFileCallback);
const certificate = await readFile(new URL("./fixtures/localhost-cert.pem", import.meta.url));
const privateKey = await readFile(new URL("./fixtures/localhost-key.pem", import.meta.url));
const fixturesPath = new URL("./fixtures/", import.meta.url).pathname;
const certificatePath = new URL("./fixtures/localhost-cert.pem", import.meta.url).pathname;
const transports: Http2Transport[] = [];
const listeners: Array<{ close(): Promise<void> | void }> = [];

function createTransport(
  config: ConstructorParameters<typeof Http2Transport>[0] = {},
): Http2Transport {
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
  for await (const chunk of body as AsyncIterable<Uint8Array>) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}

async function startServer(
  onRequest: (request: TransportRequest) => TransportResponse | Promise<TransportResponse>,
  tls = false,
): Promise<number> {
  const port = await getAvailablePort();
  const transport = createTransport(
    tls ? { serverTls: { cert: certificate, key: privateKey } } : {},
  );
  const listener = await transport.listen({ host: "127.0.0.1", port, onRequest });
  listeners.push(listener);
  return port;
}

async function startEchoServer(tls = false): Promise<number> {
  return startServer(
    async (request) => ({
      status: 200,
      headers: { "content-type": "application/grpc" },
      body: await readBody(request.body),
    }),
    tls,
  );
}

function grpcErrorResponse(code: number, message: string): TransportResponse {
  return {
    status: 200,
    headers: { "content-type": "application/grpc", "x-request-id": "request-42" },
    trailers: {
      "grpc-status": String(code),
      "grpc-message": encodeGrpcMessage(message),
      "x-error-context": "validation",
    },
    body: null,
  };
}

function encodeGrpcMessage(message: string): string {
  return encodeURIComponent(message).replace(/%2F/gi, "/");
}

function unaryCall(
  target: string,
  credentials: grpc.ChannelCredentials,
  value: Buffer,
  options?: grpc.ClientOptions,
): Promise<Buffer> {
  const client = new grpc.Client(target, credentials, options);
  return new Promise<Buffer>((resolve, reject) => {
    client.makeUnaryRequest(
      "/hyperttp.interop.Echo/Unary",
      (request: Buffer) => request,
      (response: Buffer) => response,
      value,
      (error, response) => {
        client.close();
        if (error) reject(error);
        else resolve(response);
      },
    );
  });
}

function unaryCallWithMetadata(
  target: string,
  credentials: grpc.ChannelCredentials,
  value: Buffer,
  options?: grpc.ClientOptions,
): Promise<{
  error: grpc.ServiceError | null;
  metadata: grpc.Metadata;
  status: grpc.StatusObject;
}> {
  const client = new grpc.Client(target, credentials, options);
  return new Promise((resolve) => {
    let error: grpc.ServiceError | null = null;
    let metadata = new grpc.Metadata();
    const call = client.makeUnaryRequest(
      "/hyperttp.interop.Echo/Unary",
      (request: Buffer) => request,
      (response: Buffer) => response,
      value,
      (failure) => {
        error = failure;
      },
    );
    call.once("metadata", (received) => {
      metadata = received;
    });
    call.once("status", (status) => {
      client.close();
      resolve({ error, metadata, status });
    });
  });
}

function negotiatedAlpn(port: number): Promise<string | false> {
  return new Promise((resolve, reject) => {
    const socket = connectTls({
      host: "127.0.0.1",
      port,
      ca: certificate,
      ALPNProtocols: ["h2"],
    });
    socket.once("error", reject);
    socket.once("secureConnect", () => {
      const alpn = socket.alpnProtocol;
      socket.end();
      resolve(alpn);
    });
  });
}

async function grpcurlIsAvailable(): Promise<boolean> {
  try {
    await execFile("grpcurl", ["-version"]);
    return true;
  } catch {
    return false;
  }
}

const hasGrpcurl = await grpcurlIsAvailable();

afterEach(async () => {
  for (const listener of listeners.splice(0)) await listener.close();
  for (const transport of transports.splice(0)) await transport.destroy();
});

describe("gRPC interoperability", () => {
  it("serves a unary h2c call from grpc-js", async () => {
    const port = await startEchoServer();
    const request = Buffer.from([10, 5, 104, 101, 108, 108, 111]);

    await expect(
      unaryCall(`127.0.0.1:${port}`, grpc.credentials.createInsecure(), request),
    ).resolves.toEqual(request);
  });

  it("serves a unary TLS call from grpc-js over ALPN h2", async () => {
    const port = await startEchoServer(true);
    const request = Buffer.from([10, 5, 104, 101, 108, 108, 111]);

    await expect(negotiatedAlpn(port)).resolves.toBe("h2");
    await expect(
      unaryCall(`127.0.0.1:${port}`, grpc.credentials.createSsl(certificate), request, {
        "grpc.ssl_target_name_override": "localhost",
      }),
    ).resolves.toEqual(request);
  });

  it.each([
    [3, "INVALID_ARGUMENT"],
    [5, "NOT_FOUND"],
    [7, "PERMISSION_DENIED"],
    [13, "INTERNAL"],
  ])("returns grpc-status %i (%s) to grpc-js", async (code) => {
    const port = await startServer(() => grpcErrorResponse(code, "bad / input"));
    const result = await unaryCallWithMetadata(
      `127.0.0.1:${port}`,
      grpc.credentials.createInsecure(),
      Buffer.from([0, 0, 0, 0, 0]),
    );

    expect(result.error?.code).toBe(code);
    expect(result.error?.details).toBe("bad / input");
    expect(result.error?.metadata.get("x-error-context")).toEqual(["validation"]);
    expect(result.metadata.get("x-request-id")).toEqual(["request-42"]);
    expect(result.status.metadata.get("x-error-context")).toEqual(["validation"]);
  });

  it.skipIf(!hasGrpcurl).each([
    [3, "InvalidArgument"],
    [5, "NotFound"],
    [7, "PermissionDenied"],
    [13, "Internal"],
  ])("returns grpc-status %i (%s) to grpcurl", async (code, statusName) => {
    const port = await startServer(() => grpcErrorResponse(code, "bad / input"), true);
    const result = await execFile("grpcurl", [
      "-cacert",
      certificatePath,
      "-import-path",
      fixturesPath,
      "-proto",
      "echo.proto",
      "-authority",
      "localhost",
      "-d",
      '{"value":"hello"}',
      `127.0.0.1:${port}`,
      "hyperttp.interop.Echo/Unary",
    ]).catch((error: { stdout?: string; stderr?: string }) => error);
    const output = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;

    expect(output).toContain(`Code: ${statusName}`);
    expect(output).toContain("Message: bad / input");
  });

  it.skipIf(!hasGrpcurl)("returns initial and trailing metadata to grpcurl", async () => {
    const port = await startServer(() => grpcErrorResponse(3, "bad / input"), true);
    const result = await execFile("grpcurl", [
      "-v",
      "-cacert",
      certificatePath,
      "-import-path",
      fixturesPath,
      "-proto",
      "echo.proto",
      "-authority",
      "localhost",
      "-d",
      '{"value":"hello"}',
      `127.0.0.1:${port}`,
      "hyperttp.interop.Echo/Unary",
    ]).catch((error: { stdout?: string; stderr?: string }) => error);
    const output = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;

    expect(output).toContain("Code: InvalidArgument");
    expect(output).toContain("Message: bad / input");
    expect(output).toContain("x-error-context: validation");
    expect(output).toContain("x-request-id: request-42");
  });

  it.skipIf(!hasGrpcurl)("serves a unary TLS call from grpcurl", async () => {
    const port = await startEchoServer(true);
    const { stdout } = await execFile("grpcurl", [
      "-cacert",
      certificatePath,
      "-import-path",
      fixturesPath,
      "-proto",
      "echo.proto",
      "-authority",
      "localhost",
      "-d",
      '{"value":"hello"}',
      `127.0.0.1:${port}`,
      "hyperttp.interop.Echo/Unary",
    ]);

    expect(JSON.parse(stdout)).toEqual({ value: "hello" });
  });
});
