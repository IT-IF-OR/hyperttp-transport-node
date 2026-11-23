import { createServer } from "node:net";
import type { AddressInfo } from "node:net";
import { Http2Transport } from "../src/index.js";

async function getAvailablePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((resolve, reject) => {
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", resolve);
  });
  const port = (probe.address() as AddressInfo).port;
  await new Promise<void>((resolve, reject) =>
    probe.close((error) => (error ? reject(error) : resolve())),
  );
  return port;
}

const port = await getAvailablePort();
const serverTransport = new Http2Transport();
const server = await serverTransport.listen({
  host: "127.0.0.1",
  port,
  onRequest: () => ({
    status: 200,
    headers: { "content-type": "application/grpc" },
    trailers: { "grpc-status": "0" },
    body: new Uint8Array([0, 0, 0, 0, 0]),
  }),
});
const client = new Http2Transport({ baseUrl: `http://127.0.0.1:${port}` });

try {
  const response = await client.execute({
    method: "POST",
    url: "/example.Echo/Call",
    protocol: "grpc",
    headers: { "content-type": "application/grpc", te: "trailers" },
    body: new Uint8Array([0, 0, 0, 0, 0]),
  });
  if (response.trailers?.["grpc-status"] !== "0") {
    throw new Error("Expected grpc-status trailer from Bun node:http2 compatibility layer.");
  }
} finally {
  await client.destroy();
  await server.close();
  await serverTransport.destroy();
}
