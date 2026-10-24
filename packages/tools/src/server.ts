import { createServer, type Server } from "node:http"
import type { AddressInfo } from "node:net"
import { createHttpAdapterHandler } from "@idemantis/adapter-http"
import type { ToolAdapter } from "./tools.js"

export interface ServeToolsOptions {
  token?: string
  host?: string
  port?: number
}

export interface ServedTools {
  url: string
  server: Server
  close(): Promise<void>
}

export async function serveTools(
  adapter: ToolAdapter,
  options: ServeToolsOptions = {},
): Promise<ServedTools> {
  const server = createServer(
    createHttpAdapterHandler({
      adapter: adapter.adapter,
      adapter_version: adapter.adapter_version,
      adapter_package_digest: adapter.adapter_package_digest,
      operations: adapter.operations,
      ...(options.token ? { token: options.token } : {}),
    }),
  )
  const host = options.host ?? "127.0.0.1"
  await new Promise<void>((resolvePromise, reject) => {
    server.once("error", reject)
    server.listen(options.port ?? 0, host, () => resolvePromise())
  })
  const { port } = server.address() as AddressInfo
  return {
    url: `http://${host.includes(":") ? `[${host}]` : host}:${port}/`,
    server,
    close: () =>
      new Promise<void>((resolvePromise) => {
        server.closeAllConnections()
        server.close(() => resolvePromise())
      }),
  }
}
