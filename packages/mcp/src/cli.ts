import { resolve } from "node:path"
import { pathToFileURL } from "node:url"
import { IdemantisClient } from "@idemantis/client"
import { McpServer, serveStdio } from "./server.js"

export async function main(environment: NodeJS.ProcessEnv = process.env): Promise<void> {
  const { IDEMANTIS_URL: url, IDEMANTIS_API_KEY: apiKey } = environment
  if (!url || !apiKey) {
    throw new TypeError("Set IDEMANTIS_URL and IDEMANTIS_API_KEY to connect to a runner")
  }
  await serveStdio(new McpServer({ client: new IdemantisClient({ url, api_key: apiKey }) }))
}

const direct = process.argv[1]
  ? import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  : false

if (direct) {
  main().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : "MCP server failed"}\n`)
    process.exitCode = 1
  })
}
