/**
 * WebSocket endpoint for external clients (Codex CLI, etc.).
 *
 * Accepts WS connections at `ws://localhost:<port>/v1/responses`,
 * receives JSON request frames, and streams back JSON event frames.
 *
 * Protocol (client ↔ proxy ↔ upstream):
 *   Client: {"type":"response.create","model":"codex","instructions":"...","input":[...]}
 *   Proxy → Upstream: CodexResponsesRequest (via CodexApi.createResponse with useWebSocket=true)
 *   Upstream → Proxy: SSE events (response.created, response.output_text.delta, etc.)
 *   Proxy → Client: {"type":"response.created","data":{...}}
 */

import { WebSocket, WebSocketServer } from "ws";
import type { IncomingMessage } from "http";
import type { Server } from "http";
import type { AccountPool } from "../auth/account-pool.js";
import type { CookieJar } from "../proxy/cookie-jar.js";
import type { ProxyPool } from "../proxy/proxy-pool.js";
import { CodexApi } from "../proxy/codex-api.js";
import type { CodexResponsesRequest, CodexInputItem } from "../proxy/codex-api.js";
import type { ParsedRateLimit } from "../proxy/rate-limit-headers.js";
import { prepareSchema } from "../translation/shared-utils.js";

// ── Server attachment ──────────────────────────────────────────────

export interface WsServerHandle {
  wss: WebSocketServer;
  close: () => void;
}

/**
 * Attach a WebSocket server to the given HTTP server.
 * WS requests are routed via the "upgrade" event on the HTTP server.
 */
export function attachWebSocketServer(
  httpServer: Server,
  accountPool: AccountPool,
  cookieJar: CookieJar,
  proxyPool: ProxyPool,
): WsServerHandle {
  const wss = new WebSocketServer({ noServer: true });

  httpServer.on("upgrade", (request, socket, head) => {
    const pathname = request.url?.split("?")[0] || "";

    if (pathname === "/v1/responses"
      || pathname === "/ws/responses"
      || pathname === "/v1/responses/websocket") {
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit("connection", ws, request);
      });
      return;
    }
    // Let HTTP handle other paths normally
  });

  wss.on("connection", (ws, request) => {
    handleClientConnection(ws, request, accountPool, cookieJar, proxyPool);
  });

  wss.on("error", (err: Error) => {
    console.error("[WS] Server error:", err.message);
  });

  console.log("[WS] Server attached — listening on /v1/responses and /ws/responses");

  return { wss, close: () => wss.close() };
}

// ── Client connection handler ──────────────────────────────────────

function handleClientConnection(
  ws: WebSocket,
  request: IncomingMessage,
  accountPool: AccountPool,
  cookieJar: CookieJar,
  proxyPool: ProxyPool,
): void {
  const clientId = request.socket.remoteAddress ?? "unknown";
  console.log(`[WS] Client connected from ${clientId}`);

  ws.on("message", async (rawData: WebSocket.RawData) => {
    const text = rawData.toString("utf-8");

    let body: Record<string, unknown>;
    try {
      body = JSON.parse(text) as Record<string, unknown>;
    } catch {
      sendError(ws, "invalid_json", "Malformed JSON request body");
      return;
    }

    // response.create is the only supported message type
    if (body.type !== "response.create") {
      sendError(ws, "invalid_type", `Unsupported message type: ${body.type ?? "none"}`);
      return;
    }

    await handleCreateResponse(ws, body, accountPool, cookieJar, proxyPool);
  });

  ws.on("close", (code) => {
    console.log(`[WS] Client disconnected from ${clientId} (code=${code})`);
  });

  ws.on("error", (err: Error) => {
    console.error(`[WS] Client error from ${clientId}:`, err.message);
  });
}

/**
 * Handle a response.create request from the client.
 * Builds a CodexResponsesRequest, calls the Codex API, and streams events back.
 */
async function handleCreateResponse(
  ws: WebSocket,
  body: Record<string, unknown>,
  accountPool: AccountPool,
  cookieJar: CookieJar,
  proxyPool: ProxyPool,
): Promise<void> {
  // Auth check
  if (!accountPool.isAuthenticated()) {
    sendError(ws, "not_authenticated", "No available accounts. Please login first.");
    return;
  }

  // Parse request fields
  const rawModel = typeof body.model === "string" ? body.model : "codex";
  const model = rawModel.replace(/(-fast|-flex|-auto)$/, "");

  const codexRequest: CodexResponsesRequest = {
    model,
    instructions: typeof body.instructions === "string" ? body.instructions : "",
    input: Array.isArray(body.input) ? (body.input as CodexInputItem[]) : [],
    stream: true,
    store: false,
  };

  // WebSocket transport (multi-turn support)
  codexRequest.useWebSocket = true;
  if (typeof body.previous_response_id === "string") {
    codexRequest.previous_response_id = body.previous_response_id;
  }

  // Reasoning
  if (isRecord(body.reasoning)) {
    codexRequest.reasoning = {};
    if (typeof body.reasoning.effort === "string") codexRequest.reasoning.effort = body.reasoning.effort;
    if (typeof body.reasoning.summary === "string") codexRequest.reasoning.summary = body.reasoning.summary;
  }

  // Service tier
  if (typeof body.service_tier === "string") {
    codexRequest.service_tier = body.service_tier;
  }

  // Tools
  if (Array.isArray(body.tools)) codexRequest.tools = body.tools;
  if (body.tool_choice !== undefined) {
    codexRequest.tool_choice = body.tool_choice as CodexResponsesRequest["tool_choice"];
  }

  // Text format
  if (isRecord(body.text) && isRecord(body.text.format)) {
    let formatSchema: Record<string, unknown> | undefined;
    if (isRecord(body.text.format.schema)) {
      const prepared = prepareSchema(body.text.format.schema);
      formatSchema = prepared.schema;
    }
    codexRequest.text = {
      format: {
        type: body.text.format.type as "text" | "json_object" | "json_schema",
        ...(typeof body.text.format.name === "string" ? { name: body.text.format.name } : {}),
        ...(formatSchema ? { schema: formatSchema } : {}),
        ...(typeof body.text.format.strict === "boolean" ? { strict: body.text.format.strict } : {}),
      },
    };
  }

  // Optional fields
  if (typeof body.prompt_cache_key === "string") codexRequest.prompt_cache_key = body.prompt_cache_key;
  if (Array.isArray(body.include)) codexRequest.include = body.include as string[];

  // Acquire account
  const entry = accountPool.acquire();
  if (!entry) {
    sendError(ws, "no_account", "No available accounts. All accounts are rate-limited or expired.");
    return;
  }

  let capturedResponseId: string | null = null;
  const onRateLimits = (_rl: ParsedRateLimit) => {};
  const onUsage = (_u: { input_tokens: number; output_tokens: number }) => {};

  try {
    const api = new CodexApi(
      entry.token,
      entry.accountId,
      cookieJar,
      entry.entryId,
      proxyPool.resolveProxyUrl(entry.entryId) ?? null,
      undefined,
      undefined,
    );

    const response = await api.createResponse(codexRequest, undefined, onRateLimits);

    // Stream events back as JSON frames
    for await (const event of api.parseStream(response)) {
      const frame = JSON.stringify({
        type: event.event,
        data: event.data,
      });
      ws.send(frame);

      // Capture response ID for multi-turn
      if (event.event === "response.created" || event.event === "response.in_progress") {
        if (isRecord(event.data) && typeof event.data.response === "object" && event.data.response) {
          const resp = event.data.response as Record<string, unknown>;
          if (typeof resp.id === "string") capturedResponseId = resp.id;
        }
      }

      // Close stream after terminal events
      if (event.event === "response.completed" || event.event === "response.failed" || event.event === "error") {
        if (event.event === "response.completed" && capturedResponseId) {
          console.log(`[WS] Response completed: id=${capturedResponseId}`);
        }
      }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`[WS] Request error: ${msg}`);

    if (msg.includes("429") || msg.toLowerCase().includes("rate_limit")) {
      sendError(ws, "rate_limit_exceeded", msg);
    } else if (msg.includes("401") || msg.toLowerCase().includes("auth")) {
      sendError(ws, "authentication_error", msg);
    } else {
      sendError(ws, "request_failed", msg);
    }
  } finally {
    accountPool.release(entry.entryId);
  }
}

// ── Helpers ─────────────────────────────────────────────────────────

function sendError(ws: WebSocket, code: string, message: string): void {
  ws.send(
    JSON.stringify({
      type: "error",
      data: { code, message },
    })
  );
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}
