const http = require("http");

const PORT = process.env.PORT || 3000;
const UPSTREAM = "https://100milionseek.top/v1/chat/completions";
const GATEWAY_APP_KEY = process.env.GATEWAY_APP_KEY;

const API_KEYS = [
  { slot: 1, key: process.env.API_KEY_1 },
  { slot: 2, key: process.env.API_KEY_2 },
  { slot: 3, key: process.env.API_KEY_3 }
].filter(item => item.key && item.key.trim());

const UPSTREAM_TIMEOUT = 120000; // 120 seconds idle timeout
const MAX_REQUEST_SIZE = 2 * 1024 * 1024;
const BUFFER_LIMIT = 5; // Bilang ng chunks bago i-commit sa client

let activeKeyIndex = 0;

function sendJson(res, status, data) {
  if (res.headersSent || res.destroyed) return;

  res.writeHead(status, {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache"
  });

  res.end(JSON.stringify(data));
}

function isBusyResponse(value) {
  return /service\s+is\s+busy\s+right\s+now|passes\s+from\s+\$?1\s+raise\s+your\s+limits|temporarily\s+unavailable|upstream\s+busy/i.test(value);
}

function advanceKey(index) {
  if (!API_KEYS.length) return;

  if (activeKeyIndex === index) {
    activeKeyIndex = (index + 1) % API_KEYS.length;
  }
}

function readRequest(req) {
  return new Promise((resolve, reject) => {
    let chunks = [];
    let size = 0;

    req.on("data", chunk => {
      size += chunk.length;

      if (size > MAX_REQUEST_SIZE) {
        reject(new Error("Request too large"));
        req.destroy();
        return;
      }

      chunks.push(chunk);
    });

    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

async function sendUpstream(req, res, body) {
  if (!API_KEYS.length) {
    return sendJson(res, 500, {
      error: { message: "No upstream API keys configured" }
    });
  }

  let lastError = null;

  for (let attempt = 0; attempt < API_KEYS.length; attempt++) {
    if (res.destroyed || res.writableEnded) return;

    const index = activeKeyIndex;
    const item = API_KEYS[index];
    const controller = new AbortController();
    
    let timeout = null;
    let clientCommitted = false;

    const resetTimeout = () => {
        if (timeout) clearTimeout(timeout);
        timeout = setTimeout(() => {
            console.log(`Idle timeout reached for slot ${item.slot}`);
            controller.abort();
        }, UPSTREAM_TIMEOUT);
    };

    resetTimeout();

    try {
      console.log(`Using API key slot ${item.slot}`);

      const upstream = await fetch(UPSTREAM, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${item.key}`,
          "Accept": "text/event-stream, application/json"
        },
        body,
        signal: controller.signal
      });

      if (!upstream.ok) {
        const errorText = await upstream.text();
        throw new Error(`HTTP ${upstream.status}: ${errorText.slice(0, 1000)}`);
      }

      if (!upstream.body) {
        throw new Error("Empty upstream response");
      }

      const contentType = upstream.headers.get("content-type") || "application/json";
      const reader = upstream.body.getReader();
      let bufferedChunks = [];

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        
        resetTimeout();

        const chunk = Buffer.from(value);
        
        if (isBusyResponse(chunk.toString("utf8"))) {
            throw new Error("Upstream busy during streaming");
        }

        bufferedChunks.push(chunk);

        if (!clientCommitted && bufferedChunks.length >= BUFFER_LIMIT) {
            activeKeyIndex = index;
            console.log(`API key slot ${item.slot} succeeded and committed to client.`);
            
            res.writeHead(upstream.status, {
                "Content-Type": contentType,
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive"
            });
            
            for (const c of bufferedChunks) {
                if (!res.write(c)) {
                    await new Promise((resolve, reject) => {
                        const cleanup = () => {
                            res.removeListener("drain", onDrain);
                            res.removeListener("close", onClose);
                            res.removeListener("error", onError);
                        };
                        const onDrain = () => { cleanup(); resolve(); };
                        const onClose = () => { cleanup(); reject(new Error("Client disconnected")); };
                        const onError = error => { cleanup(); reject(error); };
                        res.once("drain", onDrain);
                        res.once("close", onClose);
                        res.once("error", onError);
                    });
                }
            }
            bufferedChunks = [];
            clientCommitted = true;
        } else if (clientCommitted) {
            if (!res.write(chunk)) {
                await new Promise((resolve, reject) => {
                    const cleanup = () => {
                        res.removeListener("drain", onDrain);
                        res.removeListener("close", onClose);
                        res.removeListener("error", onError);
                    };
                    const onDrain = () => { cleanup(); resolve(); };
                    const onClose = () => { cleanup(); reject(new Error("Client disconnected")); };
                    const onError = error => { cleanup(); reject(error); };
                    res.once("drain", onDrain);
                    res.once("close", onClose);
                    res.once("error", onError);
                });
            }
        }
      }

      if (!clientCommitted) {
          activeKeyIndex = index;
          res.writeHead(upstream.status, { 
              "Content-Type": contentType, 
              "Cache-Control": "no-cache, no-transform",
              "X-Accel-Buffering": "no",
              "Connection": "keep-alive"
          });
          for (const c of bufferedChunks) res.write(c);
      }

      if (!res.destroyed && !res.writableEnded) {
        res.end();
      }

      console.log(`API key slot ${item.slot} stream completed.`);
      return;

    } catch (error) {
      lastError = error;
      
      if (clientCommitted) {
          console.error(`API key slot ${item.slot} failed mid-stream (Cannot fallback): ${error.message}`);
          if (!res.destroyed && !res.writableEnded) res.destroy(error);
          return;
      } else {
          console.error(`API key slot ${item.slot} failed before commit: ${error.message}`);
          advanceKey(index);
      }
    } finally {
      if (timeout) clearTimeout(timeout);
    }
  }

  sendJson(res, 502, {
    error: {
      message: "All upstream API keys failed",
      details: lastError ? lastError.message : "Unknown error"
    }
  });
}

const server = http.createServer(async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  if (req.method === "GET" && req.url === "/health") {
    return sendJson(res, 200, {
      status: "ok",
      configuredKeys: API_KEYS.length,
      activeSlot: API_KEYS.length ? API_KEYS[activeKeyIndex].slot : null
    });
  }

  if (req.method !== "POST" || req.url !== "/v1/chat/completions") {
    return sendJson(res, 404, {
      error: { message: "Not found" }
    });
  }

  if (!GATEWAY_APP_KEY) {
    return sendJson(res, 500, {
      error: { message: "Gateway app key not configured" }
    });
  }

  const auth = req.headers.authorization || "";
  const suppliedKey = auth.replace(/^Bearer\s+/i, "");

  if (suppliedKey !== GATEWAY_APP_KEY) {
    return sendJson(res, 401, {
      error: { message: "Unauthorized" }
    });
  }

  let body;

  try {
    body = await readRequest(req);
    JSON.parse(body.toString("utf8"));
  } catch (error) {
    return sendJson(res, 400, {
      error: { message: error.message || "Invalid JSON" }
    });
  }

  await sendUpstream(req, res, body);
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Troy Gateway running on port ${PORT}`);
  console.log(`Configured API keys: ${API_KEYS.length}`);
  console.log(
    `Initial active key: ${
      API_KEYS.length ? API_KEYS[activeKeyIndex].slot : "NONE"
    }`
  );
});
