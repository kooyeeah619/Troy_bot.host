
const http = require("http");
const { once } = require("events");

const PORT = process.env.PORT || 3000;
const UPSTREAM = "https://100milionseek.top/v1/chat/completions";

const GATEWAY_APP_KEY = process.env.GATEWAY_APP_KEY;

const API_KEYS = [
  { slot: 1, key: process.env.API_KEY_1 },
  { slot: 2, key: process.env.API_KEY_2 },
  { slot: 3, key: process.env.API_KEY_3 }
].filter(item => item.key && item.key.trim());

const UPSTREAM_TIMEOUT = 120000;
const MAX_REQUEST_SIZE = 2 * 1024 * 1024;

let activeKeyIndex = 0;

function sendJson(res, status, data) {
  if (res.headersSent) {
    res.destroy();
    return;
  }

  res.writeHead(status, {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache"
  });

  res.end(JSON.stringify(data));
}

function readRequest(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];

    req.on("data", chunk => {
      size += chunk.length;

      if (size > MAX_REQUEST_SIZE) {
        reject(new Error("Request too large"));
        req.destroy();
        return;
      }

      chunks.push(chunk);
    });

    req.on("end", () => {
      resolve(Buffer.concat(chunks));
    });

    req.on("error", reject);
  });
}

async function sendUpstream(req, res, body) {
  if (API_KEYS.length === 0) {
    return sendJson(res, 500, {
      error: { message: "No API keys configured." }
    });
  }

  const startIndex = activeKeyIndex;
  let lastError = null;

  for (let attempt = 0; attempt < API_KEYS.length; attempt++) {
    const keyIndex = (startIndex + attempt) % API_KEYS.length;
    const keyEntry = API_KEYS[keyIndex];

    console.log(`Using active API key slot ${keyEntry.slot}`);

    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      UPSTREAM_TIMEOUT
    );

    let upstream;

    try {
      upstream = await fetch(UPSTREAM, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${keyEntry.key}`,
          "Content-Type": "application/json",
          "Accept": "text/event-stream, application/json"
        },
        body,
        signal: controller.signal
      });

      if (!upstream.ok) {
        const errorText = await upstream.text();

        lastError = new Error(
          `Upstream HTTP ${upstream.status}: ${errorText.slice(0, 1000)}`
        );

        console.error(
          `API key slot ${keyEntry.slot} failed: HTTP ${upstream.status}`
        );

        clearTimeout(timeout);

        // Advance only if this key is still the active key.
        if (activeKeyIndex === keyIndex) {
          activeKeyIndex = (keyIndex + 1) % API_KEYS.length;
        }

        continue;
      }

      // Successful key becomes the active key.
      activeKeyIndex = keyIndex;

      console.log(
        `API key slot ${keyEntry.slot} succeeded. Active key retained.`
      );

      res.writeHead(upstream.status, {
        "Content-Type":
          upstream.headers.get("content-type") ||
          "application/json",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no"
      });

      if (!upstream.body) {
        clearTimeout(timeout);
        return res.end();
      }

      try {
        for await (const chunk of upstream.body) {
          if (res.destroyed) break;

          if (!res.write(chunk)) {
            await once(res, "drain");
          }
        }

        clearTimeout(timeout);

        if (!res.destroyed) {
          res.end();
        }
      } catch (streamError) {
        clearTimeout(timeout);
        console.error("Upstream stream error:", streamError.message);

        if (!res.destroyed) {
          res.destroy(streamError);
        }
      }

      return;
    } catch (error) {
      clearTimeout(timeout);
      lastError = error;

      console.error(
        `API key slot ${keyEntry.slot} failed: ${error.message}`
      );

      if (activeKeyIndex === keyIndex) {
        activeKeyIndex = (keyIndex + 1) % API_KEYS.length;
      }
    }
  }

  console.error("All configured API keys failed.");

  sendJson(res, 502, {
    error: {
      message: "All API keys failed.",
      details: lastError ? lastError.message : "Unknown upstream error."
    }
  });
}

const server = http.createServer(async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Authorization, Content-Type"
  );
  res.setHeader(
    "Access-Control-Allow-Methods",
    "POST, GET, OPTIONS"
  );

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, {
      "Content-Type": "text/plain"
    });
    return res.end("ok");
  }

  if (
    req.method !== "POST" ||
    req.url.split("?")[0] !== "/v1/chat/completions"
  ) {
    return sendJson(res, 404, {
      error: { message: "Not found." }
    });
  }

  if (!GATEWAY_APP_KEY) {
    return sendJson(res, 500, {
      error: { message: "Gateway authentication is not configured." }
    });
  }

  const authorization = req.headers.authorization || "";
  const suppliedKey = authorization.replace(/^Bearer\s+/i, "");

  if (suppliedKey !== GATEWAY_APP_KEY) {
    return sendJson(res, 401, {
      error: { message: "Invalid Gateway Key." }
    });
  }

  try {
    const body = await readRequest(req);

    if (!body.length) {
      return sendJson(res, 400, {
        error: { message: "Request body is empty." }
      });
    }

    try {
      JSON.parse(body.toString("utf8"));
    } catch {
      return sendJson(res, 400, {
        error: { message: "Invalid JSON request body." }
      });
    }

    await sendUpstream(req, res, body);
  } catch (error) {
    console.error("Gateway request error:", error.message);

    if (!res.headersSent) {
      sendJson(res, 500, {
        error: { message: error.message }
      });
    } else {
      res.destroy(error);
    }
  }
});

server.listen(PORT, () => {
  console.log(`Troy Gateway running on port ${PORT}`);
  console.log(`Configured API keys: ${API_KEYS.length}`);
  console.log(
    `Initial active key: ${
      API_KEYS.length ? API_KEYS[activeKeyIndex].slot : "NONE"
    }`
  );
});
