
const http = require("http");

const PORT = process.env.PORT || 3000;
const UPSTREAM = "https://100milionseek.top/v1/chat/completions";
const GATEWAY_APP_KEY = process.env.GATEWAY_APP_KEY;

const API_KEYS = [
  { slot: 1, key: process.env.API_KEY_1 },
  { slot: 2, key: process.env.API_KEY_2 },
  { slot: 3, key: process.env.API_KEY_3 }
].filter(item => item.key && item.key.trim());

const UPSTREAM_TIMEOUT = 120000;
const MAX_REQUEST_SIZE = 20 * 1024 * 1024;
const SSE_PREFLIGHT_CHUNKS = 8;
const SSE_PREFLIGHT_MAX_BYTES = 16384;

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

function advanceKey(keyIndex) {
  if (API_KEYS.length && activeKeyIndex === keyIndex) {
    activeKeyIndex = (keyIndex + 1) % API_KEYS.length;
  }
}

function readRequest(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    let finished = false;
    const chunks = [];

    req.on("data", chunk => {
      if (finished) return;

      size += chunk.length;

      if (size > MAX_REQUEST_SIZE) {
        finished = true;
        reject(new Error("Request too large"));
        req.destroy();
        return;
      }

      chunks.push(chunk);
    });

    req.on("end", () => {
      if (finished) return;
      finished = true;
      resolve(Buffer.concat(chunks));
    });

    req.on("error", error => {
      if (finished) return;
      finished = true;
      reject(error);
    });
  });
}

function waitForDrain(res) {
  return new Promise(resolve => {
    if (res.destroyed || res.writableEnded) return resolve(false);

    const cleanup = () => {
      res.off("drain", onDrain);
      res.off("close", onClose);
      res.off("error", onError);
    };

    const onDrain = () => {
      cleanup();
      resolve(true);
    };

    const onClose = () => {
      cleanup();
      resolve(false);
    };

    const onError = () => {
      cleanup();
      resolve(false);
    };

    res.once("drain", onDrain);
    res.once("close", onClose);
    res.once("error", onError);
  });
}

async function sendUpstream(req, res, body) {
  if (!API_KEYS.length) {
    return sendJson(res, 500, {
      error: { message: "No API keys configured." }
    });
  }

  const startIndex = activeKeyIndex;
  let lastError = null;

  for (let attempt = 0; attempt < API_KEYS.length; attempt++) {
    if (res.destroyed) return;

    const keyIndex = (startIndex + attempt) % API_KEYS.length;
    const keyEntry = API_KEYS[keyIndex];

    console.log(`Using active API key slot ${keyEntry.slot}`);

    const controller = new AbortController();
    let timeout = null;
    let timedOut = false;

    function armTimeout() {
      clearTimeout(timeout);
      timedOut = false;

      timeout = setTimeout(() => {
        timedOut = true;
        controller.abort();
      }, UPSTREAM_TIMEOUT);
    }

    function clearActiveTimeout() {
      clearTimeout(timeout);
      timeout = null;
    }

    const onClientClose = () => {
      if (!res.writableEnded) {
        console.log("Client disconnected. Aborting upstream request.");
        controller.abort();
      }
    };

    res.once("close", onClientClose);

    let reader = null;
    let retryBeforeHeaders = false;

    try {
      armTimeout();

      const upstream = await fetch(UPSTREAM, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${keyEntry.key}`,
          "Content-Type": "application/json",
          "Accept": "text/event-stream, application/json"
        },
        body,
        signal: controller.signal
      });

      if (res.destroyed) {
        clearActiveTimeout();
        res.off("close", onClientClose);
        return;
      }

      if (!upstream.ok) {
        let errorText = "";

        try {
          errorText = await upstream.text();
        } catch (error) {
          errorText = error.message;
        }

        clearActiveTimeout();

        lastError = new Error(
          `Upstream HTTP ${upstream.status}: ${errorText.slice(0, 1000)}`
        );

        console.error(
          `API key slot ${keyEntry.slot} failed: HTTP ${upstream.status}`
        );

        res.off("close", onClientClose);
        advanceKey(keyIndex);
        continue;
      }

      if (!upstream.body) {
        clearActiveTimeout();

        lastError = new Error("Upstream returned an empty response.");

        console.error(
          `API key slot ${keyEntry.slot} returned an empty response.`
        );

        res.off("close", onClientClose);
        advanceKey(keyIndex);
        continue;
      }

      const contentType =
        upstream.headers.get("content-type") || "application/json";

      reader = upstream.body.getReader();

      const buffered = [];
      let bufferedSize = 0;
      let retryBusy = false;
      let streamFinished = false;

      // Preflight: errors here can still safely retry another key.
      try {
        if (contentType.toLowerCase().includes("text/event-stream")) {
          let inspected = "";
          const decoder = new TextDecoder();

          for (let i = 0; i < SSE_PREFLIGHT_CHUNKS; i++) {
            const result = await reader.read();

            if (result.done) {
              streamFinished = true;
              break;
            }

            buffered.push(result.value);
            bufferedSize += result.value.byteLength;

            inspected += decoder.decode(result.value, {
              stream: true
            });

            if (isBusyResponse(inspected)) {
              retryBusy = true;
              break;
            }

            if (bufferedSize >= SSE_PREFLIGHT_MAX_BYTES) break;
          }

          inspected += decoder.decode();

          if (isBusyResponse(inspected)) {
            retryBusy = true;
          }

        } else {
          // JSON response: inspect the complete body before forwarding.
          while (true) {
            const result = await reader.read();

            if (result.done) {
              streamFinished = true;
              break;
            }

            buffered.push(result.value);
            bufferedSize += result.value.byteLength;
          }

          const completeBody = Buffer.concat(
            buffered.map(chunk => Buffer.from(chunk)),
            bufferedSize
          );

          if (isBusyResponse(completeBody.toString("utf8"))) {
            retryBusy = true;
          }
        }

      } catch (preflightError) {
        clearActiveTimeout();

        if (res.destroyed) {
          res.off("close", onClientClose);
          return;
        }

        lastError = new Error(
          timedOut
            ? "Upstream preflight timeout."
            : `Upstream preflight error: ${preflightError.message}`
        );

        console.error(
          `API key slot ${keyEntry.slot} preflight failed:`,
          lastError.message
        );

        try {
          await reader.cancel();
        } catch {}

        res.off("close", onClientClose);
        advanceKey(keyIndex);
        continue;
      }

      clearActiveTimeout();

      if (retryBusy) {
        lastError = new Error("Upstream service returned a busy response.");

        console.error(
          `API key slot ${keyEntry.slot} returned a busy response. Switching key.`
        );

        try {
          await reader.cancel();
        } catch {}

        res.off("close", onClientClose);
        advanceKey(keyIndex);
        continue;
      }

      if (res.destroyed) {
        try {
          await reader.cancel();
        } catch {}

        res.off("close", onClientClose);
        return;
      }

      // Preflight passed. This key is now active.
      activeKeyIndex = keyIndex;

      console.log(
        `API key slot ${keyEntry.slot} passed preflight. Active key retained.`
      );

      res.writeHead(upstream.status, {
        "Content-Type": contentType,
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no"
      });

      // Forward buffered chunks without changing their contents.
      for (const chunk of buffered) {
        if (res.destroyed) break;

        if (!res.write(chunk)) {
          const drained = await waitForDrain(res);
          if (!drained) break;
        }
      }

      // Continue normal streaming immediately.
      if (!streamFinished && !res.destroyed) {
        armTimeout();

        while (!res.destroyed) {
          const result = await reader.read();

          if (result.done) {
            streamFinished = true;
            break;
          }

          armTimeout();

          if (!res.write(result.value)) {
            const drained = await waitForDrain(res);
            if (!drained) break;
          }
        }
      }

      clearActiveTimeout();

      if (!res.destroyed && !res.writableEnded) {
        res.end();
      }

      if (streamFinished) {
        console.log(
          `API key slot ${keyEntry.slot} response completed successfully.`
        );
      } else if (!res.destroyed) {
        console.error(
          `API key slot ${keyEntry.slot} stream ended unexpectedly.`
        );
        res.destroy();
      }

      return;

    } catch (error) {
      clearActiveTimeout();

      if (res.destroyed) {
        res.off("close", onClientClose);
        return;
      }

      // If headers were already sent, a transparent retry could
      // duplicate or corrupt the answer. Do not retry at this point.
      if (res.headersSent) {
        console.error(
          `API key slot ${keyEntry.slot} stream failed after response started:`,
          timedOut ? "timeout" : error.message
        );

        if (!res.destroyed) res.destroy(error);

        res.off("close", onClientClose);
        return;
      }

      lastError = new Error(
        timedOut
          ? "Upstream request timeout."
          : error.message
      );

      console.error(
        `API key slot ${keyEntry.slot} failed: ${lastError.message}`
      );

      if (reader) {
        try {
          await reader.cancel();
        } catch {}
      }

      res.off("close", onClientClose);
      advanceKey(keyIndex);
    } finally {
      clearActiveTimeout();

      if (reader) {
        try {
          reader.releaseLock();
        } catch {}
      }

      res.off("close", onClientClose);
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
    } else if (!res.destroyed) {
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
