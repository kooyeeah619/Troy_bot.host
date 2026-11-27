const http = require("http");

const PORT = process.env.PORT || 3000;
const UPSTREAM = "https://100milionseek.top/v1/chat/completions";

const API_KEYS = [
  process.env.API_KEY_1,
  process.env.API_KEY_2,
  process.env.API_KEY_3
].filter(Boolean);

const GATEWAY_APP_KEY = process.env.GATEWAY_APP_KEY;

const RETRY_WAIT = 60000;
const UPSTREAM_TIMEOUT = 120000;
const MAX_REQUEST_SIZE = 2 * 1024 * 1024;
const MAX_RESPONSE_SIZE = 64 * 1024 * 1024;

class RetryableError extends Error {}

function json(res, status, data) {
  if (res.destroyed || res.writableEnded) return;

  res.writeHead(status, {
    "Content-Type": "application/json"
  });

  res.end(JSON.stringify(data));
}

function waitForRetry(res, isGone) {
  return new Promise(resolve => {
    if (isGone()) return resolve(false);

    let finished = false;

    const finish = result => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      res.off("close", onClose);
      resolve(result);
    };

    const onClose = () => finish(false);
    const timer = setTimeout(() => finish(true), RETRY_WAIT);

    res.once("close", onClose);
  });
}

async function readRequest(req) {
  const chunks = [];
  let size = 0;

  for await (const chunk of req) {
    size += chunk.length;

    if (size > MAX_REQUEST_SIZE) {
      throw new RetryableError("Request too large");
    }

    chunks.push(chunk);
  }

  return Buffer.concat(chunks).toString("utf8");
}

async function readUpstream(response) {
  if (!response.body) {
    throw new RetryableError("Empty upstream body");
  }

  const reader = response.body.getReader();
  const chunks = [];
  let size = 0;

  try {
    while (true) {
      const result = await reader.read();

      if (result.done) break;

      size += result.value.length;

      if (size > MAX_RESPONSE_SIZE) {
        throw new RetryableError("Response too large");
      }

      chunks.push(Buffer.from(result.value));
    }

    return Buffer.concat(chunks);
  } catch (error) {
    await reader.cancel().catch(() => {});
    throw error;
  } finally {
    reader.releaseLock();
  }
}

function validateResponse(buffer, contentType) {
  const text = buffer.toString("utf8").trim();

  if (!text) {
    throw new RetryableError("Empty response");
  }

  const isSSE =
    /text\/event-stream/i.test(contentType || "") ||
    /(^|\n)(data:|event:)/m.test(text);

  if (isSSE) {
    let done = false;

    for (const event of text.split(/\r?\n\r?\n/)) {
      let eventName = "";
      const data = [];

      for (const line of event.split(/\r?\n/)) {
        if (line.startsWith("event:")) {
          eventName = line.slice(6).trim();
        }

        if (line.startsWith("data:")) {
          data.push(line.slice(5).trimStart());
        }
      }

      const payload = data.join("\n");

      if (eventName === "error") {
        throw new RetryableError("SSE error event");
      }

      if (!payload) continue;

      if (payload === "[DONE]") {
        done = true;
        continue;
      }

      let parsed;

      try {
        parsed = JSON.parse(payload);
      } catch {
        throw new RetryableError("Invalid SSE JSON");
      }

      if (parsed?.error || parsed?.type === "error") {
        throw new RetryableError("Upstream SSE error");
      }
    }

    if (!done) {
      throw new RetryableError("Incomplete SSE stream");
    }

    return;
  }

  let parsed;

  try {
    parsed = JSON.parse(text);
  } catch {
    throw new RetryableError("Invalid JSON response");
  }

  if (parsed?.error) {
    throw new RetryableError("Upstream JSON error");
  }
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
    req.url !== "/v1/chat/completions"
  ) {
    return json(res, 404, { error: "Not found" });
  }

  if (
    !GATEWAY_APP_KEY ||
    req.headers.authorization !== `Bearer ${GATEWAY_APP_KEY}`
  ) {
    return json(res, 401, {
      error: "Invalid Gateway Key"
    });
  }

  if (!API_KEYS.length) {
    return json(res, 500, {
      error: "No API keys configured"
    });
  }

  let clientGone = false;
  let activeController = null;

  res.on("close", () => {
    if (!res.writableEnded) {
      clientGone = true;

      if (activeController) {
        activeController.abort();
      }
    }
  });

  let data;

  try {
    const body = await readRequest(req);
    data = JSON.parse(body);
  } catch (error) {
    if (error.message === "Request too large") {
      return json(res, 413, {
        error: "Request too large"
      });
    }

    return json(res, 400, {
      error: "Invalid request JSON"
    });
  }

  if (!data || typeof data !== "object" || Array.isArray(data)) {
    return json(res, 400, {
      error: "Invalid request body"
    });
  }

  while (!clientGone) {
    let cycleFailed = false;

    for (let i = 0; i < API_KEYS.length; i++) {
      if (clientGone) return;

      const key = API_KEYS[i];
      const controller = new AbortController();

      activeController = controller;

      let timeout;

      try {
        console.log(`Trying API key slot ${i + 1}`);

        timeout = setTimeout(() => {
          controller.abort();
        }, UPSTREAM_TIMEOUT);

        const upstream = await fetch(UPSTREAM, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${key}`,
            "Content-Type": "application/json",
            Accept: "application/json, text/event-stream"
          },
          body: JSON.stringify(data),
          signal: controller.signal
        });

        if (!upstream.ok) {
          const status = upstream.status;

          await upstream.body?.cancel().catch(() => {});

          throw new RetryableError(`HTTP ${status}`);
        }

        const contentType =
          upstream.headers.get("content-type") ||
          "application/json";

        const responseBody = await readUpstream(upstream);

        validateResponse(responseBody, contentType);

        if (clientGone) return;

        clearTimeout(timeout);
        activeController = null;

        console.log(`API key slot ${i + 1} succeeded`);

        res.writeHead(upstream.status, {
          "Content-Type": contentType,
          "Cache-Control": "no-cache",
          "X-Accel-Buffering": "no",
          "Content-Length": responseBody.length
        });

        return res.end(responseBody);

      } catch (error) {
        clearTimeout(timeout);
        activeController = null;

        if (clientGone) return;

        const reason = controller.signal.aborted
          ? "timeout"
          : error.message;

        console.warn(
          `API key slot ${i + 1} failed: ${reason}`
        );

        cycleFailed = true;
      }
    }

    if (cycleFailed && !clientGone) {
      console.warn(
        "All API keys failed. Retrying from Key 1 in 60 seconds."
      );

      const shouldRetry = await waitForRetry(
        res,
        () => clientGone
      );

      if (!shouldRetry) return;
    }
  }
});

server.listen(PORT, () => {
  console.log(`Universal API Gateway running on port ${PORT}`);
});
