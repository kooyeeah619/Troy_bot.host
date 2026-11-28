const http = require("http");

const PORT = process.env.PORT || 3000;

const DEEPSEEK_UPSTREAM =
  "https://100milionseek.top/v1/chat/completions";

const GROK_BASE_URL = (
  process.env.GROK_BASE_URL || "https://100miliongrok.top/v1"
).replace(/\/+$/, "");

const GROK_UPSTREAM = `${GROK_BASE_URL}/chat/completions`;

const GATEWAY_APP_KEY = process.env.GATEWAY_APP_KEY;
const GROK_API_KEY = process.env.GROK_API_KEY;

const API_KEYS = [
  { slot: 1, key: process.env.API_KEY_1 },
  { slot: 2, key: process.env.API_KEY_2 },
  { slot: 3, key: process.env.API_KEY_3 }
].filter(item => item.key && item.key.trim());

const UPSTREAM_TIMEOUT = 180000;
const MAX_REQUEST_SIZE = 20 * 1024 * 1024;

const SSE_PREFLIGHT_CHUNKS = 4;
const SSE_PREFLIGHT_MAX_BYTES = 16384;

let activeKeyIndex = 0;
let keyStateVersion = 0;

function sendJson(res, status, data) {
  if (res.headersSent) {
    return res.end();
  }

  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8"
  });

  res.end(JSON.stringify(data));
}

function setActiveKey(index, reason) {
  if (!API_KEYS.length || index < 0 || index >= API_KEYS.length) {
    return;
  }

  if (activeKeyIndex !== index) {
    const previous = API_KEYS[activeKeyIndex]?.slot;
    const next = API_KEYS[index].slot;

    activeKeyIndex = index;
    keyStateVersion++;

    console.log(
      `Active API key changed: slot ${previous} -> slot ${next} (${reason})`
    );
  }
}

function advanceKey(keyIndex) {
  if (API_KEYS.length <= 1) return;

  if (activeKeyIndex === keyIndex) {
    setActiveKey(
      (keyIndex + 1) % API_KEYS.length,
      "upstream failure"
    );
  }
}

function getNextUntriedKey(
  attemptedKeys,
  keys,
  currentIndex
) {
  if (!keys.length) return -1;

  for (let offset = 0; offset < keys.length; offset++) {
    const index = (currentIndex + offset) % keys.length;

    if (!attemptedKeys.has(index)) {
      return index;
    }
  }

  return -1;
}

function isBusyResponse(value) {
  return /service\s+is\s+busy|passes\s+from|raise\s+your\s+limits|temporarily\s+unavailable|upstream\s+busy|too\s+many\s+requests|rate\s+limit/i.test(
    value
  );
}

async function readRequest(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let exceeded = false;

    req.on("data", chunk => {
      size += chunk.length;

      if (size > MAX_REQUEST_SIZE) {
        exceeded = true;
        return;
      }

      chunks.push(chunk);
    });

    req.on("end", () => {
      if (exceeded) {
        return reject(
          Object.assign(
            new Error("Request body exceeds 20 MB limit."),
            { statusCode: 413 }
          )
        );
      }

      resolve(Buffer.concat(chunks));
    });

    req.on("error", reject);
  });
}

function waitForDrain(res) {
  return new Promise(resolve => {
    if (res.destroyed || res.writableEnded) {
      return resolve();
    }

    res.once("drain", resolve);
    res.once("close", resolve);
  });
}

async function sendUpstream(req, res, body) {
  let requestModel = "";

  try {
    requestModel = JSON.parse(body.toString("utf8")).model || "";
  } catch {
    return sendJson(res, 400, {
      error: { message: "Invalid model request." }
    });
  }

  const useGrok = requestModel === "grok-4.7";

  const requestKeys = useGrok
    ? (GROK_API_KEY
        ? [{ slot: "GROK", key: GROK_API_KEY }]
        : [])
    : API_KEYS;

  const requestUpstream = useGrok
    ? GROK_UPSTREAM
    : DEEPSEEK_UPSTREAM;

  if (!requestKeys.length) {
    return sendJson(res, 500, {
      error: {
        message: useGrok
          ? "GROK_API_KEY is not configured."
          : "No DeepSeek API keys configured."
      }
    });
  }

  const attemptedKeys = new Set();
  let lastError = null;

  for (let attempt = 0; attempt < requestKeys.length; attempt++) {
    const startIndex = useGrok ? 0 : activeKeyIndex;

    const keyIndex = getNextUntriedKey(
      attemptedKeys,
      requestKeys,
      startIndex
    );

    if (keyIndex < 0) break;

    attemptedKeys.add(keyIndex);

    const selectedKey = requestKeys[keyIndex];

    console.log(
      `Using ${useGrok ? "Grok" : "DeepSeek"} API key slot ${selectedKey.slot}`
    );

    const controller = new AbortController();
    let idleTimer;

    function resetIdleTimer() {
      clearTimeout(idleTimer);

      idleTimer = setTimeout(() => {
        controller.abort(
          new Error("Upstream idle timeout after 3 minutes.")
        );
      }, UPSTREAM_TIMEOUT);
    }

    resetIdleTimer();

    let upstreamResponse;
    let reader;

    try {
      upstreamResponse = await fetch(requestUpstream, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${selectedKey.key}`,
          "Content-Type": "application/json",
          "Accept": "text/event-stream, application/json"
        },
        body,
        signal: controller.signal
      });

      if (!upstreamResponse.ok) {
        const errorText = await upstreamResponse.text();
        clearTimeout(idleTimer);

        lastError = new Error(
          `HTTP ${upstreamResponse.status}: ${errorText.slice(0, 1000)}`
        );

        console.log(
          `API key slot ${selectedKey.slot} failed: HTTP ${upstreamResponse.status}`
        );

        if (!useGrok) advanceKey(keyIndex);

        continue;
      }

      if (!upstreamResponse.body) {
        clearTimeout(idleTimer);

        lastError = new Error("Upstream returned an empty response.");

        if (!useGrok) advanceKey(keyIndex);

        continue;
      }

      reader = upstreamResponse.body.getReader();

      const preflightChunks = [];
      let preflightBytes = 0;
      let preflightCount = 0;
      let preflightEnded = false;

      while (
        preflightCount < SSE_PREFLIGHT_CHUNKS &&
        preflightBytes < SSE_PREFLIGHT_MAX_BYTES
      ) {
        const result = await reader.read();

        resetIdleTimer();

        if (result.done) {
          preflightEnded = true;
          break;
        }

        preflightChunks.push(result.value);
        preflightBytes += result.value.length;
        preflightCount++;

        const preview = Buffer.concat(
          preflightChunks.map(chunk => Buffer.from(chunk))
        ).toString("utf8");

        if (isBusyResponse(preview)) {
          lastError = new Error("Upstream returned a busy response.");

          console.log(
            `API key slot ${selectedKey.slot} returned a busy response. Switching key.`
          );

          await reader.cancel().catch(() => {});
          clearTimeout(idleTimer);

          if (!useGrok) advanceKey(keyIndex);

          break;
        }
      }

      if (lastError?.message === "Upstream returned a busy response.") {
        continue;
      }

      if (!useGrok) {
        setActiveKey(keyIndex, "successful preflight");
      }

      console.log(
        `API key slot ${selectedKey.slot} passed preflight. Active key retained.`
      );

      const headers = {
        "Content-Type":
          upstreamResponse.headers.get("content-type") ||
          "application/json",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no"
      };

      const contentEncoding =
        upstreamResponse.headers.get("content-encoding");

      if (contentEncoding) {
        headers["Content-Encoding"] = contentEncoding;
      }

      res.writeHead(upstreamResponse.status, headers);

      for (const chunk of preflightChunks) {
        if (res.destroyed) break;

        if (!res.write(Buffer.from(chunk))) {
          await waitForDrain(res);
        }
      }

      while (!preflightEnded && !res.destroyed) {
        const result = await reader.read();

        resetIdleTimer();

        if (result.done) break;

        if (!res.write(Buffer.from(result.value))) {
          await waitForDrain(res);
        }
      }

      clearTimeout(idleTimer);

      if (!res.destroyed && !res.writableEnded) {
        res.end();
      }

      console.log(
        `API key slot ${selectedKey.slot} response completed successfully.`
      );

      return;

    } catch (error) {
      clearTimeout(idleTimer);

      if (reader) {
        await reader.cancel().catch(() => {});
      }

      lastError = error;

      console.log(
        `API key slot ${selectedKey.slot} error: ${error.message}`
      );

      if (!useGrok) {
        advanceKey(keyIndex);
      }

      if (res.headersSent) {
        if (!res.destroyed && !res.writableEnded) {
          res.end();
        }

        if (!useGrok) {
          console.log(
            `Next request will use active API key slot ${API_KEYS[activeKeyIndex].slot}.`
          );
        }

        return;
      }
    }
  }

  if (!res.headersSent) {
    sendJson(res, 502, {
      error: {
        message: useGrok
          ? "Grok request failed."
          : "All DeepSeek API keys failed.",
        details: lastError
          ? lastError.message
          : "No available API key succeeded."
      }
    });
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
    return sendJson(res, 200, {
      status: "ok",
      service: "Troy Gateway",
      providers: {
        deepseek: API_KEYS.length > 0,
        grok: Boolean(GROK_API_KEY)
      },
      deepseekKeysConfigured: API_KEYS.length,
      activeDeepSeekKey: API_KEYS[activeKeyIndex]?.slot || null
    });
  }

  if (
    req.method !== "POST" ||
    req.url !== "/v1/chat/completions"
  ) {
    return sendJson(res, 404, {
      error: { message: "Not found." }
    });
  }

  const authorization = req.headers.authorization || "";
  const suppliedKey = authorization.replace(/^Bearer\s+/i, "");

  if (
    !GATEWAY_APP_KEY ||
    suppliedKey !== GATEWAY_APP_KEY
  ) {
    return sendJson(res, 401, {
      error: { message: "Invalid gateway API key." }
    });
  }

  try {
    const body = await readRequest(req);
    await sendUpstream(req, res, body);
  } catch (error) {
    sendJson(res, error.statusCode || 500, {
      error: { message: error.message }
    });
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Troy Gateway running on port ${PORT}`);
  console.log(`DeepSeek API keys configured: ${API_KEYS.length}`);
  console.log(`Initial active DeepSeek key: ${API_KEYS[activeKeyIndex]?.slot || "none"}`);
  console.log(`Grok API configured: ${Boolean(GROK_API_KEY)}`);
});
