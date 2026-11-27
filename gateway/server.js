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
const MAX_REQUEST_SIZE = 2 * 1024 * 1024;
const SSE_PREFLIGHT_CHUNKS = 8;

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
return /service\s+is\s+busy\s+right\s+now|passes\s+from\s+$?1\s+raise\s+your\s+limits|temporarily\s+unavailable|upstream\s+busy/i.test(value);
}

function advanceKey(keyIndex) {
if (activeKeyIndex === keyIndex) {
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
  if (!res.writableEnded) controller.abort();  
};  

res.once("close", onClientClose);  

try {  
  // Timeout only while waiting for upstream headers.  
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

  clearActiveTimeout();  

  if (res.destroyed) {  
    res.off("close", onClientClose);  
    return;  
  }  

  if (!upstream.ok) {  
    const errorText = await upstream.text();  

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
    lastError = new Error("Upstream returned an empty response.");  
    res.off("close", onClientClose);  
    advanceKey(keyIndex);  
    continue;  
  }  

  const contentType =  
    upstream.headers.get("content-type") || "application/json";  

  const reader = upstream.body.getReader();  
  const buffered = [];  
  let bufferedSize = 0;  
  let retryBusy = false;  

  // Separate inactivity timeout for response reading.  
  armTimeout();  

  try {  
    if (contentType.toLowerCase().includes("text/event-stream")) {  
      // Inspect a small initial part of SSE before sending it.  
      // This catches upstream busy messages returned as assistant text.  
      let inspected = "";  

      for (let i = 0; i < SSE_PREFLIGHT_CHUNKS; i++) {  
        const result = await reader.read();  

        if (result.done) break;  

        buffered.push(result.value);  
        bufferedSize += result.value.byteLength;  

        inspected += new TextDecoder().decode(  
          result.value,  
          { stream: true }  
        );  

        armTimeout();  

        if (isBusyResponse(inspected)) {  
          retryBusy = true;  
          break;  
        }  

        // Avoid buffering an unexpectedly large initial response.  
        if (bufferedSize >= 16384) break;  
      }  

    } else {  
      // Non-streaming JSON: inspect the complete response.  
      while (true) {  
        const result = await reader.read();  

        if (result.done) break;  

        buffered.push(result.value);  
        bufferedSize += result.value.byteLength;  

        armTimeout();  
      }  

      const completeBody = Buffer.concat(  
        buffered.map(chunk => Buffer.from(chunk)),  
        bufferedSize  
      );  

      const responseText = completeBody.toString("utf8");  

      if (isBusyResponse(responseText)) {  
        retryBusy = true;  
      }  
    }  

    if (retryBusy) {  
      clearActiveTimeout();  

      console.error(  
        `API key slot ${keyEntry.slot} returned a busy response. Switching key.`  
      );  

      try {  
        await reader.cancel();  
      } catch {}  

      res.off("close", onClientClose);  
      lastError = new Error("Upstream service is busy.");  

      advanceKey(keyIndex);  
      continue;  
    }  

    // Only now mark this key as successful.  
    activeKeyIndex = keyIndex;  

    console.log(  
      `API key slot ${keyEntry.slot} succeeded. Active key retained.`  
    );  

    res.writeHead(upstream.status, {  
      "Content-Type": contentType,  
      "Cache-Control": "no-cache",  
      "X-Accel-Buffering": "no"  
    });  

    // Send the inspected initial chunks unchanged.  
    for (const chunk of buffered) {  
      if (res.destroyed) break;  

      if (!res.write(chunk)) {  
        await new Promise(resolve => {  
          res.once("drain", resolve);  
          res.once("close", resolve);  
        });  
      }  
    }  

    // Continue streaming the remaining response.  
    while (!res.destroyed) {  
      const result = await reader.read();  

      if (result.done) break;  

      armTimeout();  

      if (!res.write(result.value)) {  
        await new Promise(resolve => {  
          res.once("drain", resolve);  
          res.once("close", resolve);  
        });  
      }  
    }  

    clearActiveTimeout();  

    if (!res.destroyed && !res.writableEnded) {  
      res.end();  
    }  

  } catch (streamError) {  
    clearActiveTimeout();  

    if (timedOut) {  
      console.error(  
        `Upstream stream inactive for ${UPSTREAM_TIMEOUT / 1000}s.`  
      );  
    } else {  
      console.error(  
        "Upstream stream error:",  
        streamError.message  
      );  
    }  

    if (!res.destroyed) {  
      res.destroy(streamError);  
    }  

  } finally {  
    clearActiveTimeout();  

    try {  
      reader.releaseLock();  
    } catch {}  

    res.off("close", onClientClose);  
  }  

  // Do not retry after response bytes have been sent.  
  return;  

} catch (error) {  
  clearActiveTimeout();  
  res.off("close", onClientClose);  

  if (res.destroyed) return;  

  lastError = error;  

  console.error(  
    `API key slot ${keyEntry.slot} failed: ${error.message}`  
  );  

  advanceKey(keyIndex);  
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
console.log(Troy Gateway running on port ${PORT});
console.log(Configured API keys: ${API_KEYS.length});
console.log(
Initial active key: ${   API_KEYS.length ? API_KEYS[activeKeyIndex].slot : "NONE"   }
);
});
