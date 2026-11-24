const http = require("http");

const PORT = process.env.PORT || 3000;
const UPSTREAM = "https://100milionseek.top/v1/chat/completions";

const API_KEYS = [
  process.env.API_KEY_1,
  process.env.API_KEY_2,
  process.env.API_KEY_3
].filter(Boolean);

const APP_KEY = process.env.TROY_APP_KEY;

let currentIndex = 0;
const exhausted = new Set();

const server = http.createServer(async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");
  res.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("ok");
  }

  if (req.method !== "POST" || req.url !== "/v1/chat/completions") {
    res.writeHead(404);
    return res.end("Not found");
  }

  if (!APP_KEY || req.headers.authorization !== `Bearer ${APP_KEY}`) {
    res.writeHead(401);
    return res.end(JSON.stringify({ error: "Invalid App Key" }));
  }

  if (!API_KEYS.length) {
    res.writeHead(500);
    return res.end(JSON.stringify({ error: "No API keys configured" }));
  }

  let body = "";
  req.on("data", chunk => {
    body += chunk;
    if (body.length > 2 * 1024 * 1024) {
      req.destroy();
    }
  });

  req.on("end", async () => {
    let data;

    try {
      data = JSON.parse(body);
    } catch {
      res.writeHead(400);
      return res.end(JSON.stringify({ error: "Invalid JSON" }));
    }

    const tried = new Set();

    for (let attempt = 0; attempt < API_KEYS.length; attempt++) {
      let index = (currentIndex + attempt) % API_KEYS.length;

      if (exhausted.has(index) || tried.has(index)) continue;
      tried.add(index);

      try {
        const upstream = await fetch(UPSTREAM, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${API_KEYS[index]}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(data),
          signal: AbortSignal.timeout(120000)
        });

        if (upstream.status === 429) {
          const errorText = await upstream.text();

          let isQuota = /quota|daily limit|insufficient quota|credits exhausted/i.test(errorText);

          if (isQuota) {
            exhausted.add(index);
            currentIndex = (index + 1) % API_KEYS.length;
            continue;
          }

          res.writeHead(429, { "Content-Type": "application/json" });
          return res.end(errorText);
        }

        if (!upstream.ok) {
          const errorText = await upstream.text();
          res.writeHead(upstream.status, {
            "Content-Type": "application/json"
          });
          return res.end(errorText);
        }

        currentIndex = index;

        res.writeHead(200, {
          "Content-Type": upstream.headers.get("content-type") || "application/json",
          "Cache-Control": "no-cache",
          "Connection": "keep-alive"
        });

        if (upstream.body) {
          for await (const chunk of upstream.body) {
            if (res.destroyed) break;
            res.write(chunk);
          }
        }

        return res.end();

      } catch (error) {
        res.writeHead(502, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({
          error: "Upstream connection failed"
        }));
      }
    }

    res.writeHead(429, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "All available API keys exhausted" }));
  });
});

server.listen(PORT, () => {
  console.log(`TroySeek Gateway running on port ${PORT}`);
});
