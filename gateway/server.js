async function sendUpstream(req, res, body) {
  if (!API_KEYS.length) {
    return sendJson(res, 500, { error: { message: "No upstream API keys configured" } });
  }

  let lastError = null;

  for (let attempt = 0; attempt < API_KEYS.length; attempt++) {
    if (res.destroyed || res.writableEnded) return;

    const index = activeKeyIndex;
    const item = API_KEYS[index];
    const controller = new AbortController();

    const timeout = setTimeout(() => controller.abort(), UPSTREAM_TIMEOUT);

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

      // --- LOGIC PARA SA STREAMING FALLBACK ---
      // 1. Kunin muna ang unang chunk. Kung mag-fail dito, pwede pa tayong mag-fallback.
      const firstChunk = await reader.read();
      
      if (firstChunk.done) {
        throw new Error("Stream ended immediately");
      }

      // Check kung busy response bago mag-send ng headers
      const firstChunkStr = Buffer.from(firstChunk.value).toString("utf8");
      if (isBusyResponse(firstChunkStr)) {
        throw new Error("Upstream busy during initial stream");
      }

      // 2. Kung nakalampas sa check, ibig sabihin valid na yung stream. I-setup na natin ang client.
      activeKeyIndex = index;
      console.log(`API key slot ${item.slot} succeeded. Active key retained.`);

      res.writeHead(upstream.status, {
        "Content-Type": contentType,
        "Cache-Control": "no-cache, no-transform",
        "X-Accel-Buffering": "no",
        "Connection": "keep-alive"
      });

      // Isulat ang unang chunk na nakuha natin
      res.write(Buffer.from(firstChunk.value));

      // 3. Ituloy ang natitirang stream
      try {
        while (true) {
          const { done, value } = await reader.read();

          if (done) break;

          if (res.destroyed || res.writableEnded) {
            await reader.cancel();
            return;
          }

          const chunk = Buffer.from(value);

          // Dito, kapag nag-fail sa gitna, hindi na tayo makakapag-fallback nang maayos
          // dahil naipadala na natin ang headers sa client. Kaya i-destroy na lang.
          if (isBusyResponse(chunk.toString("utf8"))) {
            throw new Error("Upstream busy during streaming");
          }

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

        if (!res.destroyed && !res.writableEnded) {
          res.end();
        }

        console.log(`API key slot ${item.slot} stream completed.`);
        return; // Tapos na ang stream

      } catch (streamError) {
        // Dito nahuhuli ang "operation aborted" sa gitna ng stream
        console.error(`API key slot ${item.slot} failed mid-stream: ${streamError.message}`);
        advanceKey(index);
        if (!res.destroyed && !res.writableEnded) {
            res.destroy(streamError); // Force close connection sa client
        }
        return; // Cannot fallback anymore
      }

    } catch (error) {
      // Dito nahuhuli yung mga error BAGO mag-umpisa ang stream (e.g., 429, or failed first chunk)
      lastError = error;
      console.error(`API key slot ${item.slot} failed: ${error.message}`);
      advanceKey(index);
      
      // Ituloy ang loop para subukan ang susunod na key
      
    } finally {
      clearTimeout(timeout);
    }
  }

  sendJson(res, 502, {
    error: {
      message: "All upstream API keys failed",
      details: lastError ? lastError.message : "Unknown error"
    }
  });
      }
