require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { Readable } = require("stream");

const app = express();

app.use(cors());
app.use(express.static("public"));

const IPTV_HOST = process.env.IPTV_HOST;
const IPTV_USERNAME = process.env.IPTV_USERNAME;
const IPTV_PASSWORD = process.env.IPTV_PASSWORD;
const PORT = process.env.PORT || 3000;
const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

function buildApiUrl(action = "", extraParams = {}) {
  const url = new URL(`${IPTV_HOST}/player_api.php`);
  url.searchParams.set("username", IPTV_USERNAME);
  url.searchParams.set("password", IPTV_PASSWORD);
  if (action) {
    url.searchParams.set("action", action);
  }
  Object.entries(extraParams).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  });
  return url.toString();
}

function buildStreamUrl(streamId, extension = "m3u8") {
  return `${IPTV_HOST}/live/${encodeURIComponent(IPTV_USERNAME)}/${encodeURIComponent(IPTV_PASSWORD)}/${streamId}.${extension}`;
}

function buildVodUrl(streamId, extension = "mp4") {
  return `${IPTV_HOST}/movie/${encodeURIComponent(IPTV_USERNAME)}/${encodeURIComponent(IPTV_PASSWORD)}/${streamId}.${extension}`;
}

function buildSeriesUrl(episodeId, extension = "mp4", route = "series") {
  return `${IPTV_HOST}/${route}/${encodeURIComponent(IPTV_USERNAME)}/${encodeURIComponent(IPTV_PASSWORD)}/${episodeId}.${extension}`;
}

async function fetchXtream(url) {
  const referer = (() => {
    try {
      const parsed = new URL(url);
      return `${parsed.origin}/`;
    } catch {
      return `${IPTV_HOST}/`;
    }
  })();

  return fetch(url, {
    headers: {
      "User-Agent": DEFAULT_USER_AGENT,
      Accept: "*/*",
      Referer: referer
    }
  });
}

function toAbsoluteUrl(baseUrl, value) {
  try {
    return new URL(value, baseUrl).toString();
  } catch {
    return value;
  }
}

function rewriteM3u8ToLocalProxy(m3u8Content, baseUrl) {
  return m3u8Content
    .split("\n")
    .map((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) {
        return line;
      }
      const absolute = toAbsoluteUrl(baseUrl, trimmed);
      return `/api/proxy?u=${encodeURIComponent(absolute)}`;
    })
    .join("\n");
}

function isAllowedProxyTarget(targetUrl) {
  const parsed = new URL(targetUrl);
  if (!["http:", "https:"].includes(parsed.protocol)) {
    return false;
  }

  const hostname = parsed.hostname.toLowerCase();
  const baseHostname = new URL(IPTV_HOST).hostname.toLowerCase();
  const allowedSuffixes = [baseHostname, "ftvpro.net", "futuretv.pro"];

  return allowedSuffixes.some(
    (suffix) => hostname === suffix || hostname.endsWith(`.${suffix}`)
  );
}

function copyHeaders(source, res, extra = {}) {
  const contentType = source.headers.get("content-type");
  if (contentType) {
    res.setHeader("Content-Type", contentType);
  }
  const cacheControl = source.headers.get("cache-control");
  if (cacheControl) {
    res.setHeader("Cache-Control", cacheControl);
  }
  Object.entries(extra).forEach(([k, v]) => res.setHeader(k, v));
}

function streamFetchResponseToClient(response, res) {
  const method = (res.req?.method || "GET").toUpperCase();

  if (method === "HEAD") {
    return res.status(response.status || 200).end();
  }

  if (!response.body) {
    return res.status(502).json({ error: "Respuesta sin body en stream remoto" });
  }

  const nodeStream = Readable.fromWeb(response.body);

  nodeStream.on("error", (error) => {
    const message = String(error?.message || error || "");
    const isSocketTermination =
      error?.code === "UND_ERR_SOCKET" ||
      message.toLowerCase().includes("terminated") ||
      message.toLowerCase().includes("aborted");

    if (isSocketTermination) {
      console.warn("Stream remoto terminó prematuramente");
    } else {
      console.error("Error de stream remoto:", error);
    }

    if (!res.headersSent) {
      res.status(502).json({ error: "Se interrumpió el stream remoto" });
      return;
    }

    if (!res.writableEnded) {
      res.end();
    }
  });

  res.on("close", () => {
    if (!nodeStream.destroyed) {
      nodeStream.destroy();
    }
  });

  nodeStream.pipe(res);
}

async function pipeFetchToResponse(targetUrl, res) {
  const response = await fetchXtream(targetUrl);
  if (!response.ok || !response.body) {
    const body = await response.text().catch(() => "");
    return res.status(response.status || 502).json({
      error: "No se pudo cargar el stream",
      detail: body?.slice(0, 300) || null
    });
  }

  copyHeaders(response, res);
  return streamFetchResponseToClient(response, res);
}

app.get("/api/account", async (req, res) => {
  try {
    const response = await fetchXtream(buildApiUrl());
    const data = await response.json();
    res.json({
      user_info: {
        username: data?.user_info?.username,
        status: data?.user_info?.status,
        exp_date: data?.user_info?.exp_date,
        max_connections: data?.user_info?.max_connections,
        active_cons: data?.user_info?.active_cons,
        allowed_output_formats: data?.user_info?.allowed_output_formats
      },
      server_info: data?.server_info
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "No se pudo obtener información de la cuenta" });
  }
});

app.get("/api/categories", async (req, res) => {
  try {
    const response = await fetchXtream(buildApiUrl("get_live_categories"));
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "No se pudieron obtener categorías" });
  }
});

app.get("/api/channels", async (req, res) => {
  try {
    const response = await fetchXtream(buildApiUrl("get_live_streams"));
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "No se pudieron obtener canales" });
  }
});

app.get("/api/vod/categories", async (req, res) => {
  try {
    const response = await fetchXtream(buildApiUrl("get_vod_categories"));
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "No se pudieron obtener categorías VOD" });
  }
});

app.get("/api/vod/streams", async (req, res) => {
  try {
    const response = await fetchXtream(buildApiUrl("get_vod_streams"));
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "No se pudieron obtener películas" });
  }
});

app.get("/api/series/categories", async (req, res) => {
  try {
    const response = await fetchXtream(buildApiUrl("get_series_categories"));
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "No se pudieron obtener categorías de series" });
  }
});

app.get("/api/series", async (req, res) => {
  try {
    const response = await fetchXtream(buildApiUrl("get_series"));
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "No se pudieron obtener series" });
  }
});

app.get("/api/series/:seriesId/episodes", async (req, res) => {
  try {
    const { seriesId } = req.params;
    const response = await fetchXtream(
      buildApiUrl("get_series_info", { series_id: seriesId })
    );
    const data = await response.json();

    const episodesBySeason = data?.episodes && typeof data.episodes === "object"
      ? data.episodes
      : {};

    const episodes = Object.entries(episodesBySeason)
      .flatMap(([seasonKey, seasonEpisodes]) => {
        if (!Array.isArray(seasonEpisodes)) {
          return [];
        }
        return seasonEpisodes.map((episode) => ({
          ...episode,
          season: Number(episode?.season ?? seasonKey),
          episode_num: Number(episode?.episode_num ?? 0)
        }));
      })
      .sort((a, b) => {
        const seasonDiff = Number(a.season || 0) - Number(b.season || 0);
        if (seasonDiff !== 0) return seasonDiff;
        return Number(a.episode_num || 0) - Number(b.episode_num || 0);
      });

    res.json({
      info: data?.info || null,
      seasons: Array.isArray(data?.seasons) ? data.seasons : [],
      episodes
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "No se pudieron obtener episodios de la serie" });
  }
});

app.get("/api/stream/:streamId", (req, res) => {
  const { streamId } = req.params;
  const extension = req.query.extension || "m3u8";
  res.json({
    stream_id: streamId,
    url: `/api/play/${streamId}.${extension}`
  });
});

app.get("/api/vod/stream/:streamId", (req, res) => {
  const { streamId } = req.params;
  const extension = req.query.extension || "mp4";
  res.json({
    stream_id: streamId,
    url: `/api/play/vod/${streamId}.${extension}`
  });
});

app.get("/api/series/stream/:episodeId", (req, res) => {
  const { episodeId } = req.params;
  const extension = req.query.extension || "mp4";
  res.json({
    episode_id: episodeId,
    url: `/api/play/series/${episodeId}.${extension}`
  });
});

app.get("/live/:username/:password/:streamId.:extension", (req, res) => {
  const { streamId, extension } = req.params;
  const host = `${req.protocol}://${req.get("host")}`;
  const target = `${host}/api/play/${streamId}.${extension || "m3u8"}`;
  return res.redirect(302, target);
});

app.get("/movie/:username/:password/:streamId.:extension", (req, res) => {
  const { streamId, extension } = req.params;
  const host = `${req.protocol}://${req.get("host")}`;
  const target = `${host}/api/play/vod/${streamId}.${extension || "mp4"}`;
  return res.redirect(302, target);
});

app.get("/series/:username/:password/:episodeId.:extension", (req, res) => {
  const { episodeId, extension } = req.params;
  const host = `${req.protocol}://${req.get("host")}`;
  const target = `${host}/api/play/series/${episodeId}.${extension || "mp4"}`;
  return res.redirect(302, target);
});

app.get("/api/play/:streamId.:extension", async (req, res) => {
  try {
    const { streamId, extension } = req.params;
    const ext = extension || "m3u8";
    const remoteUrl = buildStreamUrl(streamId, ext);

    if (ext === "m3u8") {
      const response = await fetchXtream(remoteUrl);
      if (!response.ok) {
        const body = await response.text().catch(() => "");
        return res.status(response.status || 502).json({
          error: "No se pudo obtener el playlist m3u8",
          detail: body?.slice(0, 300) || null
        });
      }

      const playlist = await response.text();
      const playlistBaseUrl = response.url || remoteUrl;
      const rewritten = rewriteM3u8ToLocalProxy(playlist, playlistBaseUrl);
      copyHeaders(response, res, {
        "Content-Type": "application/vnd.apple.mpegurl",
        "Cache-Control": "no-store"
      });
      return res.send(rewritten);
    }

    return pipeFetchToResponse(remoteUrl, res);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Error al preparar el stream" });
  }
});

app.get("/api/play/vod/:streamId.:extension", async (req, res) => {
  try {
    const { streamId, extension } = req.params;
    const ext = extension || "mp4";
    const remoteUrl = buildVodUrl(streamId, ext);
    return pipeFetchToResponse(remoteUrl, res);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Error al preparar película" });
  }
});

app.get("/api/play/series/:episodeId.:extension", async (req, res) => {
  try {
    const { episodeId, extension } = req.params;
    const ext = extension || "mp4";

    const candidates = [
      buildSeriesUrl(episodeId, ext, "series"),
      buildSeriesUrl(episodeId, ext, "movie")
    ];

    let lastStatus = 500;
    let lastDetail = null;

    for (const candidate of candidates) {
      const response = await fetchXtream(candidate);
      if (response.ok && response.body) {
        copyHeaders(response, res);
        return streamFetchResponseToClient(response, res);
      }

      lastStatus = response.status || 500;
      lastDetail = await response.text().catch(() => null);
    }

    return res.status(lastStatus).json({
      error: "No se pudo cargar episodio",
      detail: lastDetail?.slice(0, 300) || null
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Error al preparar episodio" });
  }
});

app.get("/api/proxy", async (req, res) => {
  try {
    const target = req.query.u;
    if (!target) {
      return res.status(400).json({ error: "Parámetro u requerido" });
    }

    const parsed = new URL(target);
    if (!isAllowedProxyTarget(parsed.toString())) {
      return res.status(400).json({ error: "Host no permitido" });
    }

    return pipeFetchToResponse(parsed.toString(), res);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Error en proxy de stream" });
  }
});

app.listen(PORT, () => {
  console.log(`IPTV web corriendo en http://localhost:${PORT}`);
});