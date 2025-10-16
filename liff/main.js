(function(){
  const CONFIG = window.LIFF_CONFIG || {};
  const LIFF_ID = CONFIG.liffId || "";
  const API_ROOT = CONFIG.apiRoot || "/api/liff";
  const STATUS_API = CONFIG.statusApi || "/liff/watch_status";
  const LIMIT = Math.max(1, Math.min(50, Number(CONFIG.limit) || 20));
  const PLACEHOLDER_IMAGE = CONFIG.placeholderImage ||
    "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='640' height='360'><rect width='640' height='360' fill='%23f0f1f6'/><text x='50%' y='50%' dominant-baseline='middle' text-anchor='middle' font-size='28' font-family='Arial' fill='%238896a9'>暫無圖片</text></svg>";

  const state = {
    chatId: null,
    statusMap: {},
    items: [],
    isClient: false,
    loading: false,
  };

  const elOut = document.getElementById("out");
  const elList = document.getElementById("list");
  const elReload = document.getElementById("reload");
  const elSec = document.getElementById("sec");

  function setStatus(text){
    if (elOut) elOut.textContent = text || "";
  }

  function ensureSec(v){
    const n = parseInt(v, 10);
    if (Number.isNaN(n) || n < 15) return 15;
    return n;
  }

  function canonicalUrl(url){
    try {
      const u = new URL(url);
      u.hash = "";
      u.searchParams.sort?.();
      return u.toString();
    } catch (e) {
      return url;
    }
  }

  function updateWatchState(url, data){
    const key = canonicalUrl(url);
    const entry = state.statusMap[key] || {};
    state.statusMap[key] = Object.assign({}, entry, data);
  }

  function createMetaLine(label, value){
    if (!value) return null;
    const span = document.createElement("span");
    span.textContent = `${label} ${value}`;
    return span;
  }

  function createCard(item, index){
    const card = document.createElement("article");
    card.className = "card";
    card.dataset.url = item.url || "";

    const title = document.createElement("div");
    title.className = "title";
    title.textContent = `${String(index + 1).padStart(2, "0")}. ${item.title || "活動"}`;
    card.appendChild(title);

    const img = document.createElement("img");
    img.src = item.image_url || PLACEHOLDER_IMAGE;
    img.alt = item.title || "活動圖片";
    img.onerror = () => {
      if (img.src !== PLACEHOLDER_IMAGE) {
        img.src = PLACEHOLDER_IMAGE;
      }
    };
    card.appendChild(img);

    const meta = document.createElement("div");
    meta.className = "meta";
    const dateLine = createMetaLine("📅", item.date_text || item.date || "");
    const placeLine = createMetaLine("📍", item.place || "");
    if (dateLine) meta.appendChild(dateLine);
    if (placeLine) meta.appendChild(placeLine);
    card.appendChild(meta);

    const links = document.createElement("div");
    links.className = "links";
    if (item.url){
      const anchor = document.createElement("a");
      anchor.href = item.url;
      anchor.target = "_blank";
      anchor.rel = "noopener";
      anchor.textContent = "活動頁面";
      links.appendChild(anchor);
    }
    card.appendChild(links);

    const statusText = document.createElement("div");
    statusText.className = "status-text";
    statusText.textContent = item.status_text || "暫時讀不到剩餘數（可能為動態載入）";
    card.appendChild(statusText);

    const watchInfo = document.createElement("div");
    watchInfo.className = "watch-state";
    card.appendChild(watchInfo);

    const buttons = document.createElement("div");
    buttons.className = "buttons";

    const btnWatch = document.createElement("button");
    btnWatch.className = "primary btn-watch";
    btnWatch.type = "button";
    btnWatch.textContent = "✅ 開始監看";
    btnWatch.addEventListener("click", () => handleWatch(item, card, statusText, watchInfo, feedback));
    buttons.appendChild(btnWatch);

    const btnStop = document.createElement("button");
    btnStop.className = "danger btn-stop";
    btnStop.type = "button";
    btnStop.textContent = "⏹ 停止監看";
    btnStop.addEventListener("click", () => handleUnwatch(item, card, statusText, watchInfo, feedback));
    buttons.appendChild(btnStop);

    const btnQuick = document.createElement("button");
    btnQuick.className = "secondary btn-quick";
    btnQuick.type = "button";
    btnQuick.textContent = "⚡ 快速查看";
    btnQuick.addEventListener("click", () => handleQuickCheck(item, card, statusText, feedback));
    buttons.appendChild(btnQuick);

    card.appendChild(buttons);

    const feedback = document.createElement("div");
    feedback.className = "feedback";
    card.appendChild(feedback);

    updateCardWatchInfo(item.url, watchInfo);
    return card;
  }

  function updateCardWatchInfo(url, watchInfo){
    if (!watchInfo) return;
    const key = canonicalUrl(url);
    const status = state.statusMap[key];
    if (status && status.taskId){
      watchInfo.textContent = status.enabled ? `狀態：監看中｜任務 ${status.taskId}` : `狀態：任務 ${status.taskId} 已停用`;
    } else if (status && status.found){
      watchInfo.textContent = "狀態：此活動已建立任務但目前未啟用";
    } else {
      watchInfo.textContent = "";
    }
  }

  function setCardFeedback(feedbackNode, message){
    if (!feedbackNode) return;
    feedbackNode.textContent = message || "";
  }

  function setButtonsDisabled(card, disabled){
    card.querySelectorAll("button").forEach(btn => {
      btn.disabled = disabled;
    });
  }

  async function handleWatch(item, card, statusText, watchInfo, feedback){
    if (!item.url){
      alert("此活動沒有可用的 URL，無法監看。");
      return;
    }
    if (!state.chatId){
      alert("尚未取得聊天室識別，請重新開啟 LIFF 再試一次。");
      return;
    }
    const sec = ensureSec(elSec ? elSec.value : 30);
    const payload = { chat_id: state.chatId, url: item.url, period: sec };
    const btn = card.querySelector(".btn-watch");
    const original = btn ? btn.textContent : "";
    if (btn) {
      btn.disabled = true;
      btn.textContent = "送出中…";
    }
    setCardFeedback(feedback, "送出監看請求中…");
    try {
      const res = await fetch(`${API_ROOT}/watch`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(payload),
      });
      const body = await res.json();
      if (!res.ok || !body.ok){
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      updateWatchState(item.url, { watching: true, enabled: true, taskId: body.task_id, found: true });
      updateCardWatchInfo(item.url, watchInfo);
      setCardFeedback(feedback, body.message || "已開始監看。");
      if (body.detail && body.detail.status_text){
        statusText.textContent = body.detail.status_text;
      }
    } catch (err) {
      console.error("watch failed", err);
      alert(`開始監看失敗：${err.message || err}`);
      setCardFeedback(feedback, "開始監看失敗，請稍後再試。");
    } finally {
      if (btn){
        btn.disabled = false;
        btn.textContent = original || "✅ 開始監看";
      }
    }
  }

  async function handleUnwatch(item, card, statusText, watchInfo, feedback){
    if (!item.url){
      alert("此活動沒有可用的 URL。");
      return;
    }
    if (!state.chatId){
      alert("尚未取得聊天室識別，請重新開啟 LIFF 再試一次。");
      return;
    }
    const key = canonicalUrl(item.url);
    const current = state.statusMap[key] || {};
    if (!current.taskId && !current.enabled){
      alert("此活動無監看");
      return;
    }
    const payload = { chat_id: state.chatId, url: item.url };
    if (current.taskId){
      payload.task_code = current.taskId;
    }
    const btn = card.querySelector(".btn-stop");
    const original = btn ? btn.textContent : "";
    if (btn){
      btn.disabled = true;
      btn.textContent = "送出中…";
    }
    setCardFeedback(feedback, "送出停止監看請求中…");
    try {
      const res = await fetch(`${API_ROOT}/unwatch`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(payload),
      });
      const body = await res.json();
      if (!res.ok){
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      if (body.ok){
        updateWatchState(item.url, { watching: false, enabled: false, taskId: body.task_id || current.taskId, found: true });
      }
      updateCardWatchInfo(item.url, watchInfo);
      setCardFeedback(feedback, body.message || "已送出停止監看。" );
    } catch (err) {
      console.error("unwatch failed", err);
      alert(`停止監看失敗：${err.message || err}`);
      setCardFeedback(feedback, "停止監看失敗，請稍後再試。");
    } finally {
      if (btn){
        btn.disabled = false;
        btn.textContent = original || "⏹ 停止監看";
      }
    }
  }

  async function handleQuickCheck(item, card, statusText, feedback){
    if (!item.url){
      alert("此活動沒有可用的 URL。");
      return;
    }
    setButtonsDisabled(card, true);
    setCardFeedback(feedback, "快速查看中…");
    try {
      const res = await fetch(`${API_ROOT}/quick-check`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ url: item.url }),
      });
      const body = await res.json();
      if (!res.ok || !body.ok){
        throw new Error(body.error || `HTTP ${res.status}`);
      }
      if (body.detail && body.detail.status_text){
        statusText.textContent = body.detail.status_text;
      }
      setCardFeedback(feedback, "已取得最新票數資訊，請查看訊息。");
      if (body.message){
        if (state.isClient){
          try {
            await liff.sendMessages([{ type: "text", text: body.message }]);
          } catch (e) {
            console.warn("sendMessages failed", e);
          }
        }
        alert(body.message);
      }
    } catch (err) {
      console.error("quick-check failed", err);
      alert(`快速查看失敗：${err.message || err}`);
      setCardFeedback(feedback, "快速查看失敗，請稍後再試。");
    } finally {
      setButtonsDisabled(card, false);
    }
  }

  async function fetchConcerts(mode){
    const url = new URL(`${API_ROOT}/concerts`, window.location.origin);
    url.searchParams.set("mode", mode);
    url.searchParams.set("limit", String(LIMIT));
    const res = await fetch(url.toString(), { credentials: "include" });
    if (!res.ok){
      const text = await res.text();
      throw new Error(`HTTP ${res.status} ${text}`);
    }
    return res.json();
  }

  async function fetchWatchStatus(chatId, urls){
    if (!chatId || !Array.isArray(urls) || urls.length === 0) return {};
    const unique = [];
    const seen = new Set();
    urls.forEach((raw) => {
      if (typeof raw !== "string") return;
      const val = raw.trim();
      if (!val || seen.has(val)) return;
      seen.add(val);
      unique.push(val);
    });
    if (!unique.length) return {};
    const res = await fetch(STATUS_API, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ chatId, urls: unique }),
    });
    if (!res.ok){
      throw new Error(`HTTP ${res.status}`);
    }
    const body = await res.json();
    return body && body.results ? body.results : {};
  }

  function render(items){
    if (!elList) return;
    elList.innerHTML = "";
    if (!Array.isArray(items) || items.length === 0){
      const empty = document.createElement("div");
      empty.className = "empty-message";
      empty.textContent = "目前抓不到活動清單，請稍後再試。";
      elList.appendChild(empty);
      return;
    }
    items.forEach((item, idx) => {
      const card = createCard(item, idx);
      elList.appendChild(card);
    });
  }

  async function loadConcerts(){
    if (state.loading) return;
    state.loading = true;
    setStatus("載入活動清單中…");
    try {
      let data = await fetchConcerts("carousel");
      let sourceMode = data && data.source_mode ? data.source_mode : "carousel";
      if (!data || !Array.isArray(data.results) || data.results.length === 0){
        const fallback = await fetchConcerts("all");
        if (fallback && Array.isArray(fallback.results) && fallback.results.length){
          data = fallback;
          sourceMode = fallback.source_mode || "all";
        }
      }
      const items = (data && Array.isArray(data.results)) ? data.results : [];
      state.items = items;
      const urls = items.map(it => it.url).filter(Boolean);
      try {
        const statusMapRaw = await fetchWatchStatus(state.chatId, urls);
        state.statusMap = {};
        Object.keys(statusMapRaw).forEach((key) => {
          const entry = statusMapRaw[key];
          state.statusMap[canonicalUrl(key)] = entry;
        });
      } catch (e){
        console.warn("fetchWatchStatus failed", e);
      }
      render(items);
      setStatus(items.length ? `共 ${items.length} 筆活動${sourceMode === "all" ? "（使用備援資料）" : ""}` : "目前抓不到活動清單，請稍後再試。");
    } catch (err) {
      console.error("loadConcerts failed", err);
      setStatus(`載入失敗：${err.message || err}`);
      elList.innerHTML = "";
      const empty = document.createElement("div");
      empty.className = "empty-message";
      empty.textContent = "目前抓不到活動清單，請稍後再試。";
      elList.appendChild(empty);
    } finally {
      state.loading = false;
    }
  }

  async function resolveChatId(){
    if (!window.liff) return null;
    if (!state.isClient) {
      try {
        const profile = await liff.getProfile();
        if (profile && profile.userId) return profile.userId;
      } catch (e) {
        console.warn("getProfile failed", e);
      }
      return null;
    }
    try {
      const ctx = liff.getContext();
      if (ctx){
        if (ctx.type === "group" && ctx.groupId) return ctx.groupId;
        if (ctx.type === "room" && ctx.roomId) return ctx.roomId;
        if (ctx.userId) return ctx.userId;
      }
    } catch (e) {
      console.warn("getContext failed", e);
    }
    try {
      const profile = await liff.getProfile();
      if (profile && profile.userId) return profile.userId;
    } catch (e) {
      console.warn("getProfile failed", e);
    }
    return null;
  }

  async function init(){
    if (!window.liff){
      setStatus("找不到 LIFF SDK，請稍後再試。");
      return;
    }
    setStatus("初始化 LIFF 中…");
    try {
      if (LIFF_ID){
        await liff.init({ liffId: LIFF_ID });
      } else {
        await liff.init({});
      }
      state.isClient = liff.isInClient();
    } catch (err) {
      console.error("liff.init failed", err);
      setStatus("LIFF 初始化失敗，請稍後再試。");
      return;
    }

    try {
      state.chatId = await resolveChatId();
    } catch (e){
      console.warn("resolveChatId failed", e);
    }

    if (elReload){
      elReload.addEventListener("click", () => {
        loadConcerts();
      });
    }

    loadConcerts();
  }

  document.addEventListener("DOMContentLoaded", init);
})();
