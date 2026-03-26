(function () {
  var dynamicRoot = document.getElementById("milhao-dashboard-dynamic");
  if (!dynamicRoot) {
    return;
  }

  var cardsDataUrl = dynamicRoot.getAttribute("data-cards-url") || "";
  if (!cardsDataUrl) {
    return;
  }

  var exportModal = document.getElementById("milhao-export-modal");
  var exportForm = document.getElementById("milhao-export-form");
  var exportStartInput = document.getElementById("milhao-export-start");
  var exportEndInput = document.getElementById("milhao-export-end");
  var exportSubmitBtn = document.getElementById("milhao-export-submit");
  var exportErrorBox = document.getElementById("milhao-export-error");
  var exportUrl = exportModal ? (exportModal.getAttribute("data-export-url") || "") : "";
  var exportMaxDays = exportModal ? parseInt(exportModal.getAttribute("data-max-days") || "93", 10) : 93;
  if (!isFinite(exportMaxDays) || exportMaxDays <= 0) {
    exportMaxDays = 93;
  }

  var rtStatus = document.getElementById("milhao-rt-status");
  var muralIntroTargetDetails = null;
  var rtIntervalMs = 15000;
  var muralPollIntervalMs = 6000;
  var rtTimer = null;
  var muralPollTimer = null;
  var isNavigating = false;
  var isExporting = false;
  var isSubmittingMural = false;
  var dateInput = null;
  var availableDates = [];
  var shimmerCleanupTimers = new WeakMap();
  var shimmerLastRunAt = new WeakMap();
  var shimmerRafHandles = new WeakMap();
  var arrowTimers = new WeakMap();
  var rtDebug = false;

  try {
    var debugFromUrl = new URLSearchParams(window.location.search).get("rtdebug");
    var debugFromStorage = window.localStorage ? window.localStorage.getItem("bla_rt_debug") : "";
    rtDebug = debugFromUrl === "1" || debugFromStorage === "1";
  } catch (error) {
    rtDebug = false;
  }

  function debugLog(message, data) {
    if (!rtDebug || !window.console) {
      return;
    }
    if (typeof data === "undefined") {
      console.log("[BLA-RT]", message);
      return;
    }
    console.log("[BLA-RT]", message, data);
  }

  function hydrateElements() {
    dynamicRoot = document.getElementById("milhao-dashboard-dynamic");
    dateInput = dynamicRoot ? dynamicRoot.querySelector("#milhao-date-input") : null;
    rtStatus = document.getElementById("milhao-rt-status");
    cardsDataUrl = dynamicRoot ? (dynamicRoot.getAttribute("data-cards-url") || cardsDataUrl) : cardsDataUrl;
    availableDates = dateInput
      ? (dateInput.getAttribute("data-available-dates") || "")
          .split(",")
          .map(function (item) { return item.trim(); })
          .filter(Boolean)
      : [];
    syncExportDateBounds();
  }

  function buildFormUrl(targetForm) {
    var nextUrl = new URL(window.location.href);
    nextUrl.search = "";
    var params = new URLSearchParams(new FormData(targetForm));
    nextUrl.search = params.toString();
    return nextUrl.toString();
  }

  function navigatePartial(nextUrl, pushState) {
    if (isNavigating) {
      return;
    }
    isNavigating = true;
    dynamicRoot.classList.add("is-loading");
    fetch(nextUrl, { headers: { "X-Requested-With": "XMLHttpRequest" } })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("bad_response");
        }
        return response.text();
      })
      .then(function (html) {
        var doc = new DOMParser().parseFromString(html, "text/html");
        var nextDynamic = doc.getElementById("milhao-dashboard-dynamic");
        if (!nextDynamic) {
          throw new Error("missing_dynamic");
        }
        dynamicRoot.replaceWith(nextDynamic);
        if (pushState) {
          window.history.pushState({}, "", nextUrl);
        }
        hydrateElements();
        bindDynamicHandlers();
        refreshCardsRealtime();
      })
      .catch(function () {
        window.location.href = nextUrl;
      })
      .finally(function () {
        isNavigating = false;
        if (dynamicRoot) {
          dynamicRoot.classList.remove("is-loading");
        }
      });
  }

  function clearShimmerState(host) {
    if (!host) {
      return;
    }
    host.classList.remove("rt-shimmer");
    host.classList.remove("rt-shimmer-active");
    var layer = null;
    Array.prototype.some.call(host.children || [], function (child) {
      if (child && child.classList && child.classList.contains("rt-shimmer-layer")) {
        layer = child;
        return true;
      }
      return false;
    });
    if (layer) {
      layer.classList.remove("is-running");
      layer.style.opacity = "";
      layer.style.transform = "";
    }
    var timerId = shimmerCleanupTimers.get(host);
    if (timerId) {
      clearTimeout(timerId);
      shimmerCleanupTimers.delete(host);
    }
    var rafHandle = shimmerRafHandles.get(host);
    if (rafHandle) {
      cancelAnimationFrame(rafHandle);
      shimmerRafHandles.delete(host);
    }
    host.style.removeProperty("box-shadow");
  }

  function ensureShimmerLayer(host) {
    if (!host) {
      return null;
    }
    var layer = null;
    Array.prototype.some.call(host.children || [], function (child) {
      if (child && child.classList && child.classList.contains("rt-shimmer-layer")) {
        layer = child;
        return true;
      }
      return false;
    });
    if (layer) {
      return layer;
    }
    layer = document.createElement("span");
    layer.className = "rt-shimmer-layer";
    layer.setAttribute("aria-hidden", "true");
    host.appendChild(layer);
    return layer;
  }

  function resolveShimmerHost(el) {
    if (!el) {
      return null;
    }
    return (
      el.closest("[data-rt-shimmer-host]") ||
      el.closest(".panel-card") ||
      el.closest(".metrics-card") ||
      el.closest(".composition-card") ||
      el.closest(".card")
    );
  }

  function runShimmer(host) {
    if (!host) {
      debugLog("shimmer skipped: no host");
      return;
    }
    var now = Date.now();
    var lastRun = shimmerLastRunAt.get(host) || 0;
    if (now - lastRun < 90) {
      debugLog("shimmer throttled", host);
      return;
    }
    shimmerLastRunAt.set(host, now);
    host.classList.add("rt-shimmer-host");
    var layer = ensureShimmerLayer(host);
    clearShimmerState(host);
    host.classList.add("rt-shimmer");
    host.classList.add("rt-shimmer-active");
    if (!layer) {
      debugLog("shimmer layer missing", host);
      return;
    }
    layer.classList.add("is-running");
    debugLog("shimmer start", host);

    var duration = 620;
    var startTs = null;
    function step(ts) {
      if (startTs === null) {
        startTs = ts;
      }
      var progress = Math.min(1, (ts - startTs) / duration);
      var translate = -128 + (256 * progress);
      var opacity = 0;
      if (progress < 0.16) {
        opacity = (progress / 0.16) * 0.6;
      } else if (progress < 0.56) {
        opacity = 0.6 + ((progress - 0.16) / 0.4) * 0.4;
      } else {
        opacity = ((1 - progress) / 0.44) * 0.98;
      }
      if (opacity < 0) {
        opacity = 0;
      }
      if (opacity > 1) {
        opacity = 1;
      }

      layer.style.transform = "translate3d(" + translate.toFixed(2) + "%, 0, 0) skewX(-16deg)";
      layer.style.opacity = opacity.toFixed(3);

      var glow = progress <= 0.5 ? (progress / 0.5) : ((1 - progress) / 0.5);
      if (glow < 0) {
        glow = 0;
      }
      host.style.boxShadow =
        "0 0 0 1px rgba(255,156,83," + (0.28 * glow).toFixed(3) + "), 0 10px 28px rgba(255,156,83," + (0.18 * glow).toFixed(3) + ")";

      if (progress >= 1) {
        debugLog("shimmer end", host);
        clearShimmerState(host);
        return;
      }
      var nextHandle = requestAnimationFrame(step);
      shimmerRafHandles.set(host, nextHandle);
    }

    var firstHandle = requestAnimationFrame(step);
    shimmerRafHandles.set(host, firstHandle);

    var fallbackTimer = setTimeout(function () {
      clearShimmerState(host);
    }, 1000);
    shimmerCleanupTimers.set(host, fallbackTimer);
  }

  function pulseElement(el) {
    if (!el) {
      return;
    }
    el.classList.remove("rt-pop-value");
    void el.offsetWidth;
    el.classList.add("rt-pop-value");
    runShimmer(resolveShimmerHost(el));
  }

  function flashBalanceArrow(balance) {
    if (!balance) {
      return;
    }
    var arrow = document.querySelector('[data-balance-arrow="' + balance + '"]');
    if (!arrow) {
      return;
    }
    arrow.classList.add("is-active");
    var prevTimer = arrowTimers.get(arrow);
    if (prevTimer) {
      clearTimeout(prevTimer);
    }
    var timer = setTimeout(function () {
      arrow.classList.remove("is-active");
      arrowTimers.delete(arrow);
    }, 5000);
    arrowTimers.set(arrow, timer);
  }

  function setTextIfChanged(el, nextText) {
    if (!el) {
      return false;
    }
    var normalized = (nextText || "").trim();
    if (el.textContent.trim() === normalized) {
      return false;
    }
    el.textContent = normalized;
    pulseElement(el);
    return true;
  }

  function getFilters() {
    var dateValue = dateInput ? (dateInput.value || "") : "";
    return {
      date: dateValue,
    };
  }

  function resolveClosestDate(value) {
    if (!availableDates.length || !value) {
      return value;
    }
    if (availableDates.indexOf(value) >= 0) {
      return value;
    }
    var selectedTs = new Date(value + "T00:00:00").getTime();
    var best = availableDates[0];
    var bestDiff = Math.abs(new Date(best + "T00:00:00").getTime() - selectedTs);
    availableDates.forEach(function (candidate) {
      var diff = Math.abs(new Date(candidate + "T00:00:00").getTime() - selectedTs);
      if (diff < bestDiff) {
        best = candidate;
        bestDiff = diff;
      }
    });
    return best;
  }

  function openDatePicker(input) {
    if (!input) {
      return;
    }
    input.focus({ preventScroll: true });
    if (typeof input.showPicker === "function") {
      try {
        input.showPicker();
      } catch (error) {
      }
    }
  }

  function applyRealtimeData(payload) {
    if (!payload || !payload.ok) {
      return;
    }
    if (setTextIfChanged(document.getElementById("metric-total-number"), payload.total_value_display || "0")) {
      flashBalanceArrow("LIMBL01");
    }

    var totalsMap = {};
    (payload.totals_by_balance || []).forEach(function (item) {
      totalsMap[item.balance] = item;
    });
    document.querySelectorAll("[data-balance-total-number]").forEach(function (node) {
      var balance = node.getAttribute("data-balance-total-number");
      var item = totalsMap[balance];
      if (setTextIfChanged(node, item ? (item.total_display || "0") : "0")) {
        flashBalanceArrow(balance);
      }
    });

    var compositionMap = {};
    (payload.composition || []).forEach(function (item) {
      compositionMap[item.balance] = item;
    });
    document.querySelectorAll("[data-comp-segment]").forEach(function (seg) {
      var balance = seg.getAttribute("data-comp-segment");
      var item = compositionMap[balance];
      var nextPercent = parseFloat(item ? (item.percent_str || "0.0") : "0.0");
      if (!isFinite(nextPercent)) {
        nextPercent = 0;
      }
      var currentPercent = parseFloat(seg.getAttribute("data-current-percent") || seg.style.flexBasis || "0");
      if (!isFinite(currentPercent)) {
        currentPercent = 0;
      }
      if (Math.abs(currentPercent - nextPercent) > 0.05) {
        var nextFlex = nextPercent.toFixed(1) + "%";
        seg.style.flexBasis = nextFlex;
        seg.setAttribute("data-current-percent", nextPercent.toFixed(1));
        pulseElement(seg);
      }
    });
    document.querySelectorAll("[data-comp-value]").forEach(function (label) {
      var balance = label.getAttribute("data-comp-value");
      var item = compositionMap[balance];
      setTextIfChanged(label, (item ? item.percent_str : "0.0") + "%");
    });

    var ingestMap = {};
    (payload.last_ingests || []).forEach(function (item) {
      ingestMap[item.balance] = item;
    });
    document.querySelectorAll("[data-last-ingest]").forEach(function (tag) {
      var balance = tag.getAttribute("data-last-ingest");
      var item = ingestMap[balance];
      if (!item) {
        return;
      }
      setTextIfChanged(tag, "Ultima Leitura " + item.label + ": " + item.time);
    });

    if (rtStatus) {
      rtStatus.textContent = "Atualizado " + (payload.updated_at || "--:--:--");
    }
  }

  function refreshCardsRealtime() {
    var filters = getFilters();
    var params = new URLSearchParams();
    if (filters.date) {
      params.set("date", filters.date);
    }
    fetch(cardsDataUrl + "?" + params.toString(), { headers: { "X-Requested-With": "XMLHttpRequest" } })
      .then(function (response) { return response.json(); })
      .then(function (payload) {
        applyRealtimeData(payload);
      })
      .catch(function () {
        if (rtStatus) {
          rtStatus.textContent = "Atualizacao indisponivel";
        }
      });
  }

  function startRealtime() {
    if (rtTimer) {
      clearInterval(rtTimer);
    }
    rtTimer = setInterval(refreshCardsRealtime, rtIntervalMs);
    if (muralPollTimer) {
      clearInterval(muralPollTimer);
    }
    muralPollTimer = setInterval(pollMuralUpdates, muralPollIntervalMs);
  }

  function parseIsoDate(value) {
    if (!value || typeof value !== "string") {
      return null;
    }
    var parts = value.split("-");
    if (parts.length !== 3) {
      return null;
    }
    var year = parseInt(parts[0], 10);
    var month = parseInt(parts[1], 10);
    var day = parseInt(parts[2], 10);
    if (!isFinite(year) || !isFinite(month) || !isFinite(day)) {
      return null;
    }
    return new Date(year, month - 1, day);
  }

  function formatIsoDate(dateObj) {
    if (!dateObj || Object.prototype.toString.call(dateObj) !== "[object Date]") {
      return "";
    }
    var year = dateObj.getFullYear();
    var month = String(dateObj.getMonth() + 1).padStart(2, "0");
    var day = String(dateObj.getDate()).padStart(2, "0");
    return year + "-" + month + "-" + day;
  }

  function getDateRangeDays(startIso, endIso) {
    var start = parseIsoDate(startIso);
    var end = parseIsoDate(endIso);
    if (!start || !end) {
      return 0;
    }
    return Math.floor((end.getTime() - start.getTime()) / 86400000) + 1;
  }

  function getCsrfToken() {
    var cookie = document.cookie || "";
    var parts = cookie.split(";");
    for (var i = 0; i < parts.length; i += 1) {
      var item = parts[i].trim();
      if (item.indexOf("csrftoken=") === 0) {
        return decodeURIComponent(item.substring("csrftoken=".length));
      }
    }
    return "";
  }

  function getExportTriggerBtn() {
    return dynamicRoot ? dynamicRoot.querySelector("[data-export-open]") : null;
  }

  function getMuralForm() {
    return dynamicRoot ? dynamicRoot.querySelector("[data-mural-form]") : null;
  }

  function getMuralDetails() {
    return dynamicRoot ? dynamicRoot.querySelector("[data-mural-details]") : null;
  }

  function getMuralIntroModal() {
    return dynamicRoot ? dynamicRoot.querySelector("[data-mural-intro-modal]") : null;
  }

  function getMuralListWrap() {
    return dynamicRoot ? dynamicRoot.querySelector("[data-mural-list-wrap]") : null;
  }

  function getMuralUnreadIndicator() {
    return dynamicRoot ? dynamicRoot.querySelector("[data-mural-unread-indicator]") : null;
  }

  function getMuralErrorBox() {
    return dynamicRoot ? dynamicRoot.querySelector("[data-mural-error]") : null;
  }

  function getMuralSubmitButtons() {
    return dynamicRoot ? dynamicRoot.querySelectorAll("[data-mural-submit]") : [];
  }

  function hasSeenMuralIntro() {
    try {
      return window.localStorage && window.localStorage.getItem("app_milhao_bla_mural_intro_seen_v1") === "1";
    } catch (error) {
      return false;
    }
  }

  function markMuralIntroSeen() {
    try {
      if (window.localStorage) {
        window.localStorage.setItem("app_milhao_bla_mural_intro_seen_v1", "1");
      }
    } catch (error) {
    }
  }

  function logMuralAccess(details) {
    if (!details) {
      return;
    }
    var accessUrl = details.getAttribute("data-access-url") || "";
    if (!accessUrl) {
      return;
    }
    fetch(accessUrl, {
      method: "POST",
      headers: {
        "X-CSRFToken": getCsrfToken(),
        "X-Requested-With": "XMLHttpRequest"
      },
      credentials: "same-origin"
    })
      .then(function () {
      })
      .catch(function () {
      });
  }

  function logExportAccess() {
    if (!exportModal) {
      return;
    }
    var accessUrl = exportModal.getAttribute("data-export-access-url") || "";
    if (!accessUrl) {
      return;
    }
    fetch(accessUrl, {
      method: "POST",
      headers: {
        "X-CSRFToken": getCsrfToken(),
        "X-Requested-With": "XMLHttpRequest"
      },
      credentials: "same-origin"
    })
      .then(function () {
      })
      .catch(function () {
      });
  }

  function openMuralIntro(details) {
    var modal = getMuralIntroModal();
    if (!modal || !details) {
      return;
    }
    muralIntroTargetDetails = details;
    modal.classList.remove("is-hidden");
    document.body.style.overflow = "hidden";
  }

  function closeMuralIntro() {
    var modal = getMuralIntroModal();
    if (!modal) {
      return;
    }
    modal.classList.add("is-hidden");
    muralIntroTargetDetails = null;
    if (!exportModal || exportModal.classList.contains("is-hidden")) {
      document.body.style.overflow = "";
    }
  }

  function completeMuralIntro() {
    var details = muralIntroTargetDetails || getMuralDetails();
    if (!details) {
      closeMuralIntro();
      return;
    }
    markMuralIntroSeen();
    closeMuralIntro();
    details.open = true;
  }

  function maybeShowMuralIntroOnLoad() {
    var details = getMuralDetails();
    if (!details || hasSeenMuralIntro()) {
      return;
    }
    openMuralIntro(details);
  }

  function setMuralError(message) {
    var errorBox = getMuralErrorBox();
    if (!errorBox) {
      return;
    }
    if (!message) {
      errorBox.textContent = "";
      errorBox.classList.add("is-hidden");
      return;
    }
    errorBox.textContent = message;
    errorBox.classList.remove("is-hidden");
  }

  function setMuralLoading(isLoading) {
    isSubmittingMural = !!isLoading;
    Array.prototype.forEach.call(getMuralSubmitButtons(), function (button) {
      if (!button) {
        return;
      }
      var defaultLabel = button.getAttribute("data-default-label") || button.textContent;
      if (!button.getAttribute("data-default-label")) {
        button.setAttribute("data-default-label", defaultLabel);
      }
      button.disabled = isSubmittingMural;
      button.textContent = isSubmittingMural && button.classList.contains("is-active-submit")
        ? "Enviando..."
        : defaultLabel;
      if (!isSubmittingMural) {
        button.classList.remove("is-active-submit");
      }
    });
  }

  function updateMuralCount(count) {
    var nextCount = parseInt(count, 10);
    if (!isFinite(nextCount) || nextCount < 0) {
      nextCount = 0;
    }
    dynamicRoot.querySelectorAll("[data-mural-count-text]").forEach(function (node) {
      node.textContent = String(nextCount);
    });
  }

  function setMuralUnread(hasUnread) {
    var indicator = getMuralUnreadIndicator();
    if (!indicator) {
      return;
    }
    indicator.classList.toggle("is-hidden", !hasUnread);
  }

  function parseHtmlFragment(html) {
    var template = document.createElement("template");
    template.innerHTML = html || "";
    return template.content;
  }

  function syncMuralListScrollState() {
    var listWrap = getMuralListWrap();
    if (!listWrap) {
      return 0;
    }
    var notesCount = listWrap.querySelectorAll("[data-mural-note-id]").length;
    listWrap.classList.toggle("is-scrollable", notesCount > 10);
    return notesCount;
  }

  function replaceMuralList(html, highlightNoteId) {
    var listWrap = getMuralListWrap();
    if (!listWrap) {
      return;
    }
    listWrap.innerHTML = "";
    listWrap.appendChild(parseHtmlFragment(html));
    syncMuralListScrollState();
    listWrap.classList.remove("is-refreshing");
    void listWrap.offsetWidth;
    listWrap.classList.add("is-refreshing");
    setTimeout(function () {
      listWrap.classList.remove("is-refreshing");
    }, 260);
    if (!highlightNoteId) {
      return;
    }
    var noteNode = listWrap.querySelector('[data-mural-note-id="' + highlightNoteId + '"]');
    if (!noteNode) {
      return;
    }
    noteNode.classList.add("is-entering");
    if (typeof noteNode.scrollIntoView === "function") {
      noteNode.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
    setTimeout(function () {
      noteNode.classList.remove("is-entering");
    }, 420);
  }

  function getLatestMuralNoteId() {
    var listWrap = getMuralListWrap();
    if (!listWrap) {
      return "";
    }
    var notes = listWrap.querySelectorAll("[data-mural-note-id]");
    if (!notes.length) {
      return "";
    }
    var lastNote = notes[notes.length - 1];
    return lastNote.getAttribute("data-mural-note-id") || "";
  }

  function setExportError(message) {
    if (!exportErrorBox) {
      return;
    }
    if (!message) {
      exportErrorBox.textContent = "";
      exportErrorBox.classList.add("is-hidden");
      return;
    }
    exportErrorBox.textContent = message;
    exportErrorBox.classList.remove("is-hidden");
  }

  function setExportLoading(isLoading) {
    isExporting = !!isLoading;
    var triggerBtn = getExportTriggerBtn();
    if (triggerBtn) {
      triggerBtn.classList.toggle("is-loading", isExporting);
      triggerBtn.disabled = isExporting;
      triggerBtn.setAttribute("aria-disabled", isExporting ? "true" : "false");
    }
    if (exportSubmitBtn) {
      exportSubmitBtn.disabled = isExporting;
      exportSubmitBtn.textContent = isExporting ? "Gerando..." : "Exportar .xlsx";
    }
  }

  function syncExportDateBounds() {
    if (!exportStartInput || !exportEndInput) {
      return;
    }
    var minDate = dateInput ? (dateInput.getAttribute("min") || "") : "";
    var maxDate = dateInput ? (dateInput.getAttribute("max") || "") : "";
    exportStartInput.min = minDate;
    exportEndInput.min = minDate;
    exportStartInput.max = maxDate;
    exportEndInput.max = maxDate;
  }

  function seedExportDates() {
    if (!exportStartInput || !exportEndInput) {
      return;
    }
    syncExportDateBounds();
    var seedDate = (dateInput && dateInput.value) || (availableDates.length ? availableDates[availableDates.length - 1] : "");
    if (!seedDate) {
      seedDate = formatIsoDate(new Date());
    }
    exportStartInput.value = seedDate;
    exportEndInput.value = seedDate;
    setExportError("");
  }

  function openExportModal() {
    if (!exportModal) {
      return;
    }
    seedExportDates();
    exportModal.classList.remove("is-hidden");
    document.body.style.overflow = "hidden";
    if (exportStartInput) {
      setTimeout(function () {
        exportStartInput.focus({ preventScroll: true });
      }, 10);
    }
  }

  function closeExportModal() {
    if (!exportModal) {
      return;
    }
    exportModal.classList.add("is-hidden");
    document.body.style.overflow = "";
    setExportError("");
  }

  function clampToBounds(dateObj) {
    if (!dateObj) {
      return null;
    }
    var minDate = parseIsoDate(exportStartInput ? exportStartInput.min : "");
    var maxDate = parseIsoDate(exportEndInput ? exportEndInput.max : "");
    if (minDate && dateObj < minDate) {
      return minDate;
    }
    if (maxDate && dateObj > maxDate) {
      return maxDate;
    }
    return dateObj;
  }

  function applyQuickRange(days) {
    if (!exportStartInput || !exportEndInput) {
      return;
    }
    var baseDate = parseIsoDate(exportEndInput.value || (dateInput ? dateInput.value : ""));
    if (!baseDate) {
      baseDate = new Date();
    }
    baseDate = clampToBounds(baseDate);
    var startDate = new Date(baseDate.getTime());
    if (days > 0) {
      startDate.setDate(startDate.getDate() - (days - 1));
    }
    startDate = clampToBounds(startDate);
    exportStartInput.value = formatIsoDate(startDate);
    exportEndInput.value = formatIsoDate(baseDate);
    setExportError("");
  }

  function extractFilename(contentDisposition) {
    if (!contentDisposition) {
      return "milhao_bla_export.xlsx";
    }
    var utfMatch = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i);
    if (utfMatch && utfMatch[1]) {
      return decodeURIComponent(utfMatch[1].replace(/["']/g, ""));
    }
    var simpleMatch = contentDisposition.match(/filename=\"?([^\";]+)\"?/i);
    if (simpleMatch && simpleMatch[1]) {
      return simpleMatch[1].trim();
    }
    return "milhao_bla_export.xlsx";
  }

  function downloadBlob(blob, filename) {
    var link = document.createElement("a");
    var objectUrl = window.URL.createObjectURL(blob);
    link.href = objectUrl;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(function () {
      window.URL.revokeObjectURL(objectUrl);
    }, 1000);
  }

  function submitExportForm(event) {
    event.preventDefault();
    if (!exportForm || !exportUrl || isExporting) {
      return;
    }

    var startDate = exportStartInput ? (exportStartInput.value || "") : "";
    var endDate = exportEndInput ? (exportEndInput.value || "") : "";
    if (!startDate || !endDate) {
      setExportError("Selecione data inicial e final.");
      return;
    }
    if (startDate > endDate) {
      setExportError("Data inicial maior que data final.");
      return;
    }

    var days = getDateRangeDays(startDate, endDate);
    if (days <= 0 || days > exportMaxDays) {
      setExportError("Intervalo maximo de " + exportMaxDays + " dias.");
      return;
    }

    setExportError("");
    setExportLoading(true);

    var formData = new FormData();
    formData.set("start_date", startDate);
    formData.set("end_date", endDate);

    fetch(exportUrl, {
      method: "POST",
      body: formData,
      headers: {
        "X-CSRFToken": getCsrfToken(),
        "X-Requested-With": "XMLHttpRequest",
      },
      credentials: "same-origin",
    })
      .then(function (response) {
        var contentType = response.headers.get("content-type") || "";
        if (!response.ok) {
          if (contentType.indexOf("application/json") >= 0) {
            return response.json().then(function (payload) {
              var message = payload && payload.error ? payload.error : "Falha ao exportar arquivo.";
              throw new Error(message);
            });
          }
          throw new Error("Falha ao exportar arquivo.");
        }
        return response.blob().then(function (blob) {
          return {
            blob: blob,
            disposition: response.headers.get("content-disposition") || "",
          };
        });
      })
      .then(function (downloadPayload) {
        var filename = extractFilename(downloadPayload.disposition);
        downloadBlob(downloadPayload.blob, filename);
        closeExportModal();
      })
      .catch(function (error) {
        setExportError((error && error.message) || "Falha ao exportar arquivo.");
      })
      .finally(function () {
        setExportLoading(false);
      });
  }

  function submitMuralForm(targetForm, submitter) {
    if (!targetForm || isSubmittingMural) {
      return;
    }
    if (!submitter) {
      submitter = targetForm.querySelector('[data-mural-submit][name="visibilidade"]');
    }
    if (!submitter) {
      setMuralError("Escolha o tipo de nota.");
      return;
    }

    var formData = new FormData(targetForm);
    var rawText = formData.get("texto");
    var text = typeof rawText === "string" ? rawText.trim() : "";
    if (!text) {
      setMuralError("Informe o texto da nota.");
      return;
    }
    formData.set("texto", text);
    formData.set("visibilidade", submitter.value || "PUBLICA");
    setMuralError("");
    submitter.classList.add("is-active-submit");
    setMuralLoading(true);

    fetch(targetForm.action, {
      method: "POST",
      body: formData,
      headers: {
        "X-CSRFToken": getCsrfToken(),
        "X-Requested-With": "XMLHttpRequest"
      },
      credentials: "same-origin"
    })
      .then(function (response) {
        return response.json().then(function (payload) {
          if (!response.ok || !payload.ok) {
            throw new Error((payload && payload.error) || "Falha ao registrar nota.");
          }
          return payload;
        });
      })
      .then(function (payload) {
        replaceMuralList(payload.list_html, payload.note_id);
        updateMuralCount(payload.notes_count);
        setMuralUnread(!!payload.has_unread);
        targetForm.reset();
        var textField = targetForm.querySelector('textarea[name="texto"]');
        if (textField) {
          textField.focus({ preventScroll: true });
        }
      })
      .catch(function (error) {
        setMuralError((error && error.message) || "Falha ao registrar nota.");
      })
      .finally(function () {
        setMuralLoading(false);
      });
  }

  function deleteMuralNote(button) {
    if (!button || isSubmittingMural) {
      return;
    }
    var deleteUrl = button.getAttribute("data-delete-url") || "";
    if (!deleteUrl) {
      return;
    }
    if (!window.confirm("Excluir esta nota do mural?")) {
      return;
    }

    var noteNode = button.closest("[data-mural-note-id]");
    if (noteNode) {
      noteNode.classList.add("is-removing");
    }
    setMuralError("");
    setMuralLoading(true);

    fetch(deleteUrl, {
      method: "POST",
      headers: {
        "X-CSRFToken": getCsrfToken(),
        "X-Requested-With": "XMLHttpRequest"
      },
      credentials: "same-origin"
    })
      .then(function (response) {
        return response.json().then(function (payload) {
          if (!response.ok || !payload.ok) {
            throw new Error((payload && payload.error) || "Falha ao excluir nota.");
          }
          return payload;
        });
      })
      .then(function (payload) {
        replaceMuralList(payload.list_html);
        updateMuralCount(payload.notes_count);
        setMuralUnread(!!payload.has_unread);
      })
      .catch(function (error) {
        if (noteNode) {
          noteNode.classList.remove("is-removing");
        }
        setMuralError((error && error.message) || "Falha ao excluir nota.");
      })
      .finally(function () {
        setMuralLoading(false);
      });
  }

  function markMuralDayViewed() {
    var details = getMuralDetails();
    if (!details || !details.open) {
      return;
    }
    var indicator = getMuralUnreadIndicator();
    if (!indicator || indicator.classList.contains("is-hidden")) {
      return;
    }
    var markViewedUrl = details.getAttribute("data-mark-viewed-url") || "";
    var selectedDate = details.getAttribute("data-selected-date") || "";
    if (!markViewedUrl || !selectedDate) {
      return;
    }

    var formData = new FormData();
    formData.set("data_referencia", selectedDate);
    fetch(markViewedUrl, {
      method: "POST",
      body: formData,
      headers: {
        "X-CSRFToken": getCsrfToken(),
        "X-Requested-With": "XMLHttpRequest"
      },
      credentials: "same-origin"
    })
      .then(function (response) {
        return response.json().then(function (payload) {
          if (!response.ok || !payload.ok) {
            throw new Error("Falha ao marcar mural como visualizado.");
          }
          return payload;
        });
      })
      .then(function () {
        setMuralUnread(false);
      })
      .catch(function () {
      });
  }

  function pollMuralUpdates() {
    if (document.visibilityState === "hidden" || isNavigating || isSubmittingMural) {
      return;
    }
    var details = getMuralDetails();
    if (!details) {
      return;
    }
    var liveUrl = details.getAttribute("data-live-url") || "";
    var selectedDate = details.getAttribute("data-selected-date") || "";
    if (!liveUrl || !selectedDate) {
      return;
    }

    var nextUrl = new URL(liveUrl, window.location.origin);
    nextUrl.searchParams.set("date", selectedDate);
    var latestNoteId = getLatestMuralNoteId();
    if (latestNoteId) {
      nextUrl.searchParams.set("latest_note_id", latestNoteId);
    }

    fetch(nextUrl.toString(), {
      headers: { "X-Requested-With": "XMLHttpRequest" },
      credentials: "same-origin"
    })
      .then(function (response) {
        return response.json().then(function (payload) {
          if (!response.ok || !payload.ok) {
            throw new Error("Falha ao consultar mural.");
          }
          return payload;
        });
      })
      .then(function (payload) {
        if (payload.has_changed && payload.list_html) {
          replaceMuralList(payload.list_html, payload.latest_note_id);
          if (typeof payload.notes_count !== "undefined") {
            updateMuralCount(payload.notes_count);
          }
          if (details.open) {
            markMuralDayViewed();
            return;
          }
        }
        setMuralUnread(!!payload.has_unread);
      })
      .catch(function () {
      });
  }

  function bindExportHandlers() {
    if (!exportModal || exportModal.getAttribute("data-js-bound") === "1") {
      return;
    }
    exportModal.setAttribute("data-js-bound", "1");

    document.addEventListener("click", function (event) {
      var openBtn = event.target.closest("[data-export-open]");
      if (openBtn) {
        event.preventDefault();
        if (!isExporting) {
          logExportAccess();
          openExportModal();
        }
        return;
      }

      if (event.target.closest("[data-export-modal-close]")) {
        event.preventDefault();
        closeExportModal();
        return;
      }

      var quickBtn = event.target.closest("[data-export-range]");
      if (quickBtn && exportModal && !exportModal.classList.contains("is-hidden")) {
        event.preventDefault();
        var days = parseInt(quickBtn.getAttribute("data-export-range") || "0", 10);
        if (!isFinite(days) || days < 0) {
          days = 0;
        }
        applyQuickRange(days);
      }
    });

    document.addEventListener("keydown", function (event) {
      if (event.key !== "Escape") {
        return;
      }
      var introModal = getMuralIntroModal();
      if (introModal && !introModal.classList.contains("is-hidden")) {
        completeMuralIntro();
        return;
      }
      if (exportModal && !exportModal.classList.contains("is-hidden")) {
        closeExportModal();
      }
    });

    if (exportForm) {
      exportForm.addEventListener("submit", submitExportForm);
    }
  }

  function bindDynamicHandlers() {
    if (!dynamicRoot || dynamicRoot.getAttribute("data-js-bound") === "1") {
      return;
    }
    dynamicRoot.setAttribute("data-js-bound", "1");
    syncMuralListScrollState();

    dynamicRoot.addEventListener("click", function (event) {
      var directDateInput = event.target.closest("#milhao-date-input");
      if (directDateInput) {
        return;
      }

      var prevNextLink = event.target.closest(".milhao-date-arrow[href]");
      if (prevNextLink) {
        event.preventDefault();
        navigatePartial(prevNextLink.href, true);
        return;
      }

      var dateField = event.target.closest(".milhao-date-field");
      if (dateField && dateInput) {
        event.preventDefault();
        openDatePicker(dateInput);
        return;
      }

      var introClose = event.target.closest("[data-mural-intro-close]");
      if (introClose) {
        event.preventDefault();
        completeMuralIntro();
        return;
      }

      var deleteBtn = event.target.closest("[data-mural-delete]");
      if (deleteBtn) {
        event.preventDefault();
        deleteMuralNote(deleteBtn);
      }
    });

    dynamicRoot.addEventListener("change", function (event) {
      var target = event.target;
      if (!target || target.id !== "milhao-date-input") {
        return;
      }
      var resolved = resolveClosestDate(target.value || "");
      if (resolved !== (target.value || "")) {
        target.value = resolved;
      }
      if (target.form) {
        navigatePartial(buildFormUrl(target.form), true);
      }
    });

    dynamicRoot.addEventListener("submit", function (event) {
      var targetForm = event.target;
      if (!targetForm) {
        return;
      }
      if (targetForm.matches(".milhao-date-nav-form")) {
        event.preventDefault();
        navigatePartial(buildFormUrl(targetForm), true);
        return;
      }
      if (targetForm.matches("[data-mural-form]")) {
        event.preventDefault();
        submitMuralForm(targetForm, event.submitter || null);
        return;
      }
    });

    dynamicRoot.addEventListener("toggle", function (event) {
      var details = event.target;
      if (!details || !details.matches("[data-mural-details]")) {
        return;
      }
      if (details.open) {
        logMuralAccess(details);
        markMuralDayViewed();
      }
    }, true);
    maybeShowMuralIntroOnLoad();
  }

  window.addEventListener("popstate", function () {
    navigatePartial(window.location.href, false);
  });

  hydrateElements();
  bindDynamicHandlers();
  bindExportHandlers();
  refreshCardsRealtime();
  startRealtime();
})();
