(function () {
  var dynamicRoot = document.getElementById("milhao-dashboard-dynamic");
  if (!dynamicRoot) {
    return;
  }

  var cardsDataUrl = dynamicRoot.getAttribute("data-cards-url") || "";
  if (!cardsDataUrl) {
    return;
  }

  var rtStatus = document.getElementById("milhao-rt-status");
  var rtIntervalMs = 15000;
  var rtTimer = null;
  var isNavigating = false;
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
    var arrow = document.querySelector('[data-balance-arrow=\"' + balance + '\"]');
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
        return;
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
  }

  function bindDynamicHandlers() {
    if (!dynamicRoot || dynamicRoot.getAttribute("data-js-bound") === "1") {
      return;
    }
    dynamicRoot.setAttribute("data-js-bound", "1");

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
      if (!targetForm || !targetForm.matches(".milhao-date-nav-form")) {
        return;
      }
      event.preventDefault();
      navigatePartial(buildFormUrl(targetForm), true);
    });

  }

  window.addEventListener("popstate", function () {
    navigatePartial(window.location.href, false);
  });

  hydrateElements();
  bindDynamicHandlers();
  refreshCardsRealtime();
  startRealtime();
})();
