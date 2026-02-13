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
  var grid = null;
  var form = null;
  var hiddenContainer = null;
  var dateInput = null;
  var availableDates = [];
  var selected = [];
  var shimmerCleanupTimers = new WeakMap();
  var shimmerLastRunAt = new WeakMap();

  function hydrateElements() {
    dynamicRoot = document.getElementById("milhao-dashboard-dynamic");
    grid = dynamicRoot ? dynamicRoot.querySelector("#balance-chip-grid") : null;
    form = grid ? grid.closest("form") : null;
    hiddenContainer = dynamicRoot ? dynamicRoot.querySelector("#balance-hidden-inputs") : null;
    dateInput = dynamicRoot ? dynamicRoot.querySelector("#milhao-date-input") : null;
    rtStatus = document.getElementById("milhao-rt-status");
    cardsDataUrl = dynamicRoot ? (dynamicRoot.getAttribute("data-cards-url") || cardsDataUrl) : cardsDataUrl;
    availableDates = dateInput
      ? (dateInput.getAttribute("data-available-dates") || "")
          .split(",")
          .map(function (item) { return item.trim(); })
          .filter(Boolean)
      : [];
    selected = grid
      ? (grid.getAttribute("data-selected") || "")
          .split(",")
          .map(function (item) { return item.trim(); })
          .filter(Boolean)
      : [];
  }

  function setHiddenInputs(values) {
    if (!hiddenContainer) {
      return;
    }
    hiddenContainer.innerHTML = "";
    values.forEach(function (value) {
      var input = document.createElement("input");
      input.type = "hidden";
      input.name = "balance";
      input.value = value;
      hiddenContainer.appendChild(input);
    });
  }

  function updateUI() {
    if (!grid) {
      return;
    }
    var chips = Array.from(grid.querySelectorAll(".balance-chip"));
    var selectableChips = chips.filter(function (chip) {
      return chip.getAttribute("data-balance") !== "__all__";
    });
    selectableChips.forEach(function (chip) {
      var balance = chip.getAttribute("data-balance");
      var main = chip.querySelector(".balance-chip-main");
      var add = chip.querySelector(".balance-chip-add");
      var symbol = add ? add.querySelector(".balance-chip-symbol") : null;
      var isSelected = selected.indexOf(balance) >= 0;
      chip.classList.toggle("is-selected", isSelected);
      if (main) {
        main.classList.toggle("is-selected", isSelected);
      }
      if (add) {
        add.classList.toggle("is-selected", isSelected);
      }
      if (symbol) {
        symbol.textContent = isSelected ? "-" : "+";
      }
    });
    var allChip = grid.querySelector(".balance-chip-all");
    if (allChip) {
      var allSelected = selectableChips.length > 0 && selected.length === selectableChips.length;
      allChip.classList.toggle("is-selected", allSelected);
    }
    setHiddenInputs(selected);
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
        updateUI();
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
    var timerId = shimmerCleanupTimers.get(host);
    if (timerId) {
      clearTimeout(timerId);
      shimmerCleanupTimers.delete(host);
    }
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
      return;
    }
    var now = Date.now();
    var lastRun = shimmerLastRunAt.get(host) || 0;
    if (now - lastRun < 140) {
      return;
    }
    shimmerLastRunAt.set(host, now);

    host.classList.add("rt-shimmer-host");
    clearShimmerState(host);
    void host.offsetWidth;
    host.classList.add("rt-shimmer");

    var fallbackTimer = setTimeout(function () {
      clearShimmerState(host);
    }, 650);
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

  function setTextIfChanged(el, nextText) {
    if (!el) {
      return;
    }
    var normalized = (nextText || "").trim();
    if (el.textContent.trim() === normalized) {
      return;
    }
    el.textContent = normalized;
    pulseElement(el);
  }

  function setTagText(tag, text) {
    if (!tag) {
      return;
    }
    var normalized = (text || "").trim();
    if (!normalized) {
      tag.textContent = "";
      tag.classList.add("is-hidden");
      return;
    }
    tag.classList.remove("is-hidden");
    if (tag.textContent.trim() !== normalized) {
      tag.textContent = normalized;
      pulseElement(tag);
    }
  }

  function getFilters() {
    var dateValue = dateInput ? (dateInput.value || "") : "";
    return {
      date: dateValue,
      balances: selected.slice(),
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
    setTextIfChanged(document.getElementById("metric-total-number"), payload.total_value_display || "0");
    setTagText(
      document.getElementById("metric-avg-total"),
      payload.avg_total_14_display ? ("MEDIA " + payload.avg_total_14_display + " kg") : ""
    );

    var totalsMap = {};
    (payload.totals_by_balance || []).forEach(function (item) {
      totalsMap[item.balance] = item;
    });
    document.querySelectorAll("[data-balance-total-number]").forEach(function (node) {
      var balance = node.getAttribute("data-balance-total-number");
      var item = totalsMap[balance];
      setTextIfChanged(node, item ? (item.total_display || "0") : "0");
    });
    document.querySelectorAll("[data-balance-avg]").forEach(function (tag) {
      var balance = tag.getAttribute("data-balance-avg");
      var item = totalsMap[balance];
      setTagText(tag, item && item.avg_14_display ? ("MEDIA " + item.avg_14_display + " kg") : "");
    });

    var compositionMap = {};
    (payload.composition || []).forEach(function (item) {
      compositionMap[item.balance] = item;
    });
    document.querySelectorAll("[data-comp-segment]").forEach(function (seg) {
      var balance = seg.getAttribute("data-comp-segment");
      var item = compositionMap[balance];
      var nextPercent = item ? (item.percent_str || "0.0") : "0.0";
      var nextFlex = nextPercent + "%";
      if (seg.style.flexBasis !== nextFlex) {
        seg.style.flexBasis = nextFlex;
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
    filters.balances.forEach(function (balance) {
      params.append("balance", balance);
    });
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

    dynamicRoot.addEventListener("animationend", function (event) {
      if (!event || event.animationName !== "rtShimmerSweep") {
        return;
      }
      var target = event.target;
      if (!target || !target.classList || !target.classList.contains("rt-shimmer-host")) {
        return;
      }
      clearShimmerState(target);
    });

    dynamicRoot.addEventListener("click", function (event) {
      var directDateInput = event.target.closest("#milhao-date-input");
      if (directDateInput) {
        openDatePicker(directDateInput);
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

      if (!grid || !form) {
        return;
      }

      var mainButton = event.target.closest(".balance-chip-main");
      var addButton = event.target.closest(".balance-chip-add");
      if (!mainButton && !addButton) {
        return;
      }
      event.preventDefault();
      var chip = event.target.closest(".balance-chip");
      if (!chip) {
        return;
      }
      var balance = chip.getAttribute("data-balance");
      if (balance === "__all__") {
        selected = Array.from(grid.querySelectorAll(".balance-chip"))
          .map(function (item) { return item.getAttribute("data-balance"); })
          .filter(function (item) { return item && item !== "__all__"; });
        updateUI();
        navigatePartial(buildFormUrl(form), true);
        return;
      }
      if (mainButton) {
        selected = [balance];
        updateUI();
        navigatePartial(buildFormUrl(form), true);
        return;
      }
      if (addButton) {
        if (selected.indexOf(balance) >= 0) {
          selected = selected.filter(function (item) { return item !== balance; });
        } else {
          selected = selected.concat(balance);
        }
        updateUI();
        navigatePartial(buildFormUrl(form), true);
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
      if (!targetForm || !targetForm.matches(".milhao-date-nav-form, .metrics-filter-form")) {
        return;
      }
      event.preventDefault();
      navigatePartial(buildFormUrl(targetForm), true);
    });

    dynamicRoot.addEventListener("pointerdown", function (event) {
      var field = event.target.closest(".milhao-date-field");
      if (!field || !dateInput) {
        return;
      }
      openDatePicker(dateInput);
    });
  }

  window.addEventListener("popstate", function () {
    navigatePartial(window.location.href, false);
  });

  hydrateElements();
  bindDynamicHandlers();
  updateUI();
  refreshCardsRealtime();
  startRealtime();
})();
