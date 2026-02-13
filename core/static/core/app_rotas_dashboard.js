(function () {
  var scriptEl = document.currentScript || document.querySelector("script[data-dashboard-state-id]");
  if (!scriptEl) {
    return;
  }

  var stateNode = document.getElementById(scriptEl.dataset.dashboardStateId || "");
  if (!stateNode) {
    return;
  }

  var stateUrl = scriptEl.dataset.dashboardStateUrl || window.location.pathname;
  var orderUrl = scriptEl.dataset.orderUrl || "";

  var state = {};
  try {
    state = JSON.parse(stateNode.textContent || "{}");
  } catch (_error) {
    return;
  }

  var pollDelays = [5000, 9000, 14000];
  var backoffIndex = 0;
  var pollTimer = null;
  var inFlight = false;
  var inFlightCount = 0;
  var activeController = null;
  var timelinePendingIso = null;
  var timelineLoadingCards = false;
  var timelineLoadingRequests = 0;
  var pendingTimelineRequestIso = "";
  var isTimelineDragging = false;
  var timelineCommitTimer = null;
  var queuedTimelineIso = "";
  var autoplayTimer = null;
  var autoplayRunning = false;
  var lastTimelineRequestedIso = null;
  var TIMELINE_COMMIT_STABILIZE_MS = 1000;

  var els = {
    dayForm: document.getElementById("day-nav-form"),
    dayNavCapsule: document.querySelector("#day-nav-form .day-nav-capsule"),
    daySelect: document.getElementById("day-select"),
    dayPrevButton: document.getElementById("day-prev-button"),
    dayNextButton: document.getElementById("day-next-button"),
    timelineRange: document.getElementById("timeline-range"),
    timelineSliderShell: document.getElementById("timeline-slider-shell"),
    timelineSliderBase: document.getElementById("timeline-slider-base"),
    timelineSliderProgress: document.getElementById("timeline-slider-progress"),
    timelineSliderThumb: document.getElementById("timeline-slider-thumb"),
    timelineNowMarker: document.getElementById("timeline-now-marker"),
    timelineTooltip: document.getElementById("timeline-thumb-tooltip"),
    timelinePlayToggle: document.getElementById("timeline-play-toggle"),
    timelinePlayLabel: document.getElementById("timeline-play-label"),
    timelineMinus15: document.getElementById("timeline-minus-15"),
    timelinePlus15: document.getElementById("timeline-plus-15"),
    timelineNowMini: document.getElementById("timeline-now-mini"),
    timelineAtField: document.getElementById("timeline-at"),
    timelineReadLabel: document.getElementById("timeline-read-label"),
    timelineBackNow: document.getElementById("timeline-back-now"),
    lifebitBadge: document.getElementById("lifebit-badge"),
    lifebitLastSeen: document.getElementById("lifebit-last-seen"),
    totalEventsNote: document.getElementById("total-events-note"),
    selectedAtNote: document.getElementById("selected-at-note"),
    globalLigadaTrack: document.getElementById("global-ligada-track"),
    cardsContainer: document.getElementById("rotas-cards-container"),
    eventsContainer: document.getElementById("recent-events-container"),
    syncStatus: document.getElementById("dashboard-sync-status"),
    liveBadge: document.getElementById("dashboard-live-badge"),
    liveShort: document.getElementById("dashboard-live-short"),
    liveAction: document.getElementById("timeline-back-action"),
    liveHint: document.getElementById("timeline-back-hint"),
    rotasStateShells: document.querySelectorAll(".rotas-state-shell"),
  };

  function createElement(tagName, className, text) {
    var node = document.createElement(tagName);
    if (className) {
      node.className = className;
    }
    if (typeof text !== "undefined") {
      node.textContent = text;
    }
    return node;
  }

  function formatDayLabel(dayIso) {
    if (!dayIso) {
      return "--/--/----";
    }
    var parts = dayIso.split("-");
    if (parts.length !== 3) {
      return dayIso;
    }
    return parts[2] + "/" + parts[1] + "/" + parts[0];
  }

  function parseAvailableDays() {
    if (!els.daySelect) {
      return [];
    }
    if ((els.daySelect.tagName || "").toUpperCase() === "INPUT") {
      return (els.daySelect.getAttribute("data-available-days") || "")
        .split(",")
        .map(function (item) { return item.trim(); })
        .filter(Boolean);
    }
    var days = [];
    for (var i = 0; i < els.daySelect.options.length; i += 1) {
      if (els.daySelect.options[i].value) {
        days.push(els.daySelect.options[i].value);
      }
    }
    return days;
  }

  function resolveClosestDay(nextDay) {
    var availableDays = parseAvailableDays();
    if (!nextDay || !availableDays.length) {
      return nextDay;
    }
    if (availableDays.indexOf(nextDay) >= 0) {
      return nextDay;
    }
    var nextTs = Date.parse(nextDay + "T00:00:00");
    if (!Number.isFinite(nextTs)) {
      return availableDays[0];
    }
    var best = availableDays[0];
    var bestDiff = Math.abs(Date.parse(best + "T00:00:00") - nextTs);
    for (var i = 1; i < availableDays.length; i += 1) {
      var candidate = availableDays[i];
      var candidateTs = Date.parse(candidate + "T00:00:00");
      if (!Number.isFinite(candidateTs)) {
        continue;
      }
      var diff = Math.abs(candidateTs - nextTs);
      if (diff < bestDiff) {
        best = candidate;
        bestDiff = diff;
      }
    }
    return best;
  }

  function openDayPicker() {
    if (!els.daySelect || (els.daySelect.tagName || "").toUpperCase() !== "INPUT") {
      return;
    }
    els.daySelect.focus({ preventScroll: true });
    if (typeof els.daySelect.showPicker === "function") {
      try {
        els.daySelect.showPicker();
      } catch (_error) {
      }
    }
  }

  function isLiveMode() {
    return !!(state.follow_now && state.showing_now && state.selected_day === state.now_day);
  }

  function clearPollTimer() {
    if (pollTimer) {
      clearTimeout(pollTimer);
      pollTimer = null;
    }
  }

  function clearTimelineCommitTimer() {
    if (timelineCommitTimer) {
      clearTimeout(timelineCommitTimer);
      timelineCommitTimer = null;
    }
    queuedTimelineIso = "";
  }

  function enterHistoricalMode() {
    state.follow_now = false;
    clearPollTimer();
    renderLiveBadge();
  }

  function setTimelineCardsLoading(enabled) {
    var next = !!enabled;
    if (timelineLoadingCards === next) {
      return;
    }
    timelineLoadingCards = next;
    if (els.cardsContainer) {
      els.cardsContainer.classList.toggle("is-timeline-loading", next);
    }
    if (els.dayNavCapsule) {
      els.dayNavCapsule.classList.toggle("is-loading", next);
    }
    renderCards();
  }

  function beginTimelineLoading() {
    timelineLoadingRequests += 1;
    setTimelineCardsLoading(true);
  }

  function endTimelineLoading() {
    timelineLoadingRequests = Math.max(0, timelineLoadingRequests - 1);
    if (!timelineLoadingRequests) {
      setTimelineCardsLoading(false);
    }
  }

  function stopAutoplay() {
    autoplayRunning = false;
    if (autoplayTimer) {
      clearTimeout(autoplayTimer);
      autoplayTimer = null;
    }
    syncPlayUi();
  }

  function syncPlayUi() {
    if (!els.timelinePlayToggle) {
      return;
    }
    els.timelinePlayToggle.classList.toggle("is-running", autoplayRunning);
    if (els.timelinePlayLabel) {
      els.timelinePlayLabel.textContent = autoplayRunning ? "Pause" : "Play";
    }
    els.timelinePlayToggle.setAttribute("aria-label", autoplayRunning ? "Pausar timeline" : "Reproduzir timeline");
    els.timelinePlayToggle.setAttribute("title", autoplayRunning ? "Pausar timeline" : "Reproduzir timeline");
  }

  function setSyncStatus(mode) {
    if (!els.syncStatus) {
      return;
    }
    if (mode === "updating") {
      els.syncStatus.textContent = "atualizando...";
      return;
    }
    if (mode === "error") {
      els.syncStatus.textContent = "erro de sincronizacao";
      return;
    }
    els.syncStatus.textContent = "";
  }

  function renderLiveBadge() {
    if (!els.liveBadge && !els.timelineBackNow) {
      return;
    }
    var live = isLiveMode();
    if (els.liveBadge) {
      els.liveBadge.textContent = live ? "Ao vivo" : "Historico";
    }
    if (els.liveShort) {
      els.liveShort.textContent = live ? "LIVE" : "HIST";
      els.liveShort.classList.toggle("is-live", live);
      els.liveShort.classList.toggle("is-history", !live);
    }
    if (els.timelineBackNow) {
      els.timelineBackNow.classList.toggle("is-live", live);
      els.timelineBackNow.classList.toggle("is-history", !live);
      els.timelineBackNow.setAttribute("aria-label", live ? "Modo ao vivo" : "Ir para ao vivo");
      els.timelineBackNow.setAttribute("title", live ? "Modo ao vivo" : "Ir para ao vivo");
    }
    if (els.liveAction) {
      els.liveAction.textContent = "Ir para ao vivo";
      els.liveAction.classList.toggle("is-hidden", live);
    }
    if (els.liveHint) {
      els.liveHint.classList.toggle("is-hidden", live);
    }
    if (els.rotasStateShells && els.rotasStateShells.length) {
      for (var i = 0; i < els.rotasStateShells.length; i += 1) {
        els.rotasStateShells[i].classList.toggle("is-live", live);
        els.rotasStateShells[i].classList.toggle("is-history", !live);
      }
    }
  }

  function updateTimelineNote() {
    if (els.selectedAtNote) {
      els.selectedAtNote.textContent = state.selected_at_label || "-";
    }
    if (els.totalEventsNote) {
      els.totalEventsNote.textContent = String(state.total_events || 0);
    }
  }

  function renderLifebit() {
    if (els.lifebitBadge) {
      els.lifebitBadge.textContent = state.lifebit_label || (state.lifebit_connected ? "Conectado" : "Desconectado");
      els.lifebitBadge.classList.toggle("comm-on", !!state.lifebit_connected);
      els.lifebitBadge.classList.toggle("comm-off", !state.lifebit_connected);
      els.lifebitBadge.title = "Ultimo LIFEBIT: " + (state.lifebit_last_seen || "-");
    }
    if (els.lifebitLastSeen) {
      els.lifebitLastSeen.textContent = "Ultima conexao lida: " + (state.lifebit_last_seen || "-");
      els.lifebitLastSeen.classList.toggle("is-hidden", !!state.lifebit_connected);
    }
  }

  function renderDayNavigation() {
    if (els.daySelect) {
      var availableDays = Array.isArray(state.available_days) ? state.available_days.slice() : [];
      if (!availableDays.length && state.selected_day) {
        availableDays = [state.selected_day];
      }
      if ((els.daySelect.tagName || "").toUpperCase() === "INPUT") {
        if (state.selected_day) {
          els.daySelect.value = state.selected_day;
        }
        els.daySelect.setAttribute("data-available-days", availableDays.join(","));
        els.daySelect.removeAttribute("min");
        els.daySelect.removeAttribute("max");
      } else {
        var fragment = document.createDocumentFragment();
        for (var i = 0; i < availableDays.length; i += 1) {
          var dayIso = availableDays[i];
          var option = createElement("option");
          option.value = dayIso;
          option.textContent = formatDayLabel(dayIso);
          option.selected = dayIso === state.selected_day;
          fragment.appendChild(option);
        }
        els.daySelect.replaceChildren(fragment);
      }
    }

    if (els.dayPrevButton) {
      if (state.prev_day) {
        els.dayPrevButton.disabled = false;
        els.dayPrevButton.type = "submit";
        els.dayPrevButton.name = "nav_dia";
        els.dayPrevButton.value = state.prev_day;
      } else {
        els.dayPrevButton.disabled = true;
        els.dayPrevButton.type = "button";
        els.dayPrevButton.value = "";
      }
    }

    if (els.dayNextButton) {
      if (state.next_day) {
        els.dayNextButton.disabled = false;
        els.dayNextButton.type = "submit";
        els.dayNextButton.name = "nav_dia";
        els.dayNextButton.value = state.next_day;
      } else {
        els.dayNextButton.disabled = true;
        els.dayNextButton.type = "button";
        els.dayNextButton.value = "";
      }
    }
  }

  function renderTimeline() {
    var timeline = Array.isArray(state.timeline) ? state.timeline : [];
    if (els.timelineRange) {
      var timelineLength = timeline.length;
      var max = Math.max(0, timelineLength - 1);
      els.timelineRange.max = String(max);
      els.timelineRange.disabled = max < 1;
      if (typeof state.selected_index === "number" && state.selected_index >= 0) {
        els.timelineRange.value = String(state.selected_index);
      }
      updateRangeProgress();
    }

    if (els.timelineAtField) {
      els.timelineAtField.value = state.selected_at_iso || state.selected_at || "";
    }

    if (els.timelineReadLabel) {
      els.timelineReadLabel.textContent = state.selected_at_label || "-";
    }

    if (els.timelineBackNow) {
      if (state.now_day && state.now_at_iso) {
        els.timelineBackNow.setAttribute("href", "?dia=" + state.now_day + "&at=" + encodeURIComponent(state.now_at_iso));
      }
    }

    renderTimelineAvailability();

    var jumpsDisabled = timeline.length < 2 || !els.timelineRange || els.timelineRange.disabled;
    if (els.timelineMinus15) {
      els.timelineMinus15.disabled = jumpsDisabled;
    }
    if (els.timelinePlus15) {
      els.timelinePlus15.disabled = jumpsDisabled;
    }
    if (els.timelineNowMini) {
      els.timelineNowMini.disabled = false;
    }
  }

  function updateRangeProgress() {
    if (!els.timelineRange) {
      return;
    }
    var min = Number(els.timelineRange.min || "0");
    var max = Number(els.timelineRange.max || "0");
    var value = Number(els.timelineRange.value || "0");
    var pct = max > min ? ((value - min) / (max - min)) * 100 : 0;
    if (els.timelineSliderBase) {
      els.timelineSliderBase.style.background = state.global_ligada_gradient || "linear-gradient(90deg, var(--timeline-track-1), var(--timeline-track-2))";
    }
    if (els.timelineSliderProgress) {
      els.timelineSliderProgress.style.width = pct.toFixed(2) + "%";
    }
    if (els.timelineSliderThumb) {
      els.timelineSliderThumb.style.left = pct.toFixed(2) + "%";
    }
  }

  function setTimelineDraggingVisual(enabled) {
    if (!els.timelineSliderShell) {
      return;
    }
    els.timelineSliderShell.classList.toggle("is-dragging", !!enabled);
    if (els.timelineTooltip) {
      els.timelineTooltip.classList.toggle("is-below", !!enabled);
    }
  }

  function indexToPct(index, total) {
    if (total <= 1) {
      return 0;
    }
    var clamped = Math.max(0, Math.min(index, total - 1));
    return (clamped / (total - 1)) * 100;
  }

  function renderTimelineAvailability() {
    var timeline = Array.isArray(state.timeline) ? state.timeline : [];
    var total = timeline.length;
    if (!total) {
      return;
    }
    var availableIndex = Number(state.available_index);
    if (!Number.isFinite(availableIndex) || availableIndex < 0) {
      availableIndex = total - 1;
    }
    var nowPct = indexToPct(availableIndex, total);

    if (els.timelineNowMarker) {
      els.timelineNowMarker.style.left = nowPct.toFixed(3) + "%";
      els.timelineNowMarker.style.display = "none";
    }
  }

  function setTimelineTooltipVisible(visible) {
    if (!els.timelineTooltip) {
      return;
    }
    els.timelineTooltip.classList.toggle("is-hidden", !visible);
  }

  function updateTimelineTooltip(pointLabel) {
    if (!els.timelineTooltip || !els.timelineRange) {
      return;
    }
    var label = String(pointLabel || "").trim();
    if (label.indexOf(" ") > 0) {
      label = label.split(" ").pop();
    }
    els.timelineTooltip.textContent = label || "--:--:--";

    var wrap = els.timelineRange.closest(".timeline-track-wrap");
    if (!wrap) {
      return;
    }
    var wrapRect = wrap.getBoundingClientRect();
    var rangeRect = (els.timelineSliderShell || els.timelineRange).getBoundingClientRect();
    var min = Number(els.timelineRange.min || "0");
    var max = Number(els.timelineRange.max || "0");
    var value = Number(els.timelineRange.value || "0");
    var pct = max > min ? (value - min) / (max - min) : 0;
    var thumbWidth = els.timelineSliderThumb ? els.timelineSliderThumb.offsetWidth : 24;
    var x = rangeRect.left - wrapRect.left + pct * (rangeRect.width - thumbWidth) + thumbWidth / 2;
    els.timelineTooltip.style.left = x.toFixed(1) + "px";
  }

  function pointFromCurrentRange() {
    if (!els.timelineRange) {
      return null;
    }
    var timeline = Array.isArray(state.timeline) ? state.timeline : [];
    if (!timeline.length) {
      return null;
    }
    var index = Number(els.timelineRange.value || "0");
    if (!Number.isFinite(index)) {
      return null;
    }
    index = Math.max(0, Math.min(index, timeline.length - 1));
    return timeline[index] || null;
  }

  function findClosestTimelineIndexByMs(targetMs) {
    var timeline = Array.isArray(state.timeline) ? state.timeline : [];
    if (!timeline.length) {
      return -1;
    }
    var best = 0;
    for (var i = 0; i < timeline.length; i += 1) {
      var ts = Date.parse(timeline[i].iso);
      if (!Number.isFinite(ts)) {
        continue;
      }
      if (ts <= targetMs) {
        best = i;
      } else {
        break;
      }
    }
    return best;
  }

  function requestTimelineIso(nextIso) {
    if (!nextIso) {
      return;
    }
    var currentIso = state.selected_at_iso || state.selected_at || "";
    if (nextIso === currentIso) {
      return;
    }
    enterHistoricalMode();
    timelinePendingIso = null;
    clearTimelineCommitTimer();
    setTimelineCardsLoading(true);
    refreshState(
      { selected_at_iso: nextIso, events_page: 1, follow_now: false },
      { timeline_loading: true, timeline_iso: nextIso, abortPrevious: true }
    );
  }

  function scheduleTimelineCommit(nextIso) {
    if (!nextIso) {
      return;
    }
    var currentIso = state.selected_at_iso || state.selected_at || "";
    if (nextIso === currentIso) {
      if (!inFlight) {
        setTimelineCardsLoading(false);
      }
      return;
    }
    queuedTimelineIso = nextIso;
    setTimelineCardsLoading(true);
    if (timelineCommitTimer) {
      clearTimeout(timelineCommitTimer);
    }
    timelineCommitTimer = setTimeout(function () {
      var isoToCommit = queuedTimelineIso;
      timelineCommitTimer = null;
      queuedTimelineIso = "";
      if (!isoToCommit) {
        if (!inFlight) {
          setTimelineCardsLoading(false);
        }
        return;
      }
      if (isoToCommit === (state.selected_at_iso || state.selected_at || "")) {
        if (!inFlight) {
          setTimelineCardsLoading(false);
        }
        return;
      }
      if (isoToCommit === lastTimelineRequestedIso && inFlight) {
        return;
      }
      lastTimelineRequestedIso = isoToCommit;
      refreshState(
        { selected_at_iso: isoToCommit, events_page: 1, follow_now: false },
        { timeline_loading: true, timeline_iso: isoToCommit, abortPrevious: true }
      );
    }, TIMELINE_COMMIT_STABILIZE_MS);
  }

  function goToLiveNow() {
    stopAutoplay();
    clearTimelineCommitTimer();
    state.follow_now = true;
    refreshState(
      {
        selected_day: state.now_day || state.selected_day,
        selected_at_iso: state.now_at_iso || state.selected_at_iso,
        events_page: 1,
        follow_now: true,
      },
      { timeline_loading: true, timeline_iso: state.now_at_iso || state.selected_at_iso || "", abortPrevious: true }
    );
  }

  function autoplayStep() {
    if (!autoplayRunning || !els.timelineRange) {
      stopAutoplay();
      return;
    }
    var timeline = Array.isArray(state.timeline) ? state.timeline : [];
    if (!timeline.length) {
      stopAutoplay();
      return;
    }
    var currentIndex = Number(els.timelineRange.value || state.selected_index || 0);
    if (!Number.isFinite(currentIndex)) {
      currentIndex = 0;
    }
    currentIndex = Math.max(0, Math.min(currentIndex, timeline.length - 1));

    var maxIndex = Number(state.available_index);
    if (!Number.isFinite(maxIndex) || maxIndex < 0) {
      maxIndex = timeline.length - 1;
    }
    maxIndex = Math.max(0, Math.min(maxIndex, timeline.length - 1));

    if (currentIndex >= maxIndex) {
      stopAutoplay();
      return;
    }

    var nextIndex = Math.min(maxIndex, currentIndex + 1);
    var point = timeline[nextIndex];
    if (!point || !point.iso) {
      stopAutoplay();
      return;
    }

    els.timelineRange.value = String(nextIndex);
    updateRangeProgress();
    if (els.timelineReadLabel) {
      els.timelineReadLabel.textContent = point.label;
    }
    if (els.selectedAtNote) {
      els.selectedAtNote.textContent = point.label;
    }
    requestTimelineIso(point.iso);
    autoplayTimer = setTimeout(autoplayStep, 450);
  }

  function toggleAutoplay() {
    if (autoplayRunning) {
      stopAutoplay();
      return;
    }
    enterHistoricalMode();
    autoplayRunning = true;
    syncPlayUi();
    autoplayStep();
  }

  function buildCardNode() {
    var card = createElement("a", "panel-card rota-card");
    card.draggable = true;

    var top = createElement("div", "panel-card-top");
    var left = createElement("div");
    var title = createElement("div", "panel-title");
    title.dataset.role = "title";
    var prefix = createElement("div", "muted rota-prefix");
    prefix.dataset.role = "prefix";
    left.appendChild(title);
    left.appendChild(prefix);
    top.appendChild(left);
    top.appendChild(createElement("div", "rota-drag-hint", "::"));
    card.appendChild(top);

    var main = createElement("div", "rota-main");
    main.dataset.role = "main";
    card.appendChild(main);

    var status = createElement("div", "rota-status");
    var play = createElement("span", "status-chip", "Play");
    play.dataset.role = "play";
    var pause = createElement("span", "status-chip", "Pause");
    pause.dataset.role = "pause";
    status.appendChild(play);
    status.appendChild(pause);
    card.appendChild(status);

    var context = createElement("div", "muted rota-context-status");
    context.dataset.role = "context";
    card.appendChild(context);

    return card;
  }

  function ensureCommDisconnectedUi(card) {
    var badge = card.querySelector(".rota-comm-badge");
    if (!badge) {
      badge = createElement("span", "rota-comm-badge");
      badge.setAttribute("aria-hidden", "true");
      card.appendChild(badge);
    }
    var footnote = card.querySelector('[data-role="comm-last-seen"]');
    if (!footnote) {
      footnote = createElement("div", "muted rota-comm-footnote");
      footnote.dataset.role = "comm-last-seen";
      card.appendChild(footnote);
    }
    return { badge: badge, footnote: footnote };
  }

  function applyCardState(card, rota) {
    card.href = rota.detail_url || "#";
    card.dataset.prefix = rota.prefixo || "";

    card.className = "panel-card rota-card";
    if (rota.play_blink) {
      card.classList.add("is-play-blink");
    } else if (rota.play_on) {
      card.classList.add("is-play-on");
    }
    if (rota.pause_on) {
      card.classList.add("is-pause-on");
    }
    if (timelineLoadingCards) {
      card.classList.add("is-timeline-loading");
    }

    var title = card.querySelector('[data-role="title"]') || card.querySelector(".panel-title");
    var prefix = card.querySelector('[data-role="prefix"]') || card.querySelector(".rota-prefix");
    var main = card.querySelector('[data-role="main"]') || card.querySelector(".rota-main");
    var context = card.querySelector('[data-role="context"]') || card.querySelector(".rota-context-status");
    var play = card.querySelector('[data-role="play"]') || card.querySelector(".rota-status .status-chip:first-child");
    var pause = card.querySelector('[data-role="pause"]') || card.querySelector(".rota-status .status-chip:last-child");

    if (title) {
      title.textContent = rota.titulo || rota.prefixo || "-";
    }
    if (prefix) {
      prefix.textContent = rota.prefixo || "";
      prefix.style.display = rota.nome_exibicao ? "" : "none";
    }
    if (main) {
      main.textContent = (rota.origem_display || "--") + " > " + (rota.destino_display || "--");
    }
    if (context) {
      context.textContent = rota.context_status || "Estado indefinido";
    }
    if (play) {
      play.className = "status-chip";
      if (rota.play_blink) {
        play.classList.add("is-blink");
      } else if (rota.play_on) {
        play.classList.add("is-active");
      }
    }
    if (pause) {
      pause.className = "status-chip";
      if (rota.pause_on) {
        pause.classList.add("is-active");
        pause.classList.add("pause");
      }
    }

    if (!state.lifebit_connected) {
      var commUi = ensureCommDisconnectedUi(card);
      if (commUi.footnote) {
        commUi.footnote.textContent = "Ultima leitura: " + (state.lifebit_last_seen || "-");
      }
    } else {
      var currentBadge = card.querySelector(".rota-comm-badge");
      if (currentBadge) { currentBadge.remove(); }
      var currentFootnote = card.querySelector('[data-role="comm-last-seen"]');
      if (currentFootnote) { currentFootnote.remove(); }
      var oldOverlay = card.querySelector(".rota-comm-overlay");
      if (oldOverlay) { oldOverlay.remove(); }
    }

    var loadingClock = card.querySelector(".rota-card-loading-clock");
    if (timelineLoadingCards) {
      if (!loadingClock) {
        loadingClock = createElement("span", "rota-card-loading-clock");
        loadingClock.setAttribute("aria-hidden", "true");
        card.appendChild(loadingClock);
      }
    } else if (loadingClock) {
      loadingClock.remove();
    }
  }

  function renderCards() {
    if (!els.cardsContainer) {
      return;
    }

    var cards = Array.isArray(state.cards) ? state.cards : [];
    if (!cards.length) {
      els.cardsContainer.replaceChildren(createElement("p", "muted", "Nenhuma rota encontrada para o dia selecionado."));
      return;
    }

    var grid = document.getElementById("rotas-grid");
    if (!grid) {
      grid = createElement("div", "rotas-grid");
      grid.id = "rotas-grid";
      els.cardsContainer.replaceChildren(grid);
    }

    if (grid.querySelector(".rota-card.is-dragging")) {
      return;
    }

    var existing = {};
    var existingCards = grid.querySelectorAll(".rota-card[data-prefix]");
    for (var i = 0; i < existingCards.length; i += 1) {
      existing[existingCards[i].dataset.prefix] = existingCards[i];
    }

    var seen = {};
    for (var idx = 0; idx < cards.length; idx += 1) {
      var rota = cards[idx];
      var key = rota.prefixo || "";
      var node = existing[key] || buildCardNode();
      applyCardState(node, rota);
      seen[key] = true;

      var atIndexNode = grid.children[idx] || null;
      if (node !== atIndexNode) {
        grid.insertBefore(node, atIndexNode);
      }
    }

    for (var j = grid.children.length - 1; j >= 0; j -= 1) {
      var child = grid.children[j];
      var childKey = child.dataset ? child.dataset.prefix : "";
      if (childKey && !seen[childKey]) {
        child.remove();
      }
    }
  }

  function renderEvents() {
    if (!els.eventsContainer) {
      return;
    }

    var payload = state.eventos_recentes || {};
    var items = Array.isArray(payload.items) ? payload.items : [];
    var page = payload.page || {};

    if (!items.length) {
      els.eventsContainer.replaceChildren(createElement("p", "muted", "Sem eventos para listar."));
      return;
    }

    var wrapper = createElement("div", "table-wrap");
    var table = createElement("table", "table");
    var thead = createElement("thead");
    var headerRow = createElement("tr");
    ["Data/Hora", "Rota", "Atributo", "Valor", "Tag"].forEach(function (label) {
      headerRow.appendChild(createElement("th", "", label));
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    var tbody = createElement("tbody");
    for (var i = 0; i < items.length; i += 1) {
      var item = items[i];
      var row = createElement("tr");
      row.appendChild(createElement("td", "", item.timestamp_display || "-"));
      row.appendChild(createElement("td", "", item.prefixo || "-"));
      row.appendChild(createElement("td", "", item.atributo || "-"));
      row.appendChild(createElement("td", "", String(item.valor_display == null ? "-" : item.valor_display)));
      var tagCell = createElement("td");
      tagCell.appendChild(createElement("code", "", item.tag || "-"));
      row.appendChild(tagCell);
      tbody.appendChild(row);
    }
    table.appendChild(tbody);
    wrapper.appendChild(table);

    var fragment = document.createDocumentFragment();
    fragment.appendChild(wrapper);

    if ((page.num_pages || 0) > 1) {
      var pagination = createElement("div", "events-pagination");
      pagination.style.display = "flex";
      pagination.style.gap = "8px";
      pagination.style.alignItems = "center";
      pagination.style.marginTop = "10px";

      var prevButton = createElement("button", "btn btn-ghost", "Anterior");
      prevButton.type = "button";
      if (page.has_previous && page.previous_page) {
        prevButton.dataset.eventsPage = String(page.previous_page);
      } else {
        prevButton.disabled = true;
      }
      pagination.appendChild(prevButton);

      pagination.appendChild(createElement("span", "panel-tag", "Pagina " + (page.number || 1) + " de " + (page.num_pages || 1)));

      var nextButton = createElement("button", "btn btn-ghost", "Proxima");
      nextButton.type = "button";
      if (page.has_next && page.next_page) {
        nextButton.dataset.eventsPage = String(page.next_page);
      } else {
        nextButton.disabled = true;
      }
      pagination.appendChild(nextButton);
      fragment.appendChild(pagination);
    }

    els.eventsContainer.replaceChildren(fragment);
  }

  function renderDashboard() {
    renderLiveBadge();
    renderLifebit();
    renderDayNavigation();
    renderTimeline();
    updateTimelineNote();
    renderEvents();
    renderCards();
    initDragDrop();
  }

  function syncBrowserUrl() {
    var params = new URLSearchParams(window.location.search);
    if (state.selected_day) {
      params.set("dia", state.selected_day);
    } else {
      params.delete("dia");
    }
    if (state.selected_at_iso || state.selected_at) {
      params.set("at", state.selected_at_iso || state.selected_at);
    } else {
      params.delete("at");
    }
    if (state.events_page && Number(state.events_page) > 1) {
      params.set("events_page", String(state.events_page));
    } else {
      params.delete("events_page");
    }
    params.delete("partial");
    params.delete("follow_now");
    var query = params.toString();
    history.replaceState(null, "", query ? window.location.pathname + "?" + query : window.location.pathname);
  }

  function buildRequestParams(overrides) {
    var params = new URLSearchParams();
    params.set("partial", "state");

    var selectedDay = overrides.selected_day || state.selected_day;
    if (selectedDay) {
      params.set("dia", selectedDay);
    }

    var selectedAt = overrides.selected_at_iso || overrides.selected_at || state.selected_at_iso || state.selected_at;
    if (selectedAt) {
      params.set("at", selectedAt);
    }

    var eventsPage = Number(overrides.events_page || state.events_page || 1);
    if (eventsPage > 1) {
      params.set("events_page", String(eventsPage));
    }

    var followNow = typeof overrides.follow_now === "boolean" ? overrides.follow_now : !!state.follow_now;
    if (followNow) {
      params.set("follow_now", "1");
    }
    return params;
  }

  function currentDelay() {
    return pollDelays[Math.min(backoffIndex, pollDelays.length - 1)];
  }

  function scheduleNextPoll(delayMs) {
    clearPollTimer();
    if (state.config_missing || !isLiveMode()) {
      return;
    }
    pollTimer = setTimeout(function () {
      if (document.hidden) {
        scheduleNextPoll(currentDelay());
        return;
      }
      refreshState({}, { poll: true, abortPrevious: false });
    }, delayMs);
  }

  function refreshState(overrides, options) {
    var requestOverrides = overrides || {};
    var requestOptions = options || {};
    var timelineLoadingRequest = !!requestOptions.timeline_loading;
    var timelineIso = requestOptions.timeline_iso || "";

    if (inFlight && requestOptions.poll) {
      return Promise.resolve();
    }

    if (timelineLoadingRequest && timelineIso && timelineIso === pendingTimelineRequestIso) {
      return Promise.resolve();
    }

    if (activeController && requestOptions.abortPrevious !== false) {
      activeController.abort();
    }

    var controller = new AbortController();
    activeController = controller;
    inFlightCount += 1;
    inFlight = true;
    if (timelineLoadingRequest) {
      if (timelineIso) {
        pendingTimelineRequestIso = timelineIso;
      }
      beginTimelineLoading();
    }
    setSyncStatus("updating");

    var params = buildRequestParams(requestOverrides);
    var requestUrl = stateUrl + "?" + params.toString();

    return fetch(requestUrl, {
      headers: { "X-Requested-With": "XMLHttpRequest" },
      signal: controller.signal,
    })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("HTTP " + response.status);
        }
        return response.json();
      })
      .then(function (data) {
        if (!data || !data.ok) {
          throw new Error("invalid-payload");
        }
        state = Object.assign({}, state, data);
        backoffIndex = 0;
        setSyncStatus("");
        renderDashboard();
        syncBrowserUrl();
      })
      .catch(function (error) {
        if (error && error.name === "AbortError") {
          return;
        }
        backoffIndex = Math.min(backoffIndex + 1, pollDelays.length - 1);
        setSyncStatus("error");
      })
      .finally(function () {
        if (activeController === controller) {
          activeController = null;
        }
        inFlightCount = Math.max(0, inFlightCount - 1);
        inFlight = inFlightCount > 0;
        if (timelineLoadingRequest) {
          endTimelineLoading();
          if (timelineIso && pendingTimelineRequestIso === timelineIso) {
            pendingTimelineRequestIso = "";
          }
        }
        scheduleNextPoll(currentDelay());
      });
  }

  function handleDayChange(nextDay) {
    if (!nextDay) {
      return;
    }
    if (nextDay === state.selected_day) {
      return;
    }
    clearTimelineCommitTimer();
    lastTimelineRequestedIso = null;
    enterHistoricalMode();
    refreshState(
      { selected_day: nextDay, selected_at_iso: "", events_page: 1, follow_now: false },
      { timeline_loading: true, abortPrevious: true }
    );
  }

  if (els.dayForm) {
    els.dayForm.addEventListener("submit", function (event) {
      event.preventDefault();
      if (els.daySelect && els.daySelect.value) {
        var nextDayFromSubmit = resolveClosestDay(els.daySelect.value);
        if (nextDayFromSubmit && els.daySelect.value !== nextDayFromSubmit) {
          els.daySelect.value = nextDayFromSubmit;
        }
        handleDayChange(nextDayFromSubmit);
      }
    });
  }

  if (els.dayPrevButton) {
    els.dayPrevButton.addEventListener("click", function (event) {
      if (els.dayPrevButton.disabled || !els.dayPrevButton.value) {
        return;
      }
      event.preventDefault();
      handleDayChange(els.dayPrevButton.value);
    });
  }

  if (els.dayNextButton) {
    els.dayNextButton.addEventListener("click", function (event) {
      if (els.dayNextButton.disabled || !els.dayNextButton.value) {
        return;
      }
      event.preventDefault();
      handleDayChange(els.dayNextButton.value);
    });
  }

  if (els.daySelect) {
    function shouldOpenFromDayCapsule(eventTarget) {
      if (!eventTarget) {
        return false;
      }
      if (eventTarget === els.daySelect) {
        return false;
      }
      if (eventTarget.closest && eventTarget.closest("#day-prev-button, #day-next-button")) {
        return false;
      }
      return true;
    }

    if (els.dayNavCapsule) {
      els.dayNavCapsule.addEventListener("pointerdown", function (event) {
        if (!shouldOpenFromDayCapsule(event.target)) {
          return;
        }
        openDayPicker();
      });
      els.dayNavCapsule.addEventListener("click", function (event) {
        if (!shouldOpenFromDayCapsule(event.target)) {
          return;
        }
        openDayPicker();
      });
    }

    var dayPickerField = els.daySelect.closest(".day-picker-field");
    if (dayPickerField) {
      dayPickerField.addEventListener("pointerdown", function (event) {
        if (!shouldOpenFromDayCapsule(event.target)) {
          return;
        }
        openDayPicker();
      });
      dayPickerField.addEventListener("click", function (event) {
        if (!shouldOpenFromDayCapsule(event.target)) {
          return;
        }
        openDayPicker();
      });
    }
    els.daySelect.addEventListener("change", function () {
      var nextDay = resolveClosestDay(els.daySelect.value);
      if (nextDay && els.daySelect.value !== nextDay) {
        els.daySelect.value = nextDay;
      }
      handleDayChange(nextDay);
    });
  }

  if (els.timelineRange) {
    function applyTimelinePreview() {
      var index = Number(els.timelineRange.value);
      var timeline = Array.isArray(state.timeline) ? state.timeline : [];
      var point = timeline[index];
      if (!point) {
        return null;
      }
      enterHistoricalMode();
      stopAutoplay();
      timelinePendingIso = point.iso;
      if (els.timelineReadLabel) {
        els.timelineReadLabel.textContent = point.label;
      }
      if (els.selectedAtNote) {
        els.selectedAtNote.textContent = point.label;
      }
      updateRangeProgress();
      updateTimelineTooltip(point.label);
      return point;
    }

    function commitTimelineSelection() {
      setTimelineTooltipVisible(false);
      if (!timelinePendingIso) {
        var currentPoint = pointFromCurrentRange();
        if (currentPoint && currentPoint.iso) {
          timelinePendingIso = currentPoint.iso;
        }
      }
      if (!timelinePendingIso) {
        if (!inFlight) {
          setTimelineCardsLoading(false);
        }
        return;
      }
      if (timelinePendingIso === (state.selected_at_iso || state.selected_at || "")) {
        timelinePendingIso = null;
        if (!inFlight) {
          setTimelineCardsLoading(false);
        }
        return;
      }
      var nextIso = timelinePendingIso;
      timelinePendingIso = null;
      scheduleTimelineCommit(nextIso);
    }

    els.timelineRange.addEventListener("pointerdown", function () {
      isTimelineDragging = true;
      setTimelineDraggingVisual(true);
      enterHistoricalMode();
      stopAutoplay();
      setTimelineCardsLoading(true);
      var point = pointFromCurrentRange();
      updateTimelineTooltip(point ? point.label : "");
      setTimelineTooltipVisible(true);
    });
    els.timelineRange.addEventListener("mousedown", function () {
      isTimelineDragging = true;
      setTimelineDraggingVisual(true);
      stopAutoplay();
      var point = pointFromCurrentRange();
      updateTimelineTooltip(point ? point.label : "");
      setTimelineTooltipVisible(true);
    });
    els.timelineRange.addEventListener("touchstart", function () {
      isTimelineDragging = true;
      setTimelineDraggingVisual(true);
      stopAutoplay();
      var point = pointFromCurrentRange();
      updateTimelineTooltip(point ? point.label : "");
      setTimelineTooltipVisible(true);
    }, { passive: true });
    els.timelineRange.addEventListener("input", function () {
      applyTimelinePreview();
      setTimelineTooltipVisible(true);
    });
    els.timelineRange.addEventListener("change", function () {
      if (isTimelineDragging) {
        return;
      }
      commitTimelineSelection();
    });
    els.timelineRange.addEventListener("pointerup", function () {
      commitTimelineSelection();
      isTimelineDragging = false;
      setTimelineDraggingVisual(false);
    });
    els.timelineRange.addEventListener("mouseup", function () {
      if (!isTimelineDragging) {
        return;
      }
      commitTimelineSelection();
      isTimelineDragging = false;
      setTimelineDraggingVisual(false);
    });
    els.timelineRange.addEventListener("touchend", function () {
      if (!isTimelineDragging) {
        return;
      }
      commitTimelineSelection();
      isTimelineDragging = false;
      setTimelineDraggingVisual(false);
    }, { passive: true });
    els.timelineRange.addEventListener("pointercancel", function () {
      isTimelineDragging = false;
      setTimelineDraggingVisual(false);
      clearTimelineCommitTimer();
      setTimelineCardsLoading(false);
      setTimelineTooltipVisible(false);
    });
    els.timelineRange.addEventListener("blur", function () {
      var wasDragging = isTimelineDragging;
      isTimelineDragging = false;
      setTimelineDraggingVisual(false);
      if (wasDragging) {
        clearTimelineCommitTimer();
        setTimelineCardsLoading(false);
      }
      setTimelineTooltipVisible(false);
    });

    if (els.timelineMinus15) {
      els.timelineMinus15.addEventListener("click", function () {
        stopAutoplay();
        var timeline = Array.isArray(state.timeline) ? state.timeline : [];
        if (!timeline.length) {
          return;
        }
        var baseIndex = Number(els.timelineRange.value || state.selected_index || 0);
        baseIndex = Math.max(0, Math.min(baseIndex, timeline.length - 1));
        var basePoint = timeline[baseIndex];
        var baseMs = Date.parse(basePoint.iso);
        if (!Number.isFinite(baseMs)) {
          return;
        }
        var targetMs = baseMs - 15 * 60 * 1000;
        var targetIndex = findClosestTimelineIndexByMs(targetMs);
        if (targetIndex < 0) {
          return;
        }
        els.timelineRange.value = String(targetIndex);
        updateRangeProgress();
        var point = timeline[targetIndex];
        if (els.timelineReadLabel) {
          els.timelineReadLabel.textContent = point.label;
        }
        if (els.selectedAtNote) {
          els.selectedAtNote.textContent = point.label;
        }
        requestTimelineIso(point.iso);
      });
    }

    if (els.timelinePlus15) {
      els.timelinePlus15.addEventListener("click", function () {
        stopAutoplay();
        var timeline = Array.isArray(state.timeline) ? state.timeline : [];
        if (!timeline.length) {
          return;
        }
        var baseIndex = Number(els.timelineRange.value || state.selected_index || 0);
        baseIndex = Math.max(0, Math.min(baseIndex, timeline.length - 1));
        var basePoint = timeline[Math.max(0, Math.min(baseIndex, timeline.length - 1))];
        var baseMs = Date.parse(basePoint.iso);
        if (!Number.isFinite(baseMs)) {
          return;
        }
        var targetIndex = findClosestTimelineIndexByMs(baseMs + 15 * 60 * 1000);
        if (targetIndex < 0) {
          return;
        }
        els.timelineRange.value = String(targetIndex);
        updateRangeProgress();
        var point = timeline[targetIndex];
        if (els.timelineReadLabel) {
          els.timelineReadLabel.textContent = point.label;
        }
        if (els.selectedAtNote) {
          els.selectedAtNote.textContent = point.label;
        }
        requestTimelineIso(point.iso);
      });
    }
  }

  if (els.timelineBackNow) {
    els.timelineBackNow.addEventListener("click", function (event) {
      event.preventDefault();
      goToLiveNow();
    });
  }

  if (els.timelineNowMini) {
    els.timelineNowMini.addEventListener("click", function (event) {
      event.preventDefault();
      event.stopPropagation();
      goToLiveNow();
    });
  }

  if (els.timelinePlayToggle) {
    els.timelinePlayToggle.addEventListener("click", function () {
      toggleAutoplay();
    });
  }

  if (els.eventsContainer) {
    els.eventsContainer.addEventListener("click", function (event) {
      var target = event.target;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      var button = target.closest("[data-events-page]");
      if (!button || button.disabled) {
        return;
      }
      event.preventDefault();
      var page = Number(button.dataset.eventsPage || "1");
      if (page < 1) {
        return;
      }
      enterHistoricalMode();
      refreshState({ events_page: page, follow_now: false });
    });
  }

  function getCsrfToken() {
    var match = document.cookie.match(/(?:^|; )csrftoken=([^;]+)/);
    return match ? decodeURIComponent(match[1]) : "";
  }

  function initDragDrop() {
    var grid = document.getElementById("rotas-grid");
    var status = document.getElementById("rotas-order-status");
    if (!grid || !orderUrl) {
      return;
    }
    if (grid.dataset.dragReady === "1") {
      return;
    }
    grid.dataset.dragReady = "1";

    var dragged = null;
    var moved = false;
    var suppressClick = false;

    function cardList() {
      return Array.prototype.slice.call(grid.querySelectorAll(".rota-card[data-prefix]"));
    }

    function persistOrder() {
      var prefixos = cardList()
        .map(function (el) {
          return el.getAttribute("data-prefix");
        })
        .filter(Boolean);

      fetch(orderUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Requested-With": "XMLHttpRequest",
          "X-CSRFToken": getCsrfToken(),
        },
        body: JSON.stringify({ prefixos: prefixos }),
      })
        .then(function (response) {
          return response.json();
        })
        .then(function (data) {
          if (!status) {
            return;
          }
          status.textContent = data && data.ok ? "Ordem salva." : "Falha ao salvar a ordem.";
          setTimeout(function () {
            status.textContent = "";
          }, 1800);
        })
        .catch(function () {
          if (!status) {
            return;
          }
          status.textContent = "Falha ao salvar a ordem.";
          setTimeout(function () {
            status.textContent = "";
          }, 1800);
        });
    }

    grid.addEventListener("dragstart", function (event) {
      var card = event.target.closest(".rota-card[data-prefix]");
      if (!card) {
        return;
      }
      dragged = card;
      moved = false;
      suppressClick = false;
      card.classList.add("is-dragging");
      if (event.dataTransfer) {
        event.dataTransfer.effectAllowed = "move";
      }
    });

    grid.addEventListener("dragover", function (event) {
      if (!dragged) {
        return;
      }
      event.preventDefault();
      var target = event.target.closest(".rota-card[data-prefix]");
      if (!target || target === dragged) {
        return;
      }
      var rect = target.getBoundingClientRect();
      var after = event.clientY > rect.top + rect.height / 2;
      grid.insertBefore(dragged, after ? target.nextSibling : target);
      moved = true;
    });

    grid.addEventListener("drop", function (event) {
      if (!dragged) {
        return;
      }
      event.preventDefault();
    });

    grid.addEventListener("dragend", function () {
      if (!dragged) {
        return;
      }
      dragged.classList.remove("is-dragging");
      dragged = null;
      if (moved) {
        suppressClick = true;
        persistOrder();
        setTimeout(function () {
          suppressClick = false;
        }, 120);
      }
    });

    grid.addEventListener(
      "click",
      function (event) {
        if (!suppressClick) {
          return;
        }
        event.preventDefault();
        event.stopPropagation();
      },
      true
    );
  }

  renderDashboard();
  syncPlayUi();
  syncBrowserUrl();
  scheduleNextPoll(currentDelay());
})();
