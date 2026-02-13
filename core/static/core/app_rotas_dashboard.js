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

  var pollDelays = [2500, 5000, 10000];
  var backoffIndex = 0;
  var pollTimer = null;
  var inFlight = false;
  var activeController = null;
  var timelinePendingIso = null;
  var timelineLoadingCards = false;

  var els = {
    dayForm: document.getElementById("day-nav-form"),
    daySelect: document.getElementById("day-select"),
    dayPrevButton: document.getElementById("day-prev-button"),
    dayNextButton: document.getElementById("day-next-button"),
    timelineRange: document.getElementById("timeline-range"),
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
    rotasSection: document.getElementById("rotas-section"),
    rotasModeContext: document.getElementById("rotas-mode-context"),
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

  function isLiveMode() {
    return !!(state.follow_now && state.showing_now && state.selected_day === state.now_day);
  }

  function clearPollTimer() {
    if (pollTimer) {
      clearTimeout(pollTimer);
      pollTimer = null;
    }
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
    renderCards();
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
      els.liveAction.textContent = live ? "Sincronizando agora" : "Ir para ao vivo";
    }
    if (els.liveHint) {
      els.liveHint.classList.toggle("is-hidden", live);
    }
    if (els.rotasSection) {
      els.rotasSection.classList.toggle("is-live", live);
      els.rotasSection.classList.toggle("is-history", !live);
    }
    if (els.rotasModeContext) {
      els.rotasModeContext.classList.toggle("is-live", live);
      els.rotasModeContext.classList.toggle("is-history", !live);
      els.rotasModeContext.textContent = live
        ? "Atualizando em tempo real"
        : "Visualizando snapshot do historico";
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
      var fragment = document.createDocumentFragment();
      var availableDays = Array.isArray(state.available_days) ? state.available_days.slice() : [];
      if (!availableDays.length && state.selected_day) {
        availableDays = [state.selected_day];
      }
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
    if (els.timelineRange) {
      var timelineLength = Array.isArray(state.timeline) ? state.timeline.length : 0;
      var max = Math.max(0, timelineLength - 1);
      els.timelineRange.max = String(max);
      els.timelineRange.disabled = max < 1;
      if (typeof state.selected_index === "number" && state.selected_index >= 0) {
        els.timelineRange.value = String(state.selected_index);
      }
    }

    if (els.timelineAtField) {
      els.timelineAtField.value = state.selected_at_iso || state.selected_at || "";
    }

    if (els.timelineReadLabel) {
      els.timelineReadLabel.textContent = state.selected_at_label || "-";
    }

    if (els.globalLigadaTrack && state.global_ligada_gradient) {
      els.globalLigadaTrack.style.background = state.global_ligada_gradient;
    }

    if (els.timelineBackNow) {
      if (state.now_day && state.now_at_iso) {
        els.timelineBackNow.setAttribute("href", "?dia=" + state.now_day + "&at=" + encodeURIComponent(state.now_at_iso));
      }
    }
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

  function ensureCommOverlay(card) {
    var overlay = card.querySelector(".rota-comm-overlay");
    if (!overlay) {
      overlay = createElement("div", "rota-comm-overlay");
      overlay.setAttribute("aria-hidden", "true");
      overlay.appendChild(createElement("span", "rota-comm-icon", "X"));
      var lastSeen = createElement("span", "rota-comm-last-seen");
      lastSeen.dataset.role = "overlay-last-seen";
      overlay.appendChild(lastSeen);
      card.appendChild(overlay);
    }
    return overlay;
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
    if (!state.lifebit_connected) {
      card.classList.add("is-comm-down");
    }
    if (timelineLoadingCards) {
      card.classList.add("is-timeline-loading");
    }

    var title = card.querySelector('[data-role="title"]');
    var prefix = card.querySelector('[data-role="prefix"]');
    var main = card.querySelector('[data-role="main"]');
    var context = card.querySelector('[data-role="context"]');
    var play = card.querySelector('[data-role="play"]');
    var pause = card.querySelector('[data-role="pause"]');

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
      var overlay = ensureCommOverlay(card);
      var lastSeen = overlay.querySelector('[data-role="overlay-last-seen"]');
      if (lastSeen) {
        lastSeen.textContent = "Ultima conexao lida: " + (state.lifebit_last_seen || "-");
      }
    } else {
      var currentOverlay = card.querySelector(".rota-comm-overlay");
      if (currentOverlay) {
        currentOverlay.remove();
      }
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
    renderCards();
    renderEvents();
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

    if (inFlight && requestOptions.poll) {
      return Promise.resolve();
    }

    if (activeController && requestOptions.abortPrevious !== false) {
      activeController.abort();
    }

    var controller = new AbortController();
    activeController = controller;
    inFlight = true;
    if (timelineLoadingRequest) {
      setTimelineCardsLoading(true);
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
        inFlight = false;
        if (timelineLoadingRequest) {
          setTimelineCardsLoading(false);
        }
        scheduleNextPoll(currentDelay());
      });
  }

  function handleDayChange(nextDay) {
    if (!nextDay) {
      return;
    }
    enterHistoricalMode();
    refreshState(
      { selected_day: nextDay, selected_at_iso: "", events_page: 1, follow_now: false },
      { timeline_loading: true, abortPrevious: true }
    );
  }

  if (els.dayForm) {
    els.dayForm.addEventListener("submit", function (event) {
      event.preventDefault();
      var submitter = event.submitter;
      var nextDay = null;
      if (submitter && submitter.name === "nav_dia" && submitter.value) {
        nextDay = submitter.value;
      } else if (els.daySelect && els.daySelect.value) {
        nextDay = els.daySelect.value;
      }
      handleDayChange(nextDay);
    });
  }

  if (els.daySelect) {
    els.daySelect.addEventListener("change", function () {
      handleDayChange(els.daySelect.value);
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
      timelinePendingIso = point.iso;
      if (els.timelineReadLabel) {
        els.timelineReadLabel.textContent = point.label;
      }
      if (els.selectedAtNote) {
        els.selectedAtNote.textContent = point.label;
      }
      return point;
    }

    function commitTimelineSelection() {
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
      refreshState(
        { selected_at_iso: nextIso, events_page: 1, follow_now: false },
        { timeline_loading: true, abortPrevious: true }
      );
    }

    els.timelineRange.addEventListener("pointerdown", function () {
      enterHistoricalMode();
      setTimelineCardsLoading(true);
    });
    els.timelineRange.addEventListener("input", function () {
      applyTimelinePreview();
    });
    els.timelineRange.addEventListener("change", commitTimelineSelection);
    els.timelineRange.addEventListener("pointerup", commitTimelineSelection);
    els.timelineRange.addEventListener("pointercancel", function () {
      setTimelineCardsLoading(false);
    });
    els.timelineRange.addEventListener("blur", function () {
      setTimelineCardsLoading(false);
    });
  }

  if (els.timelineBackNow) {
    els.timelineBackNow.addEventListener("click", function (event) {
      event.preventDefault();
      state.follow_now = true;
      refreshState(
        {
          selected_day: state.now_day || state.selected_day,
          selected_at_iso: state.now_at_iso || state.selected_at_iso,
          events_page: 1,
          follow_now: true,
        },
        { timeline_loading: true, abortPrevious: true }
      );
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
  syncBrowserUrl();
  scheduleNextPoll(currentDelay());
})();
