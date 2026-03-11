(function () {
  if (!window.SAASDataGrid || !window.SAASDataGrid.utils) {
    return;
  }

  var utils = window.SAASDataGrid.utils;
  var root = document.getElementById("radar-trabalhos-grid");
  if (!root) {
    return;
  }

  var config = window.RadarTrabalhosTableConfig || {};
  var canManage = !!config.canManage;
  var STATUS_OPTIONS = [
    { value: "EXECUTANDO", label: "Executando" },
    { value: "PENDENTE", label: "Pendente" },
    { value: "FINALIZADA", label: "Finalizada" },
  ];
  var WEEKDAY_LABELS = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sab"];

  var modalEl = null;
  var modalTitleEl = null;
  var modalSubtitleEl = null;
  var modalBodyEl = null;
  var modalOpenLinkEl = null;
  var modalRequestToken = 0;
  var modalRowObserver = null;

  var modalState = {
    isOpen: false,
    trabalhoId: "",
    detalheUrl: "",
    atividades: [],
  };

  var statusRequestInFlight = false;
  var agendaRequestInFlight = false;
  var agendaRequestPending = false;
  var agendaRequestTimer = null;
  var activeStatusMenu = null;
  var activeStatusTrigger = null;
  var activeAgendaOverlay = null;
  var activeAgendaTrigger = null;
  var activeAgendaRowId = "";
  var activeAgendaDates = [];
  var activeAgendaMonth = null;

  function setPageMessage(message, level) {
    var box = document.getElementById("cadastro-message");
    if (!box) {
      return;
    }
    box.textContent = message;
    box.className = "notice notice-" + (level || "info");
    box.style.display = "block";
  }

  function parseJsonResponse(resp) {
    return resp.text().then(function (text) {
      var payload = {};
      if (text) {
        try {
          payload = JSON.parse(text);
        } catch (e) {
          payload = {};
        }
      }
      if (!resp.ok) {
        throw payload;
      }
      return payload;
    });
  }

  function escapeSelectorValue(value) {
    var raw = String(value || "");
    if (window.CSS && typeof window.CSS.escape === "function") {
      return window.CSS.escape(raw);
    }
    return raw.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  }

  function postFormData(url, data) {
    var getCookie = window.RadarShared && window.RadarShared.getCookie
      ? window.RadarShared.getCookie
      : function () {
          return "";
        };
    return fetch(url, {
      method: "POST",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRFToken": getCookie("csrftoken"),
      },
      body: data,
    }).then(parseJsonResponse);
  }

  function normalizeAtividadeRow(row) {
    return {
      id: row && row.id !== undefined && row.id !== null ? String(row.id) : "",
      nome: row && row.nome ? String(row.nome) : "",
      descricao: row && row.descricao ? String(row.descricao) : "",
      status: row && row.status ? String(row.status) : "PENDENTE",
      status_label: row && row.status_label ? String(row.status_label) : "Pendente",
      horas_trabalho: row && row.horas_trabalho ? String(row.horas_trabalho) : "",
      agenda_dias: Array.isArray(row && row.agenda_dias)
        ? row.agenda_dias.map(function (item) {
            return String(item || "").trim();
          }).filter(Boolean)
        : [],
      agenda_total_dias: Number(row && row.agenda_total_dias ? row.agenda_total_dias : 0),
      ordem: Number(row && row.ordem ? row.ordem : 0),
    };
  }

  function normalizeAtividadeRows(rows) {
    return (Array.isArray(rows) ? rows : [])
      .map(normalizeAtividadeRow)
      .sort(function (a, b) {
        if (a.ordem === b.ordem) {
          return a.id.localeCompare(b.id);
        }
        return a.ordem - b.ordem;
      });
  }

  function getAtividadeById(atividadeId) {
    var key = String(atividadeId || "");
    return modalState.atividades.find(function (row) {
      return String(row.id) === key;
    });
  }

  function updateAtividadeInState(payload) {
    var normalized = normalizeAtividadeRow(payload || {});
    if (!normalized.id) {
      return;
    }
    var idx = -1;
    for (var i = 0; i < modalState.atividades.length; i += 1) {
      if (String(modalState.atividades[i].id) === normalized.id) {
        idx = i;
        break;
      }
    }
    if (idx === -1) {
      return;
    }
    modalState.atividades[idx] = normalized;
    modalState.atividades.sort(function (a, b) {
      if (a.ordem === b.ordem) {
        return a.id.localeCompare(b.id);
      }
      return a.ordem - b.ordem;
    });
  }

  function ensureModal() {
    if (modalEl) {
      return;
    }
    modalEl = document.createElement("div");
    modalEl.className = "radar-work-modal";
    modalEl.setAttribute("hidden", "hidden");
    modalEl.innerHTML =
      '<div class="radar-work-modal-backdrop" data-radar-work-modal-close></div>' +
      '<section class="radar-work-modal-dialog" role="dialog" aria-modal="true" aria-labelledby="radar-work-modal-title">' +
      '<header class="radar-work-modal-head">' +
      '<div class="radar-work-modal-heading">' +
      '<h2 id="radar-work-modal-title">Atividades do trabalho</h2>' +
      '<p class="muted" data-radar-work-modal-subtitle></p>' +
      "</div>" +
      '<div class="radar-work-modal-actions">' +
      '<a class="btn btn-ghost btn-compact" href="#" target="_self" data-radar-work-modal-open-detail>Abrir trabalho</a>' +
      '<button type="button" class="btn btn-outline btn-compact" data-radar-work-modal-close>Fechar</button>' +
      "</div>" +
      "</header>" +
      '<div class="radar-work-modal-body" data-radar-work-modal-body></div>' +
      "</section>";
    document.body.appendChild(modalEl);

    modalTitleEl = modalEl.querySelector("#radar-work-modal-title");
    modalSubtitleEl = modalEl.querySelector("[data-radar-work-modal-subtitle]");
    modalBodyEl = modalEl.querySelector("[data-radar-work-modal-body]");
    modalOpenLinkEl = modalEl.querySelector("[data-radar-work-modal-open-detail]");

    modalEl.addEventListener("click", function (event) {
      var closeTarget = event.target.closest("[data-radar-work-modal-close]");
      if (closeTarget) {
        event.preventDefault();
        closeModal();
        return;
      }
      var statusTrigger = event.target.closest(".js-work-modal-status-trigger");
      if (statusTrigger && modalBodyEl && modalBodyEl.contains(statusTrigger)) {
        event.preventDefault();
        event.stopPropagation();
        openStatusMenu(statusTrigger);
        return;
      }
      var agendaTrigger = event.target.closest(".js-work-modal-agenda-trigger");
      if (agendaTrigger && modalBodyEl && modalBodyEl.contains(agendaTrigger)) {
        event.preventDefault();
        event.stopPropagation();
        openAgendaOverlay(agendaTrigger);
      }
    });
  }

  function setModalLoading(message) {
    if (!modalBodyEl) {
      return;
    }
    modalBodyEl.innerHTML =
      '<div class="radar-work-modal-state">' +
      utils.escHtml(message || "Carregando atividades...") +
      "</div>";
  }

  function setModalError(message) {
    if (!modalBodyEl) {
      return;
    }
    modalBodyEl.innerHTML =
      '<div class="radar-work-modal-state is-error">' +
      utils.escHtml(message || "Nao foi possivel carregar as atividades.") +
      "</div>";
  }

  function renderModalStatusCell(row) {
    var badgeHtml = utils.statusBadge(row.status, row.status_label);
    if (!canManage) {
      return badgeHtml;
    }
    return (
      '<button class="radar-status-trigger js-work-modal-status-trigger" type="button" data-atividade-id="' +
      utils.escHtml(row.id) +
      '" data-current-status="' +
      utils.escHtml(row.status || "") +
      '" aria-haspopup="menu" aria-expanded="false" aria-label="Alterar status">' +
      badgeHtml +
      "</button>"
    );
  }

  function renderModalAgendaCell(row) {
    var total = Number(row.agenda_total_dias || 0);
    var buttonClass = "radar-agenda-trigger js-work-modal-agenda-trigger";
    if (total > 0) {
      buttonClass += " is-active";
    }
    if (!canManage) {
      buttonClass += " is-readonly";
    }
    return (
      '<button class="' +
      buttonClass +
      '" type="button" data-atividade-id="' +
      utils.escHtml(row.id) +
      '" aria-haspopup="dialog" aria-expanded="false" aria-label="' +
      (canManage ? "Editar agenda" : "Visualizar agenda") +
      '">' +
      '<span class="radar-agenda-icon" aria-hidden="true">&#128197;</span>' +
      '<span class="radar-agenda-count">' +
      utils.escHtml(total) +
      "</span>" +
      "</button>"
    );
  }

  function renderModalRows() {
    if (!modalBodyEl) {
      return;
    }
    closeStatusMenu();
    if (!modalState.atividades.length) {
      modalBodyEl.innerHTML =
        '<div class="radar-work-modal-state">Nenhuma atividade cadastrada para este trabalho.</div>';
      refreshAgendaAnchor();
      return;
    }
    var rowsHtml = modalState.atividades
      .map(function (row) {
        var descricao = row.descricao || "Atividade sem descricao.";
        var horasNode = row.horas_trabalho
          ? utils.slotBadge(row.horas_trabalho, "h")
          : "-";
        return (
          '<tr data-atividade-id="' +
          utils.escHtml(row.id) +
          '">' +
          '<td class="radar-work-modal-col-nome">' +
          '<div class="radar-work-modal-atividade-nome">' +
          utils.escHtml(row.nome || "-") +
          "</div>" +
          '<div class="radar-work-modal-atividade-desc">' +
          utils.escHtml(descricao) +
          "</div>" +
          "</td>" +
          '<td class="radar-work-modal-col-status">' +
          renderModalStatusCell(row) +
          "</td>" +
          '<td class="radar-work-modal-col-agenda">' +
          renderModalAgendaCell(row) +
          "</td>" +
          '<td class="radar-work-modal-col-horas">' +
          horasNode +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
    modalBodyEl.innerHTML =
      '<div class="table-wrap radar-work-modal-table-wrap">' +
      '<table class="table radar-work-modal-table">' +
      "<thead>" +
      "<tr>" +
      "<th>Atividade</th>" +
      "<th>Status</th>" +
      "<th>Agenda</th>" +
      "<th>Horas</th>" +
      "</tr>" +
      "</thead>" +
      "<tbody>" +
      rowsHtml +
      "</tbody>" +
      "</table>" +
      "</div>";
    refreshAgendaAnchor();
  }

  function extractAtividadesFromHtml(htmlText) {
    var parser = new window.DOMParser();
    var doc = parser.parseFromString(htmlText || "", "text/html");
    var dataScript = doc.getElementById("radar-atividades-data");
    if (!dataScript) {
      return null;
    }
    try {
      var parsed = JSON.parse(dataScript.textContent || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch (e) {
      return null;
    }
  }

  function loadAtividades(trabalhoId, detalheUrl) {
    if (!detalheUrl) {
      setModalError("Trabalho sem pagina de detalhes.");
      return;
    }
    var requestToken = ++modalRequestToken;
    setModalLoading("Carregando atividades...");
    fetch(detalheUrl, {
      method: "GET",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    })
      .then(function (resp) {
        if (!resp.ok) {
          throw {};
        }
        return resp.text();
      })
      .then(function (htmlText) {
        if (requestToken !== modalRequestToken) {
          return;
        }
        if (!modalState.isOpen || String(modalState.trabalhoId) !== String(trabalhoId)) {
          return;
        }
        var rows = extractAtividadesFromHtml(htmlText);
        if (rows === null) {
          setModalError("Nao foi possivel ler os dados das atividades deste trabalho.");
          return;
        }
        modalState.atividades = normalizeAtividadeRows(rows);
        renderModalRows();
      })
      .catch(function () {
        if (requestToken !== modalRequestToken) {
          return;
        }
        setModalError("Nao foi possivel carregar as atividades agora.");
      });
  }

  function openModal(trabalhoData) {
    if (!trabalhoData || !trabalhoData.detalheUrl) {
      return;
    }
    ensureModal();
    closeStatusMenu();
    closeAgendaOverlay();

    modalState.isOpen = true;
    modalState.trabalhoId = String(trabalhoData.id || "");
    modalState.detalheUrl = String(trabalhoData.detalheUrl || "");
    modalState.atividades = [];

    if (modalTitleEl) {
      modalTitleEl.textContent = trabalhoData.nome || "Atividades do trabalho";
    }
    if (modalSubtitleEl) {
      modalSubtitleEl.textContent = trabalhoData.descricao
        ? trabalhoData.descricao
        : "Use atalhos rapidos para status e agenda das atividades.";
    }
    if (modalOpenLinkEl) {
      modalOpenLinkEl.href = modalState.detalheUrl;
    }
    modalEl.hidden = false;
    modalEl.classList.add("is-open");
    document.body.classList.add("radar-work-modal-open");

    loadAtividades(modalState.trabalhoId, modalState.detalheUrl);
  }

  function closeModal() {
    if (!modalEl) {
      return;
    }
    modalState.isOpen = false;
    modalState.trabalhoId = "";
    modalState.detalheUrl = "";
    modalState.atividades = [];
    modalRequestToken += 1;
    closeStatusMenu();
    closeAgendaOverlay();
    modalEl.classList.remove("is-open");
    modalEl.hidden = true;
    document.body.classList.remove("radar-work-modal-open");
  }

  function getTrabalhoDataFromRow(rowEl) {
    if (!rowEl) {
      return null;
    }
    var rowId = rowEl.getAttribute("data-row-id") || "";
    if (!rowId) {
      return null;
    }
    var link = rowEl.querySelector(".radar-row-link[href]");
    if (!link) {
      return null;
    }
    var descricaoEl = rowEl.querySelector(".radar-desc-marquee-text");
    return {
      id: rowId,
      nome: (link.textContent || "").trim(),
      descricao: descricaoEl ? (descricaoEl.textContent || "").trim() : "",
      detalheUrl: link.href,
    };
  }

  function syncClickableRows() {
    var body = root.querySelector("[data-dg-body]");
    if (!body) {
      return;
    }
    var tableRows = body.querySelectorAll("tr[data-row-id]");
    tableRows.forEach(function (rowEl) {
      var hasLink = !!rowEl.querySelector(".radar-row-link[href]");
      rowEl.classList.toggle("radar-trabalho-clickable-row", hasLink);
    });
  }

  function bindRowClicks() {
    root.addEventListener("click", function (event) {
      var link = event.target.closest(".radar-row-link");
      if (link && root.contains(link)) {
        return;
      }
      var rowEl = event.target.closest("tbody tr[data-row-id]");
      if (!rowEl || !root.contains(rowEl)) {
        return;
      }
      if (event.target.closest("button, input, select, textarea, summary, details, [role='button']")) {
        return;
      }
      var trabalhoData = getTrabalhoDataFromRow(rowEl);
      if (!trabalhoData) {
        return;
      }
      event.preventDefault();
      openModal(trabalhoData);
    });

    syncClickableRows();

    var body = root.querySelector("[data-dg-body]");
    if (body && !modalRowObserver) {
      modalRowObserver = new MutationObserver(function () {
        syncClickableRows();
      });
      modalRowObserver.observe(body, {
        childList: true,
        subtree: true,
      });
    }
  }

  function closeStatusMenu() {
    if (activeStatusTrigger) {
      activeStatusTrigger.setAttribute("aria-expanded", "false");
    }
    if (activeStatusMenu && activeStatusMenu.parentNode) {
      activeStatusMenu.parentNode.removeChild(activeStatusMenu);
    }
    activeStatusMenu = null;
    activeStatusTrigger = null;
  }

  function positionStatusMenu(menuEl, triggerEl) {
    if (!menuEl || !triggerEl) {
      return;
    }
    var triggerRect = triggerEl.getBoundingClientRect();
    var menuRect = menuEl.getBoundingClientRect();
    var top = triggerRect.bottom + 6;
    var left = triggerRect.left;
    if (left + menuRect.width > window.innerWidth - 8) {
      left = window.innerWidth - menuRect.width - 8;
    }
    if (left < 8) {
      left = 8;
    }
    if (top + menuRect.height > window.innerHeight - 8) {
      top = triggerRect.top - menuRect.height - 6;
    }
    if (top < 8) {
      top = 8;
    }
    menuEl.style.left = left + "px";
    menuEl.style.top = top + "px";
    menuEl.style.minWidth = Math.max(140, Math.round(triggerRect.width)) + "px";
  }

  function updateAtividadeStatus(atividadeId, nextStatus) {
    if (!modalState.detalheUrl || !atividadeId || !nextStatus || statusRequestInFlight) {
      return;
    }
    statusRequestInFlight = true;
    var data = new FormData();
    data.set("action", "quick_status_atividade");
    data.set("atividade_id", atividadeId);
    data.set("status", nextStatus);
    postFormData(modalState.detalheUrl, data)
      .then(function (payload) {
        if (!payload || !payload.ok) {
          throw payload || {};
        }
        updateAtividadeInState(payload);
        renderModalRows();
      })
      .catch(function (err) {
        setPageMessage((err && err.message) || "Nao foi possivel atualizar o status.", "error");
      })
      .finally(function () {
        statusRequestInFlight = false;
      });
  }

  function openStatusMenu(triggerEl) {
    if (!triggerEl || !canManage || statusRequestInFlight) {
      return;
    }
    if (activeStatusMenu && activeStatusTrigger === triggerEl) {
      closeStatusMenu();
      return;
    }
    closeStatusMenu();
    closeAgendaOverlay();
    activeStatusTrigger = triggerEl;
    activeStatusTrigger.setAttribute("aria-expanded", "true");

    var atividadeId = triggerEl.getAttribute("data-atividade-id") || "";
    var currentStatus = triggerEl.getAttribute("data-current-status") || "";
    var menuEl = document.createElement("div");
    menuEl.className = "radar-status-menu";
    menuEl.setAttribute("role", "menu");

    STATUS_OPTIONS.forEach(function (option) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "radar-status-option";
      button.setAttribute("role", "menuitem");
      button.setAttribute("data-status", option.value);
      button.textContent = option.label;
      if (option.value === currentStatus) {
        button.classList.add("is-current");
      }
      button.addEventListener("click", function (event) {
        event.preventDefault();
        event.stopPropagation();
        closeStatusMenu();
        if (option.value === currentStatus) {
          return;
        }
        updateAtividadeStatus(atividadeId, option.value);
      });
      menuEl.appendChild(button);
    });

    document.body.appendChild(menuEl);
    activeStatusMenu = menuEl;
    positionStatusMenu(menuEl, triggerEl);
  }

  function pad2(value) {
    return String(value).padStart(2, "0");
  }

  function isoFromDate(dateObj) {
    return [
      dateObj.getFullYear(),
      pad2(dateObj.getMonth() + 1),
      pad2(dateObj.getDate()),
    ].join("-");
  }

  function parseIsoDate(isoText) {
    var match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(isoText || "").trim());
    if (!match) {
      return null;
    }
    var year = Number(match[1]);
    var month = Number(match[2]);
    var day = Number(match[3]);
    var parsed = new Date(year, month - 1, day);
    if (
      parsed.getFullYear() !== year ||
      parsed.getMonth() + 1 !== month ||
      parsed.getDate() !== day
    ) {
      return null;
    }
    return parsed;
  }

  function monthStart(dateObj) {
    return new Date(dateObj.getFullYear(), dateObj.getMonth(), 1);
  }

  function monthLabel(dateObj) {
    var label = dateObj.toLocaleDateString("pt-BR", { month: "long", year: "numeric" });
    return label ? label.charAt(0).toUpperCase() + label.slice(1) : "";
  }

  function getAgendaDatesFromAtividade(atividade) {
    if (!atividade || !Array.isArray(atividade.agenda_dias)) {
      return [];
    }
    var cleaned = [];
    var seen = {};
    atividade.agenda_dias.forEach(function (item) {
      var iso = String(item || "").trim();
      if (!parseIsoDate(iso) || seen[iso]) {
        return;
      }
      seen[iso] = true;
      cleaned.push(iso);
    });
    cleaned.sort();
    return cleaned;
  }

  function closeAgendaOverlay() {
    if (agendaRequestTimer) {
      window.clearTimeout(agendaRequestTimer);
      agendaRequestTimer = null;
    }
    if (activeAgendaTrigger) {
      activeAgendaTrigger.setAttribute("aria-expanded", "false");
    }
    if (activeAgendaOverlay && activeAgendaOverlay.parentNode) {
      activeAgendaOverlay.parentNode.removeChild(activeAgendaOverlay);
    }
    activeAgendaOverlay = null;
    activeAgendaTrigger = null;
    activeAgendaRowId = "";
    activeAgendaDates = [];
    activeAgendaMonth = null;
    agendaRequestPending = false;
  }

  function positionAgendaOverlay(overlayEl, triggerEl) {
    if (!overlayEl || !triggerEl) {
      return;
    }
    var triggerRect = triggerEl.getBoundingClientRect();
    var overlayRect = overlayEl.getBoundingClientRect();
    var top = triggerRect.bottom + 8;
    var left = triggerRect.left - Math.max(0, overlayRect.width - triggerRect.width);
    if (left + overlayRect.width > window.innerWidth - 8) {
      left = window.innerWidth - overlayRect.width - 8;
    }
    if (left < 8) {
      left = 8;
    }
    if (top + overlayRect.height > window.innerHeight - 8) {
      top = triggerRect.top - overlayRect.height - 8;
    }
    if (top < 8) {
      top = 8;
    }
    overlayEl.style.left = left + "px";
    overlayEl.style.top = top + "px";
  }

  function renderAgendaOverlay() {
    if (!activeAgendaOverlay || !activeAgendaMonth) {
      return;
    }
    var monthLabelEl = activeAgendaOverlay.querySelector("[data-agenda-month]");
    if (monthLabelEl) {
      monthLabelEl.textContent = monthLabel(activeAgendaMonth);
    }

    var summaryEl = activeAgendaOverlay.querySelector("[data-agenda-summary]");
    if (summaryEl) {
      summaryEl.textContent = activeAgendaDates.length + " dia(s) selecionado(s)";
    }

    var gridEl = activeAgendaOverlay.querySelector("[data-agenda-grid]");
    if (!gridEl) {
      return;
    }

    var selectedMap = {};
    activeAgendaDates.forEach(function (iso) {
      selectedMap[iso] = true;
    });

    var month = activeAgendaMonth.getMonth();
    var year = activeAgendaMonth.getFullYear();
    var firstDay = new Date(year, month, 1);
    var lastDay = new Date(year, month + 1, 0);
    var dayCount = lastDay.getDate();
    var firstWeekday = firstDay.getDay();
    var todayIso = isoFromDate(new Date());
    var html = "";

    WEEKDAY_LABELS.forEach(function (weekday) {
      html += '<div class="radar-agenda-weekday">' + utils.escHtml(weekday) + "</div>";
    });

    for (var pad = 0; pad < firstWeekday; pad += 1) {
      html += '<div class="radar-agenda-day is-blank" aria-hidden="true"></div>';
    }

    for (var day = 1; day <= dayCount; day += 1) {
      var dateObj = new Date(year, month, day);
      var iso = isoFromDate(dateObj);
      var classes = "radar-agenda-day";
      if (selectedMap[iso]) {
        classes += " is-selected";
      }
      if (iso === todayIso) {
        classes += " is-today";
      }
      html +=
        '<button type="button" class="' +
        classes +
        '" data-agenda-day="' +
        iso +
        '" aria-pressed="' +
        (selectedMap[iso] ? "true" : "false") +
        '"' +
        (canManage ? "" : ' disabled aria-disabled="true"') +
        ' aria-label="' +
        (canManage ? "Selecionar dia" : "Dia da agenda") +
        " " +
        day +
        '">' +
        day +
        "</button>";
    }

    gridEl.innerHTML = html;
  }

  function queueAgendaSync() {
    if (agendaRequestTimer) {
      window.clearTimeout(agendaRequestTimer);
    }
    agendaRequestTimer = window.setTimeout(function () {
      agendaRequestTimer = null;
      submitAgendaSync();
    }, 120);
  }

  function submitAgendaSync() {
    if (!modalState.detalheUrl || !activeAgendaRowId) {
      return;
    }
    if (agendaRequestInFlight) {
      agendaRequestPending = true;
      return;
    }
    agendaRequestInFlight = true;
    agendaRequestPending = false;
    var data = new FormData();
    data.set("action", "set_agenda_atividade");
    data.set("atividade_id", activeAgendaRowId);
    data.set("dias_execucao", JSON.stringify(activeAgendaDates.slice().sort()));
    postFormData(modalState.detalheUrl, data)
      .then(function (payload) {
        if (!payload || !payload.ok) {
          throw payload || {};
        }
        updateAtividadeInState(payload);
        var row = getAtividadeById(payload.id || activeAgendaRowId);
        activeAgendaDates = row ? getAgendaDatesFromAtividade(row) : [];
        renderModalRows();
        if (activeAgendaOverlay && activeAgendaTrigger) {
          renderAgendaOverlay();
          positionAgendaOverlay(activeAgendaOverlay, activeAgendaTrigger);
        }
      })
      .catch(function (err) {
        setPageMessage((err && err.message) || "Nao foi possivel atualizar a agenda.", "error");
      })
      .finally(function () {
        agendaRequestInFlight = false;
        if (agendaRequestPending) {
          agendaRequestPending = false;
          submitAgendaSync();
        }
      });
  }

  function toggleAgendaDate(isoDate) {
    if (!isoDate || !parseIsoDate(isoDate)) {
      return;
    }
    var current = {};
    activeAgendaDates.forEach(function (iso) {
      current[iso] = true;
    });
    if (current[isoDate]) {
      activeAgendaDates = activeAgendaDates.filter(function (iso) {
        return iso !== isoDate;
      });
    } else {
      activeAgendaDates.push(isoDate);
    }
    activeAgendaDates.sort();
    renderAgendaOverlay();
    queueAgendaSync();
  }

  function openAgendaOverlay(triggerEl) {
    if (!triggerEl || !modalBodyEl || !modalBodyEl.contains(triggerEl)) {
      return;
    }
    var atividadeId = triggerEl.getAttribute("data-atividade-id") || "";
    if (!atividadeId) {
      return;
    }
    if (activeAgendaOverlay && activeAgendaTrigger === triggerEl) {
      closeAgendaOverlay();
      return;
    }

    closeAgendaOverlay();
    closeStatusMenu();

    var atividade = getAtividadeById(atividadeId);
    if (!atividade) {
      setPageMessage("Atividade nao encontrada.", "warning");
      return;
    }

    activeAgendaTrigger = triggerEl;
    activeAgendaTrigger.setAttribute("aria-expanded", "true");
    activeAgendaRowId = atividadeId;
    activeAgendaDates = getAgendaDatesFromAtividade(atividade);

    if (activeAgendaDates.length) {
      activeAgendaMonth = monthStart(parseIsoDate(activeAgendaDates[0]) || new Date());
    } else {
      activeAgendaMonth = monthStart(new Date());
    }

    var overlayEl = document.createElement("div");
    overlayEl.className = "radar-agenda-overlay" + (canManage ? "" : " is-readonly");
    overlayEl.setAttribute("role", "dialog");
    overlayEl.setAttribute("aria-label", "Agenda da atividade");
    overlayEl.innerHTML =
      '<div class="radar-agenda-head">' +
      '<button type="button" class="radar-agenda-nav" data-agenda-prev aria-label="Mes anterior">&lt;</button>' +
      '<strong data-agenda-month></strong>' +
      '<button type="button" class="radar-agenda-nav" data-agenda-next aria-label="Proximo mes">&gt;</button>' +
      "</div>" +
      '<div class="radar-agenda-grid" data-agenda-grid></div>' +
      '<div class="radar-agenda-foot">' +
      '<span data-agenda-summary>0 dia(s) selecionado(s)</span>' +
      (canManage
        ? '<button type="button" class="radar-agenda-clear" data-agenda-clear>Limpar</button>'
        : '<span class="radar-agenda-readonly">Somente leitura</span>') +
      "</div>";

    overlayEl.addEventListener("click", function (event) {
      var prevBtn = event.target.closest("[data-agenda-prev]");
      if (prevBtn) {
        event.preventDefault();
        activeAgendaMonth = monthStart(new Date(activeAgendaMonth.getFullYear(), activeAgendaMonth.getMonth() - 1, 1));
        renderAgendaOverlay();
        positionAgendaOverlay(activeAgendaOverlay, activeAgendaTrigger);
        return;
      }
      var nextBtn = event.target.closest("[data-agenda-next]");
      if (nextBtn) {
        event.preventDefault();
        activeAgendaMonth = monthStart(new Date(activeAgendaMonth.getFullYear(), activeAgendaMonth.getMonth() + 1, 1));
        renderAgendaOverlay();
        positionAgendaOverlay(activeAgendaOverlay, activeAgendaTrigger);
        return;
      }
      var clearBtn = event.target.closest("[data-agenda-clear]");
      if (clearBtn) {
        event.preventDefault();
        if (!canManage) {
          return;
        }
        if (activeAgendaDates.length) {
          activeAgendaDates = [];
          renderAgendaOverlay();
          queueAgendaSync();
        }
        return;
      }
      var dayBtn = event.target.closest("[data-agenda-day]");
      if (dayBtn) {
        event.preventDefault();
        if (!canManage) {
          return;
        }
        toggleAgendaDate(dayBtn.getAttribute("data-agenda-day"));
      }
    });

    document.body.appendChild(overlayEl);
    activeAgendaOverlay = overlayEl;
    renderAgendaOverlay();
    positionAgendaOverlay(overlayEl, triggerEl);
  }

  function refreshAgendaAnchor() {
    if (!activeAgendaOverlay || !activeAgendaRowId || !modalBodyEl) {
      return;
    }
    var nextTrigger = modalBodyEl.querySelector(
      '.js-work-modal-agenda-trigger[data-atividade-id="' + escapeSelectorValue(activeAgendaRowId) + '"]'
    );
    if (!nextTrigger) {
      closeAgendaOverlay();
      return;
    }
    if (activeAgendaTrigger && activeAgendaTrigger !== nextTrigger) {
      activeAgendaTrigger.setAttribute("aria-expanded", "false");
    }
    activeAgendaTrigger = nextTrigger;
    activeAgendaTrigger.setAttribute("aria-expanded", "true");
    positionAgendaOverlay(activeAgendaOverlay, activeAgendaTrigger);
  }

  function bindGlobalEvents() {
    document.addEventListener("mousedown", function (event) {
      if (activeStatusMenu) {
        if (activeStatusMenu.contains(event.target)) {
          return;
        }
        if (activeStatusTrigger && activeStatusTrigger.contains(event.target)) {
          return;
        }
        closeStatusMenu();
      }
      if (activeAgendaOverlay) {
        if (activeAgendaOverlay.contains(event.target)) {
          return;
        }
        if (activeAgendaTrigger && activeAgendaTrigger.contains(event.target)) {
          return;
        }
        closeAgendaOverlay();
      }
    });

    document.addEventListener("keydown", function (event) {
      if (event.key !== "Escape") {
        return;
      }
      if (activeAgendaOverlay) {
        closeAgendaOverlay();
        return;
      }
      if (activeStatusMenu) {
        closeStatusMenu();
        return;
      }
      if (modalState.isOpen) {
        closeModal();
      }
    });

    window.addEventListener("resize", function () {
      closeStatusMenu();
      closeAgendaOverlay();
    });
    window.addEventListener(
      "scroll",
      function () {
        closeStatusMenu();
        closeAgendaOverlay();
      },
      true
    );
  }

  bindRowClicks();
  bindGlobalEvents();
})();
