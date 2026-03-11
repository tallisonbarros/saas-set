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
  var MODAL_GRID_ID = "radar-atividades-modal-grid";

  var modalEl = null;
  var modalBodyEl = null;
  var modalGrid = null;
  var rowObserver = null;

  var modalState = {
    isOpen: false,
    trabalhoId: "",
    detalheUrl: "",
    trabalhoNome: "",
    trabalhoDescricao: "",
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

  function payloadToRowPatch(payload) {
    return {
      nome: payload.nome || "",
      descricao: payload.descricao || "",
      status: payload.status || "",
      status_label: payload.status_label || payload.status || "",
      horas_trabalho: payload.horas_trabalho || "",
      inicio_execucao_display: payload.inicio_execucao_display || "",
      finalizada_display: payload.finalizada_display || "",
      agenda_dias: Array.isArray(payload.agenda_dias) ? payload.agenda_dias : [],
      agenda_total_dias: Number(payload.agenda_total_dias || 0),
      ordem: Number(payload.ordem || 0),
    };
  }

  function pad2(value) {
    return String(value).padStart(2, "0");
  }

  function isoFromDate(dateObj) {
    return [dateObj.getFullYear(), pad2(dateObj.getMonth() + 1), pad2(dateObj.getDate())].join("-");
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
    if (parsed.getFullYear() !== year || parsed.getMonth() + 1 !== month || parsed.getDate() !== day) {
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

  function getRowAgendaDates(row) {
    if (!row || !Array.isArray(row.agenda_dias)) {
      return [];
    }
    var cleaned = [];
    var seen = {};
    row.agenda_dias.forEach(function (item) {
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

  function buildGridShell() {
    return (
      '<div class="datagrid datagrid-modal-atividades" id="' + MODAL_GRID_ID + '">' +
      '<div class="datagrid-create" data-dg-create hidden></div>' +
      '<div class="datagrid-toolbar"><div class="datagrid-summary"><span data-dg-summary>Carregando...</span><button class="btn btn-ghost btn-compact" type="button" data-dg-clear-filters>Limpar filtros</button></div><details class="datagrid-column-picker" data-dg-column-picker><summary class="btn btn-ghost btn-compact">Colunas</summary><div class="datagrid-column-picker-body" data-dg-column-picker-body></div></details></div>' +
      '<div class="table-wrap datagrid-wrap"><table class="table datagrid-table radar-work-table radar-atividades-table" data-dg-table><thead data-dg-head></thead><tbody data-dg-body><tr><td class="muted" colspan="1">Carregando dados...</td></tr></tbody></table></div>' +
      '<div class="datagrid-pagination datagrid-pagination-modal"><label class="field datagrid-page-size"><span>Linhas por pagina</span><select data-dg-page-size><option value="10" selected>10</option></select></label><div class="datagrid-pager-actions"><button class="btn btn-ghost btn-compact" type="button" data-dg-prev-page>Anterior</button><span class="muted" data-dg-page-indicator>Pagina 1 de 1</span><button class="btn btn-ghost btn-compact" type="button" data-dg-next-page>Proxima</button></div></div>' +
      '</div>'
    );
  }

  function renderGridHeader() {
    var title = utils.escHtml(modalState.trabalhoNome || "Atividades do trabalho");
    var description = utils.escHtml(modalState.trabalhoDescricao || "Sessao de atividades do trabalho.");
    var detailUrl = utils.escHtml(modalState.detalheUrl || "#");
    return (
      '<div class="io-card-head radar-modal-grid-head">' +
      '<div class="radar-modal-grid-head-main">' +
      '<h2 class="io-title">' +
      title +
      "</h2>" +
      '<p class="muted">' +
      description +
      "</p>" +
      "</div>" +
      '<div class="radar-modal-grid-head-actions">' +
      '<a class="btn btn-ghost btn-compact" href="' +
      detailUrl +
      '" target="_self">Abrir trabalho</a>' +
      '<button type="button" class="btn btn-outline btn-compact" data-radar-work-modal-close>Fechar</button>' +
      "</div>" +
      "</div>"
    );
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

  function setModalState(message, isError) {
    if (!modalBodyEl) {
      return;
    }
    modalBodyEl.innerHTML =
      '<section class="io-card radar-table-card">' +
      renderGridHeader() +
      '<div class="radar-work-modal-state' + (isError ? ' is-error' : '') + '">' +
      utils.escHtml(message) +
      "</div>" +
      "</section>";
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
      '<section class="radar-work-modal-dialog" role="dialog" aria-modal="true" aria-label="Atividades do trabalho">' +
      '<div class="radar-work-modal-body" data-radar-work-modal-body></div>' +
      '</section>';
    document.body.appendChild(modalEl);

    modalBodyEl = modalEl.querySelector("[data-radar-work-modal-body]");
  }

  function canReorderForState(state) {
    if (!state) {
      return false;
    }
    if (!state.sortCol) {
      return true;
    }
    return state.sortCol === "ordem" && state.sortDir === "asc";
  }

  function renderStatusCell(row, ctx) {
    var badgeHtml = ctx.statusBadge(row.status, row.status_label);
    if (!canManage) {
      return badgeHtml;
    }
    return (
      '<button class="radar-status-trigger js-status-inline-trigger" type="button" data-row-id="' +
      ctx.esc(row.id) +
      '" data-current-status="' +
      ctx.esc(row.status || "") +
      '" aria-haspopup="menu" aria-expanded="false" aria-label="Alterar status">' +
      badgeHtml +
      "</button>"
    );
  }

  function renderAgendaCell(row, ctx) {
    var total = Number(row.agenda_total_dias || 0);
    var buttonClass = "radar-agenda-trigger js-agenda-inline-trigger";
    if (total > 0) {
      buttonClass += " is-active";
    }
    if (!canManage) {
      buttonClass += " is-readonly";
    }
    return (
      '<button class="' +
      buttonClass +
      '" type="button" data-row-id="' +
      ctx.esc(row.id) +
      '" aria-haspopup="dialog" aria-expanded="false" aria-label="' +
      (canManage ? "Editar agenda" : "Visualizar agenda") +
      '"><span class="radar-agenda-icon" aria-hidden="true">&#128197;</span><span class="radar-agenda-count">' +
      ctx.esc(total) +
      "</span></button>"
    );
  }

  function setupDescriptionMarquees(scopeEl) {
    if (!scopeEl) {
      return;
    }
    var marquees = scopeEl.querySelectorAll(".radar-desc-marquee");
    marquees.forEach(function (marquee) {
      var text = marquee.querySelector(".radar-desc-marquee-text");
      if (!text) {
        return;
      }
      marquee.classList.remove("is-running");
      marquee.style.removeProperty("--marquee-distance");
      marquee.style.removeProperty("--marquee-duration");
      text.style.removeProperty("transform");

      var distance = text.scrollWidth - marquee.clientWidth;
      if (distance <= 8) {
        return;
      }
      var duration = Math.max(8, Math.round(distance / 20));
      marquee.style.setProperty("--marquee-distance", distance + "px");
      marquee.style.setProperty("--marquee-duration", duration + "s");
      marquee.classList.add("is-running");
    });
  }

  function initializeGrid(rows) {
    modalGrid = window.SAASDataGrid.create({
      rootId: MODAL_GRID_ID,
      storageKey: "radar-atividades:modal:v3:" + modalState.trabalhoId,
      rows: rows,
      pageSize: 10,
      pageSizeOptions: [10],
      defaultSort: { col: "ordem", dir: "asc" },
      noRowsText: "Nenhuma atividade cadastrada.",
      summaryFormatter: function (total) {
        return total + " atividade(s) encontrada(s)";
      },
      create: canManage
        ? {
            enabled: true,
            submitIcon: true,
            submitAriaLabel: "Salvar atividade",
            submitPosition: "end",
            fields: [
              { name: "action", type: "hidden", value: "create_atividade" },
              { name: "nome", label: "Nome", type: "text", placeholder: "Nova Atividade", required: true },
              { name: "descricao", label: "Descricao", type: "text", placeholder: "Descricao resumida" },
            ],
            onSubmit: function (ctx) {
              return postFormData(modalState.detalheUrl, ctx.formData)
                .then(function (payload) {
                  if (!payload || !payload.ok || !payload.row) {
                    return { ok: false, message: "Nao foi possivel criar a atividade." };
                  }
                  return {
                    ok: true,
                    row: Object.assign({ id: String(payload.row.id || "") }, payloadToRowPatch(payload.row)),
                    message: payload.message || "Atividade criada.",
                    level: payload.level || "success",
                  };
                })
                .catch(function (err) {
                  return {
                    ok: false,
                    message: (err && err.message) || "Nao foi possivel criar a atividade.",
                    level: "error",
                  };
                });
            },
          }
        : { enabled: false },
      columns: [
        {
          key: "nome",
          label: "Nome",
          visible: true,
          fixed: true,
          flex: true,
          minWidth: 0,
          cellClass: "radar-col-nome",
          filter: { type: "text", placeholder: "Filtrar" },
          render: function (row, ctx) {
            var nome = ctx.esc(row.nome || "-");
            var descricao = ctx.esc(row.descricao || "Atividade sem descricao.");
            return nome + '<div class="radar-desc-marquee" title="' + descricao + '"><span class="radar-desc-marquee-text">' + descricao + "</span></div>";
          },
        },
        { key: "status", label: "Status", visible: true, width: 140, minWidth: 140, filter: { type: "select", options: [{ value: "EXECUTANDO", label: "Executando" }, { value: "PENDENTE", label: "Pendente" }, { value: "FINALIZADA", label: "Finalizada" }] }, render: renderStatusCell },
        { key: "agenda_total_dias", label: "Agenda", visible: true, width: 110, minWidth: 100, compareType: "number", filter: { type: "number", min: 0, step: 1, placeholder: "0" }, render: renderAgendaCell },
        { key: "ordem", label: "Ordem", visible: false, width: 100, minWidth: 90, compareType: "number", filter: { type: "number", min: 0, step: 1, placeholder: "0" } },
      ],
      rowReorder: canManage
        ? {
            enabled: true,
            isEnabled: function (state) {
              return canReorderForState(state);
            },
            onMove: function (ctx) {
              var data = new FormData();
              data.set("action", "move_atividade_to");
              data.set("atividade_id", ctx.sourceId);
              data.set("target_atividade_id", ctx.targetId);
              return postFormData(modalState.detalheUrl, data)
                .then(function (payload) {
                  if (!payload || !payload.ok || !payload.moved) {
                    setPageMessage("Nao foi possivel mover a atividade.", "warning");
                    return false;
                  }
                  return true;
                })
                .catch(function () {
                  setPageMessage("Nao foi possivel mover a atividade.", "error");
                  return false;
                });
            },
          }
        : { enabled: false },
      onAfterRender: function (api) {
        closeStatusMenu();
        refreshAgendaAnchor();
        setupDescriptionMarquees(api.root);
      },
      onResize: function (api) {
        closeStatusMenu();
        refreshAgendaAnchor();
        setupDescriptionMarquees(api.root);
      },
    });
  }

  function renderGrid(rows) {
    if (!modalBodyEl) {
      return;
    }
    modalBodyEl.innerHTML = '<section class="io-card radar-table-card">' + renderGridHeader() + buildGridShell() + '</section>';
    initializeGrid(rows);
    if (!modalGrid) {
      setModalState("Nao foi possivel montar a grade de atividades.", true);
    }
  }

  function loadAndRenderAtividades() {
    setModalState("Carregando atividades...", false);
    var requestToken = ++modalState.requestToken;
    fetch(modalState.detalheUrl, { method: "GET", headers: { "X-Requested-With": "XMLHttpRequest" } })
      .then(function (resp) {
        if (!resp.ok) {
          throw {};
        }
        return resp.text();
      })
      .then(function (htmlText) {
        if (!modalState.isOpen || requestToken !== modalState.requestToken) {
          return;
        }
        var rows = extractAtividadesFromHtml(htmlText);
        if (rows === null) {
          setModalState("Nao foi possivel ler os dados das atividades.", true);
          return;
        }
        rows = rows.map(function (row) {
          return Object.assign({ id: String(row.id || "") }, payloadToRowPatch(row));
        });
        renderGrid(rows);
      })
      .catch(function () {
        if (!modalState.isOpen || requestToken !== modalState.requestToken) {
          return;
        }
        setModalState("Nao foi possivel carregar as atividades agora.", true);
      });
  }

  function openModal(data) {
    ensureModal();
    closeStatusMenu();
    closeAgendaOverlay();

    modalState.isOpen = true;
    modalState.trabalhoId = String(data.id || "");
    modalState.detalheUrl = String(data.detalheUrl || "");
    modalState.trabalhoNome = String(data.nome || "");
    modalState.trabalhoDescricao = String(data.descricao || "");
    modalState.requestToken = (modalState.requestToken || 0) + 1;

    modalEl.hidden = false;
    modalEl.classList.add("is-open");
    document.body.classList.add("radar-work-modal-open");

    loadAndRenderAtividades();
  }

  function closeModal() {
    if (!modalEl) {
      return;
    }
    modalState.isOpen = false;
    modalState.trabalhoId = "";
    modalState.detalheUrl = "";
    modalState.trabalhoNome = "";
    modalState.trabalhoDescricao = "";
    modalState.requestToken = (modalState.requestToken || 0) + 1;
    modalGrid = null;
    closeStatusMenu();
    closeAgendaOverlay();
    modalEl.classList.remove("is-open");
    modalEl.hidden = true;
    document.body.classList.remove("radar-work-modal-open");
  }

  function syncClickableRows() {
    var body = root.querySelector("[data-dg-body]");
    if (!body) {
      return;
    }
    body.querySelectorAll("tr[data-row-id]").forEach(function (rowEl) {
      rowEl.classList.toggle("radar-trabalho-clickable-row", !!rowEl.querySelector(".radar-row-link[href]"));
    });
  }

  function getTrabalhoDataFromRow(rowEl) {
    if (!rowEl) {
      return null;
    }
    var rowId = rowEl.getAttribute("data-row-id") || "";
    var link = rowEl.querySelector(".radar-row-link[href]");
    if (!rowId || !link) {
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

  function updateAtividadeStatus(rowId, nextStatus) {
    if (!modalState.detalheUrl || !modalGrid || !rowId || !nextStatus || statusRequestInFlight) {
      return;
    }
    statusRequestInFlight = true;
    var data = new FormData();
    data.set("action", "quick_status_atividade");
    data.set("atividade_id", rowId);
    data.set("status", nextStatus);
    postFormData(modalState.detalheUrl, data)
      .then(function (payload) {
        if (!payload || !payload.ok) {
          throw payload || {};
        }
        modalGrid.updateRow(String(payload.id || rowId), payloadToRowPatch(payload));
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

    var rowId = triggerEl.getAttribute("data-row-id") || "";
    var currentStatus = triggerEl.getAttribute("data-current-status") || "";
    var menuEl = document.createElement("div");
    menuEl.className = "radar-status-menu";
    menuEl.setAttribute("role", "menu");

    STATUS_OPTIONS.forEach(function (option) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "radar-status-option";
      button.setAttribute("role", "menuitem");
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
        updateAtividadeStatus(rowId, option.value);
      });
      menuEl.appendChild(button);
    });

    document.body.appendChild(menuEl);
    activeStatusMenu = menuEl;
    positionStatusMenu(menuEl, triggerEl);
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
    var dayCount = new Date(year, month + 1, 0).getDate();
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
      var iso = isoFromDate(new Date(year, month, day));
      var classes = "radar-agenda-day";
      if (selectedMap[iso]) {
        classes += " is-selected";
      }
      if (iso === todayIso) {
        classes += " is-today";
      }
      html += '<button type="button" class="' + classes + '" data-agenda-day="' + iso + '" aria-pressed="' + (selectedMap[iso] ? "true" : "false") + '"' + (canManage ? "" : ' disabled aria-disabled="true"') + ' aria-label="' + (canManage ? "Selecionar dia" : "Dia da agenda") + ' ' + day + '">' + day + "</button>";
    }
    gridEl.innerHTML = html;
  }

  function submitAgendaSync() {
    if (!modalState.detalheUrl || !modalGrid || !activeAgendaRowId) {
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
        modalGrid.updateRow(String(payload.id || activeAgendaRowId), payloadToRowPatch(payload));
        var row = modalGrid.getRowById(String(payload.id || activeAgendaRowId));
        activeAgendaDates = getRowAgendaDates(row);
        if (activeAgendaOverlay) {
          renderAgendaOverlay();
          if (activeAgendaTrigger) {
            positionAgendaOverlay(activeAgendaOverlay, activeAgendaTrigger);
          }
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

  function queueAgendaSync() {
    if (agendaRequestTimer) {
      window.clearTimeout(agendaRequestTimer);
    }
    agendaRequestTimer = window.setTimeout(function () {
      agendaRequestTimer = null;
      submitAgendaSync();
    }, 120);
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
    if (!triggerEl || !modalGrid) {
      return;
    }
    var rowId = triggerEl.getAttribute("data-row-id") || "";
    if (!rowId) {
      return;
    }
    if (activeAgendaOverlay && activeAgendaTrigger === triggerEl) {
      closeAgendaOverlay();
      return;
    }

    closeAgendaOverlay();
    closeStatusMenu();

    var row = modalGrid.getRowById(rowId);
    if (!row) {
      setPageMessage("Atividade nao encontrada.", "warning");
      return;
    }

    activeAgendaTrigger = triggerEl;
    activeAgendaTrigger.setAttribute("aria-expanded", "true");
    activeAgendaRowId = rowId;
    activeAgendaDates = getRowAgendaDates(row);
    activeAgendaMonth = monthStart(activeAgendaDates.length ? parseIsoDate(activeAgendaDates[0]) || new Date() : new Date());

    var overlayEl = document.createElement("div");
    overlayEl.className = "radar-agenda-overlay" + (canManage ? "" : " is-readonly");
    overlayEl.setAttribute("role", "dialog");
    overlayEl.setAttribute("aria-label", "Agenda da atividade");
    overlayEl.innerHTML =
      '<div class="radar-agenda-head"><button type="button" class="radar-agenda-nav" data-agenda-prev aria-label="Mes anterior">&lt;</button><strong data-agenda-month></strong><button type="button" class="radar-agenda-nav" data-agenda-next aria-label="Proximo mes">&gt;</button></div>' +
      '<div class="radar-agenda-grid" data-agenda-grid></div>' +
      '<div class="radar-agenda-foot"><span data-agenda-summary>0 dia(s) selecionado(s)</span>' +
      (canManage ? '<button type="button" class="radar-agenda-clear" data-agenda-clear>Limpar</button>' : '<span class="radar-agenda-readonly">Somente leitura</span>') +
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
    if (!activeAgendaOverlay || !activeAgendaRowId || !modalEl) {
      return;
    }
    var nextTrigger = modalEl.querySelector('.js-agenda-inline-trigger[data-row-id="' + activeAgendaRowId + '"]');
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

  function bindEvents() {
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
      var data = getTrabalhoDataFromRow(rowEl);
      if (!data) {
        return;
      }
      event.preventDefault();
      openModal(data);
    });

    syncClickableRows();
    var body = root.querySelector("[data-dg-body]");
    if (body && !rowObserver) {
      rowObserver = new MutationObserver(function () {
        syncClickableRows();
      });
      rowObserver.observe(body, { childList: true, subtree: true });
    }

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

  ensureModal();
  modalEl.addEventListener("click", function (event) {
    var closeTarget = event.target.closest("[data-radar-work-modal-close]");
    if (closeTarget) {
      event.preventDefault();
      closeModal();
      return;
    }
    var statusTrigger = event.target.closest(".js-status-inline-trigger");
    if (statusTrigger && modalEl.contains(statusTrigger)) {
      event.preventDefault();
      event.stopPropagation();
      openStatusMenu(statusTrigger);
      return;
    }
    var agendaTrigger = event.target.closest(".js-agenda-inline-trigger");
    if (agendaTrigger && modalEl.contains(agendaTrigger)) {
      event.preventDefault();
      event.stopPropagation();
      openAgendaOverlay(agendaTrigger);
    }
  });

  bindEvents();
})();
