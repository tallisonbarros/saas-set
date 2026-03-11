(function () {
  if (!window.SAASDataGrid || !window.SAASDataGrid.utils) {
    return;
  }

  var utils = window.SAASDataGrid.utils;
  var root = document.getElementById("radar-atividades-grid");
  if (!root) {
    return;
  }

  var config = window.RadarAtividadesTableConfig || {};
  var canManage = !!config.canManage;
  var rows = utils.parseJsonScript("radar-atividades-data");
  var trabalhoId = root.getAttribute("data-dg-scope") || "global";
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
  var STATUS_OPTIONS = [
    { value: "EXECUTANDO", label: "Executando" },
    { value: "PENDENTE", label: "Pendente" },
    { value: "FINALIZADA", label: "Finalizada" },
  ];
  var WEEKDAY_LABELS = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sab"];

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

  function postFormData(data) {
    var getCookie = window.RadarShared && window.RadarShared.getCookie
      ? window.RadarShared.getCookie
      : function () {
          return "";
        };
    return fetch(window.location.pathname + window.location.search, {
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
    };
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
    if (!activeAgendaRowId) {
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
    postFormData(data)
      .then(function (payload) {
        if (!payload || !payload.ok) {
          throw payload || {};
        }
        var patch = payloadToRowPatch(payload);
        grid.updateRow(payload.id || activeAgendaRowId, patch);
        activeAgendaDates = Array.isArray(patch.agenda_dias) ? patch.agenda_dias.slice() : [];
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
    if (!triggerEl) {
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

    var row = grid.getRowById(rowId);
    if (!row) {
      setPageMessage("Atividade nao encontrada.", "warning");
      return;
    }

    activeAgendaTrigger = triggerEl;
    activeAgendaTrigger.setAttribute("aria-expanded", "true");
    activeAgendaRowId = rowId;
    activeAgendaDates = getRowAgendaDates(row);

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
    if (!activeAgendaOverlay || !activeAgendaRowId) {
      return;
    }
    var nextTrigger = root.querySelector('.js-agenda-inline-trigger[data-row-id="' + activeAgendaRowId + '"]');
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

  function updateAtividadeStatus(rowId, nextStatus) {
    if (!rowId || !nextStatus || statusRequestInFlight) {
      return;
    }
    statusRequestInFlight = true;
    var data = new FormData();
    data.set("action", "quick_status_atividade");
    data.set("atividade_id", rowId);
    data.set("status", nextStatus);
    postFormData(data)
      .then(function (payload) {
        if (!payload || !payload.ok) {
          throw payload || {};
        }
        grid.updateRow(payload.id || rowId, payloadToRowPatch(payload));
      })
      .catch(function (err) {
        setPageMessage((err && err.message) || "Nao foi possivel atualizar o status.", "error");
      })
      .finally(function () {
        statusRequestInFlight = false;
      });
  }

  function openStatusMenu(triggerEl) {
    if (!triggerEl || statusRequestInFlight || !canManage) {
      return;
    }
    if (activeStatusMenu && activeStatusTrigger === triggerEl) {
      closeStatusMenu();
      return;
    }

    closeStatusMenu();
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
        updateAtividadeStatus(rowId, option.value);
      });
      menuEl.appendChild(button);
    });

    document.body.appendChild(menuEl);
    activeStatusMenu = menuEl;
    positionStatusMenu(menuEl, triggerEl);
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
      '">' +
      '<span class="radar-agenda-icon" aria-hidden="true">&#128197;</span>' +
      '<span class="radar-agenda-count">' +
      ctx.esc(total) +
      "</span>" +
      "</button>"
    );
  }

  var columns = [
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
        var label = nome;
        if (canManage) {
          label =
            '<button class="radar-row-link radar-link-btn js-editar-atividade" type="button" data-atividade-id="' +
            ctx.esc(row.id) +
            '">' +
            nome +
            "</button>";
        }
        return (
          label +
          '<div class="radar-desc-marquee" title="' +
          descricao +
          '">' +
          '<span class="radar-desc-marquee-text">' +
          descricao +
          "</span></div>"
        );
      },
    },
    {
      key: "descricao",
      label: "Descricao",
      visible: false,
      width: 260,
      minWidth: 220,
      cellClass: "radar-cell-wrap",
      filter: { type: "text", placeholder: "Filtrar" },
    },
    {
      key: "status",
      label: "Status",
      visible: true,
      width: 140,
      minWidth: 140,
      filter: {
        type: "select",
        options: [
          { value: "EXECUTANDO", label: "Executando" },
          { value: "PENDENTE", label: "Pendente" },
          { value: "FINALIZADA", label: "Finalizada" },
        ],
      },
      render: renderStatusCell,
    },
    {
      key: "agenda_total_dias",
      label: "Agenda",
      visible: true,
      width: 110,
      minWidth: 100,
      compareType: "number",
      filter: { type: "number", min: 0, step: 1, placeholder: "0" },
      render: renderAgendaCell,
    },
    {
      key: "horas_trabalho",
      label: "Horas",
      visible: true,
      width: 120,
      minWidth: 120,
      compareType: "number",
      filter: { type: "number", min: 0, step: 0.1, placeholder: "0" },
      render: function (row, ctx) {
        var value = row.horas_trabalho || "";
        return value ? ctx.slotBadge(value, "h") : "-";
      },
    },
    {
      key: "inicio_execucao_display",
      label: "Inicio",
      visible: false,
      width: 170,
      minWidth: 150,
      filter: { type: "text", placeholder: "Filtrar" },
    },
    {
      key: "finalizada_display",
      label: "Finalizacao",
      visible: false,
      width: 170,
      minWidth: 150,
      filter: { type: "text", placeholder: "Filtrar" },
    },
    {
      key: "ordem",
      label: "Ordem",
      visible: false,
      width: 100,
      minWidth: 90,
      compareType: "number",
      filter: { type: "number", min: 0, step: 1, placeholder: "0" },
    },
  ];

  var grid = window.SAASDataGrid.create({
    rootId: "radar-atividades-grid",
    storageKey: "radar-atividades:v3:" + trabalhoId,
    rows: rows,
    pageSize: 20,
    pageSizeOptions: [10, 20, 50, 100],
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
            return postFormData(ctx.formData)
              .then(function (payload) {
                if (!payload || !payload.ok || !payload.row) {
                  return { ok: false, message: "Nao foi possivel criar a atividade." };
                }
                return {
                  ok: true,
                  row: payload.row,
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
    columns: columns,
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
            return postFormData(data).then(function (payload) {
              if (!payload || !payload.ok || !payload.moved) {
                setPageMessage("Nao foi possivel mover a atividade.", "warning");
                return false;
              }
              return true;
            }).catch(function () {
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

  if (!grid) {
    return;
  }

  var editor = document.getElementById("editar-atividade");
  var updateForm = editor ? editor.querySelector("form.io-form") : null;
  var deleteForm = editor ? editor.querySelector("form#delete-atividade-form") : null;
  var cancelButton = document.getElementById("cancelar-edicao-atividade");

  function setField(form, name, value) {
    if (!form) {
      return;
    }
    var input = form.querySelector("[name='" + name + "']");
    if (input) {
      input.value = value || "";
    }
  }

  function hideEditor() {
    if (!editor) {
      return;
    }
    if (updateForm) {
      updateForm.reset();
      setField(updateForm, "atividade_id", "");
    }
    if (deleteForm) {
      setField(deleteForm, "atividade_id", "");
      var deleteButton = deleteForm.querySelector("button[type='submit']");
      if (deleteButton) {
        deleteButton.disabled = true;
      }
    }
    editor.style.display = "none";
  }

  function openEditorById(atividadeId) {
    if (!editor || !updateForm) {
      return;
    }
    var row = grid.getRowById(atividadeId);
    if (!row) {
      setPageMessage("Atividade nao encontrada.", "warning");
      return;
    }
    editor.style.display = "";
    setField(updateForm, "atividade_id", row.id);
    setField(updateForm, "nome", row.nome);
    setField(updateForm, "descricao", row.descricao);
    setField(updateForm, "status", row.status);
    setField(updateForm, "inicio_execucao_display", row.inicio_execucao_display);
    setField(updateForm, "finalizada_display", row.finalizada_display);
    if (deleteForm) {
      setField(deleteForm, "atividade_id", row.id);
      var deleteButton = deleteForm.querySelector("button[type='submit']");
      if (deleteButton) {
        deleteButton.disabled = false;
      }
    }
    editor.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  root.addEventListener("click", function (event) {
    var statusTrigger = event.target.closest(".js-status-inline-trigger");
    if (statusTrigger && root.contains(statusTrigger)) {
      event.preventDefault();
      event.stopPropagation();
      openStatusMenu(statusTrigger);
      return;
    }
    var agendaTrigger = event.target.closest(".js-agenda-inline-trigger");
    if (agendaTrigger && root.contains(agendaTrigger)) {
      event.preventDefault();
      event.stopPropagation();
      openAgendaOverlay(agendaTrigger);
      return;
    }
    var editButton = event.target.closest(".js-editar-atividade");
    if (editButton) {
      event.preventDefault();
      openEditorById(editButton.dataset.atividadeId);
    }
  });

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
    if (event.key === "Escape") {
      closeStatusMenu();
      closeAgendaOverlay();
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

  if (cancelButton) {
    cancelButton.addEventListener("click", function () {
      hideEditor();
    });
  }

  if (updateForm) {
    updateForm.addEventListener("submit", function (event) {
      event.preventDefault();
      var data = new FormData(updateForm);
      var submitButton = updateForm.querySelector("button[type='submit']");
      if (submitButton) {
        submitButton.disabled = true;
      }
      postFormData(data)
        .then(function (payload) {
          if (payload && payload.ok) {
            grid.updateRow(payload.id, payloadToRowPatch(payload));
            hideEditor();
          }
        })
        .catch(function (errPayload) {
          var message = (errPayload && errPayload.message) || "Nao foi possivel salvar a atividade.";
          setPageMessage(message, "error");
        })
        .finally(function () {
          if (submitButton) {
            submitButton.disabled = false;
          }
        });
    });
  }

  if (deleteForm) {
    deleteForm.addEventListener("submit", function (event) {
      event.preventDefault();
      if (!confirm("Excluir atividade?")) {
        return;
      }
      var data = new FormData(deleteForm);
      var submitButton = deleteForm.querySelector("button[type='submit']");
      if (submitButton) {
        submitButton.disabled = true;
      }
      postFormData(data)
        .then(function (payload) {
          if (payload && payload.ok) {
            grid.removeRow(payload.id);
            hideEditor();
          }
        })
        .catch(function () {
          setPageMessage("Nao foi possivel excluir a atividade.", "error");
        })
        .finally(function () {
          if (submitButton) {
            submitButton.disabled = false;
          }
        });
    });
  }
})();
