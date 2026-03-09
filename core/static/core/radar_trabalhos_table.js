(function () {
  if (!window.SAASDataGrid || !window.SAASDataGrid.utils) {
    return;
  }

  var utils = window.SAASDataGrid.utils;
  var root = document.getElementById("radar-trabalhos-grid");
  if (!root) {
    return;
  }
  var tableConfig = window.RadarTrabalhosTableConfig || {};
  var canManage = !!tableConfig.canManage;
  var defaultDate = tableConfig.defaultDate || "";
  var grid = null;
  var statusRequestInFlight = false;
  var activeStatusMenu = null;
  var activeStatusTrigger = null;
  var STATUS_OPTIONS = [
    { value: "EXECUTANDO", label: "Executando" },
    { value: "PENDENTE", label: "Pendente" },
    { value: "FINALIZADA", label: "Finalizada" },
  ];

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

  function setPageMessage(message, level) {
    var box = document.getElementById("cadastro-message");
    if (!box || !message) {
      return;
    }
    box.textContent = message;
    box.className = "notice notice-" + (level || "info");
    box.style.display = "block";
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

  function updateTrabalhoStatus(rowId, nextStatus) {
    if (!rowId || !nextStatus || statusRequestInFlight || !grid) {
      return;
    }
    statusRequestInFlight = true;
    var data = new FormData();
    data.set("action", "quick_status_trabalho");
    data.set("trabalho_id", rowId);
    data.set("status", nextStatus);
    postFormData(data)
      .then(function (payload) {
        if (!payload || !payload.ok) {
          throw payload || {};
        }
        grid.updateRow(payload.id || rowId, {
          status: payload.status || nextStatus,
          status_label: payload.status_label || nextStatus,
        });
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
        updateTrabalhoStatus(rowId, option.value);
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

  var rows = utils.parseJsonScript("radar-trabalhos-data");
  var radarId = root.getAttribute("data-dg-scope") || "global";

  grid = window.SAASDataGrid.create({
    rootId: "radar-trabalhos-grid",
    storageKey: "radar-trabalhos:" + radarId,
    rows: rows,
    pageSize: 20,
    pageSizeOptions: [10, 20, 50, 100],
    noRowsText: "Nenhum trabalho encontrado com os filtros atuais.",
    summaryFormatter: function (total) {
      return total + " trabalho(s) encontrado(s)";
    },
    create: canManage
        ? {
          enabled: true,
          submitIcon: true,
          submitAriaLabel: "Salvar trabalho",
          submitPosition: "end",
          fields: [
            { name: "action", type: "hidden", value: "create_trabalho" },
            { name: "nome", label: "Nome", type: "text", placeholder: "Nome do trabalho", required: true },
            { name: "data_registro", label: "Data", type: "date", value: defaultDate },
          ],
          onSubmit: function (ctx) {
            return postFormData(ctx.formData)
              .then(function (payload) {
                if (!payload || !payload.ok || !payload.row) {
                  return { ok: false, message: "Nao foi possivel criar o trabalho." };
                }
                return {
                  ok: true,
                  row: payload.row,
                  message: payload.message || "Trabalho criado.",
                  level: payload.level || "success",
                };
              })
              .catch(function (err) {
                return {
                  ok: false,
                  message: (err && err.message) || "Nao foi possivel criar o trabalho.",
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
          var descricao = ctx.esc(row.descricao || "Sem descricao.");
          var nomeNode = nome;
          if (row.detalhe_url) {
            nomeNode = '<a class="radar-row-link" href="' + ctx.esc(row.detalhe_url) + '">' + nome + "</a>";
          }
          return (
            nomeNode +
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
        width: 220,
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
        key: "classificacao",
        label: "Classificacao",
        visible: false,
        width: 160,
        minWidth: 140,
        filter: { type: "text", placeholder: "Filtrar" },
      },
      {
        key: "contrato",
        label: "Contrato",
        visible: false,
        width: 160,
        minWidth: 140,
        filter: { type: "text", placeholder: "Filtrar" },
      },
      {
        key: "data_registro",
        label: "Data registro",
        visible: true,
        width: 150,
        minWidth: 140,
        compareType: "date",
        filter: { type: "date" },
        render: function (row, ctx) {
          return ctx.esc(row.data_registro_label || "-");
        },
      },
      {
        key: "responsavel",
        label: "Responsavel",
        visible: false,
        width: 160,
        minWidth: 140,
        filter: { type: "text", placeholder: "Filtrar" },
      },
      {
        key: "setor",
        label: "Setor",
        visible: false,
        width: 160,
        minWidth: 140,
        filter: { type: "text", placeholder: "Filtrar" },
      },
      {
        key: "solicitante",
        label: "Solicitante",
        visible: false,
        width: 160,
        minWidth: 140,
        filter: { type: "text", placeholder: "Filtrar" },
      },
      {
        key: "total_atividades",
        label: "Atividades",
        visible: false,
        width: 140,
        minWidth: 140,
        compareType: "number",
        filter: { type: "number", min: 0, step: 1, placeholder: "0" },
        render: function (row, ctx) {
          return ctx.slotBadge(row.total_atividades || 0, "atividades");
        },
      },
    ],
    onAfterRender: function (api) {
      closeStatusMenu();
      setupDescriptionMarquees(api.root);
    },
    onResize: function (api) {
      setupDescriptionMarquees(api.root);
    },
  });

  if (canManage) {
    root.addEventListener("click", function (event) {
      var trigger = event.target.closest(".js-status-inline-trigger");
      if (!trigger || !root.contains(trigger)) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      openStatusMenu(trigger);
    });

    document.addEventListener("mousedown", function (event) {
      if (!activeStatusMenu) {
        return;
      }
      if (activeStatusMenu.contains(event.target)) {
        return;
      }
      if (activeStatusTrigger && activeStatusTrigger.contains(event.target)) {
        return;
      }
      closeStatusMenu();
    });

    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape") {
        closeStatusMenu();
      }
    });

    window.addEventListener("resize", closeStatusMenu);
    window.addEventListener("scroll", closeStatusMenu, true);
  }
})();
