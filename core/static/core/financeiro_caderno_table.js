(function () {
  if (!window.SAASDataGrid || !window.SAASDataGrid.utils) {
    return;
  }

  var utils = window.SAASDataGrid.utils;
  var root = document.getElementById("financeiro-compras-grid");
  if (!root) {
    return;
  }

  var config = window.FinanceiroCadernoComprasConfig || {};
  var cadernoId = String(config.cadernoId || root.getAttribute("data-dg-scope") || "global");
  var selectedMonth = String(config.selectedMonth || "").trim();
  var defaultDate = String(config.defaultDate || "").trim();
  var categoriasOptions = Array.isArray(config.categorias) ? config.categorias : [];
  var centrosOptions = Array.isArray(config.centros) ? config.centros : [];
  var rows = utils.parseJsonScript("financeiro-compras-mes-data");

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

  function getCookie(name) {
    var value = "; " + document.cookie;
    var parts = value.split("; " + name + "=");
    if (parts.length === 2) {
      return parts.pop().split(";").shift();
    }
    return "";
  }

  function postFormData(data) {
    return fetch(window.location.pathname + window.location.search, {
      method: "POST",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRFToken": getCookie("csrftoken"),
      },
      body: data,
    }).then(parseJsonResponse);
  }

  function updateSummaryChip(key, value) {
    if (!key) {
      return;
    }
    var node = document.querySelector('[data-finance-summary-key="' + key + '"]');
    if (!node) {
      return;
    }
    node.textContent = value;
  }

  function monthLabel(isoMonth) {
    var match = /^(\d{4})-(\d{2})$/.exec(isoMonth);
    if (!match) {
      return "";
    }
    return match[2] + "/" + match[1];
  }

  function toMoneyNumber(value) {
    var parsed = Number(value);
    if (!Number.isFinite(parsed)) {
      return 0;
    }
    return parsed;
  }

  function formatCurrency(value) {
    return toMoneyNumber(value).toLocaleString("pt-BR", {
      style: "currency",
      currency: "BRL",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  }

  var monthText = monthLabel(selectedMonth);

  function setupQuickCreateAdvanced(scopeEl) {
    if (!scopeEl) {
      return;
    }
    var createForm = scopeEl.querySelector(".datagrid-create-form");
    if (!createForm) {
      return;
    }
    var fieldsHost = createForm.querySelector(".datagrid-create-fields");
    if (!fieldsHost) {
      return;
    }
    var actions = createForm.querySelector(".datagrid-create-actions");
    if (!actions) {
      return;
    }

    var advancedFieldNames = ["descricao", "categoria", "centro_custo"];
    var advancedFields = [];

    advancedFieldNames.forEach(function (fieldName) {
      var fieldInput = createForm.querySelector("[name='" + fieldName + "']");
      if (!fieldInput) {
        return;
      }
      var fieldEl = fieldInput.closest(".datagrid-create-field");
      if (!fieldEl) {
        return;
      }
      fieldEl.classList.add("radar-create-advanced-field");
      advancedFields.push(fieldEl);
    });
    if (!advancedFields.length) {
      return;
    }

    ["nome", "data"].forEach(function (fieldName) {
      var fieldInput = createForm.querySelector("[name='" + fieldName + "']");
      if (!fieldInput) {
        return;
      }
      var fieldEl = fieldInput.closest(".datagrid-create-field");
      if (!fieldEl) {
        return;
      }
      fieldEl.classList.add("radar-create-basic-field");
      fieldEl.style.order = "10";
    });

    actions.classList.add("radar-create-main-actions");
    actions.style.order = "10";

    var toggleRow = fieldsHost.querySelector("[data-radar-create-advanced-row]");
    if (!toggleRow) {
      toggleRow = document.createElement("div");
      toggleRow.className = "radar-create-advanced-row";
      toggleRow.setAttribute("data-radar-create-advanced-row", "1");
      fieldsHost.appendChild(toggleRow);
    }
    toggleRow.style.order = "20";

    var advancedPanel = fieldsHost.querySelector("[data-radar-create-advanced-panel]");
    if (!advancedPanel) {
      advancedPanel = document.createElement("div");
      advancedPanel.className = "radar-create-advanced-panel";
      advancedPanel.setAttribute("data-radar-create-advanced-panel", "1");
      fieldsHost.appendChild(advancedPanel);
    }
    advancedPanel.style.order = "30";

    advancedFields.forEach(function (fieldEl) {
      if (fieldEl.parentNode !== advancedPanel) {
        advancedPanel.appendChild(fieldEl);
      }
    });

    var descricaoTextarea = createForm.querySelector("textarea[name='descricao']");
    function syncDescricaoHeight() {
      if (!descricaoTextarea) {
        return;
      }
      descricaoTextarea.style.height = "auto";
      descricaoTextarea.style.height = Math.max(40, descricaoTextarea.scrollHeight) + "px";
    }
    if (descricaoTextarea && descricaoTextarea.dataset.autogrowBound !== "1") {
      descricaoTextarea.dataset.autogrowBound = "1";
      syncDescricaoHeight();
      descricaoTextarea.addEventListener("input", syncDescricaoHeight);
    }

    var toggle = toggleRow.querySelector("[data-radar-create-advanced-toggle]");
    if (!toggle) {
      toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "radar-create-advanced-toggle";
      toggle.setAttribute("data-radar-create-advanced-toggle", "1");
      toggleRow.appendChild(toggle);
    }

    function hasAdvancedValue() {
      return advancedFieldNames.some(function (fieldName) {
        var input = createForm.querySelector("[name='" + fieldName + "']");
        var value = input ? input.value : "";
        return value !== null && value !== undefined && String(value).trim() !== "";
      });
    }

    function setAdvancedOpen(isOpen) {
      advancedPanel.classList.toggle("is-collapsed", !isOpen);
      advancedPanel.classList.toggle("is-open", isOpen);
      toggle.textContent = isOpen ? "Ocultar ajustes" : "Ajustes avancados";
      toggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
      if (isOpen) {
        syncDescricaoHeight();
      }
    }

    if (toggle.dataset.initialized !== "1") {
      setAdvancedOpen(hasAdvancedValue());
      toggle.dataset.initialized = "1";
    }

    if (toggle.dataset.bound === "1") {
      return;
    }
    toggle.dataset.bound = "1";
    toggle.addEventListener("click", function () {
      var isOpen = toggle.getAttribute("aria-expanded") === "true";
      setAdvancedOpen(!isOpen);
      if (!isOpen) {
        var firstAdvancedInput = advancedPanel.querySelector("input, select, textarea");
        if (firstAdvancedInput) {
          firstAdvancedInput.focus();
        }
      }
    });
  }

  var grid = window.SAASDataGrid.create({
    rootId: "financeiro-compras-grid",
    storageKey: "financeiro-caderno-compras:v2:" + cadernoId,
    rows: rows,
    pageSize: 20,
    pageSizeOptions: [10, 20, 50, 100],
    defaultSort: { col: "data", dir: "desc" },
    noRowsText: "Sem compras para o mes selecionado.",
    summaryFormatter: function (total) {
      if (monthText) {
        return total + " compra(s) em " + monthText;
      }
      return total + " compra(s) encontrada(s)";
    },
    create: {
      enabled: true,
      submitIcon: true,
      submitAriaLabel: "Salvar compra",
      submitPosition: "end",
      fields: [
        { name: "action", type: "hidden", value: "create_quick_compra" },
        { name: "selected_month", type: "hidden", value: selectedMonth },
        { name: "nome", label: "Nome", type: "text", placeholder: "Nova compra", required: true },
        { name: "data", label: "Data", type: "date", value: defaultDate, required: true },
        { name: "descricao", label: "Descricao", type: "textarea", placeholder: "Descricao resumida", wide: true },
        {
          name: "categoria",
          label: "Categoria",
          type: "select",
          options: [{ value: "", label: "Categoria" }].concat(categoriasOptions),
        },
        {
          name: "centro_custo",
          label: "Centro de custo",
          type: "select",
          options: [{ value: "", label: "Centro de custo" }].concat(centrosOptions),
        },
      ],
      onSubmit: function (ctx) {
        return postFormData(ctx.formData)
          .then(function (payload) {
            if (!payload || !payload.ok) {
              return { ok: false, message: "Nao foi possivel criar a compra." };
            }
            if (payload.summary && payload.summary.total_compras !== undefined) {
              updateSummaryChip("total_compras", String(payload.summary.total_compras));
            }
            return {
              ok: true,
              row: payload.in_selected_month ? payload.row : null,
              refresh: !payload.in_selected_month,
              message: payload.message || "Compra criada.",
              level: payload.level || "success",
              reset: true,
            };
          })
          .catch(function (err) {
            return {
              ok: false,
              message: (err && err.message) || "Nao foi possivel criar a compra.",
              level: "error",
            };
          });
      },
    },
    columns: [
      {
        key: "nome",
        label: "Nome",
        visible: true,
        fixed: true,
        flex: true,
        minWidth: 0,
        cellClass: "financeiro-col-nome",
        filter: { type: "text", placeholder: "Filtrar" },
        render: function (row, ctx) {
          var baseName = row.nome || row.descricao || "Compra sem nome";
          var nameHtml = ctx.esc(baseName);
          if (row.detalhe_url) {
            nameHtml = '<a class="radar-row-link" href="' + ctx.esc(row.detalhe_url) + '">' + nameHtml + "</a>";
          }
          if (!row.descricao) {
            return nameHtml;
          }
          return nameHtml + '<div class="muted">' + ctx.esc(row.descricao) + "</div>";
        },
      },
      {
        key: "data",
        label: "Data",
        visible: true,
        width: 120,
        minWidth: 110,
        compareType: "date",
        filter: false,
        render: function (row, ctx) {
          return ctx.esc(row.data_label || "-");
        },
      },
      {
        key: "status",
        label: "Status",
        visible: true,
        width: 140,
        minWidth: 130,
        filter: {
          type: "select",
          options: [
            { value: "pago", label: "Pago" },
            { value: "pendente", label: "Pendente" },
          ],
        },
        render: function (row, ctx) {
          return ctx.statusBadge(row.status || "pendente", row.status_label || "Pendente");
        },
      },
      {
        key: "itens_count",
        label: "Itens",
        visible: false,
        width: 100,
        minWidth: 90,
        compareType: "number",
        filter: { type: "number", min: 0, step: 1, placeholder: "0" },
        render: function (row, ctx) {
          return ctx.slotBadge(row.itens_count || 0, "itens");
        },
      },
      {
        key: "total_itens",
        label: "Total",
        visible: true,
        width: 140,
        minWidth: 130,
        compareType: "number",
        filter: { type: "number", min: 0, step: 0.01, placeholder: "0" },
        render: function (row, ctx) {
          return ctx.esc(formatCurrency(row.total_itens));
        },
      },
      {
        key: "total_pago",
        label: "Pago",
        visible: false,
        width: 140,
        minWidth: 130,
        compareType: "number",
        filter: { type: "number", min: 0, step: 0.01, placeholder: "0" },
        render: function (row, ctx) {
          return ctx.esc(formatCurrency(row.total_pago));
        },
      },
      {
        key: "total_pendente",
        label: "Pendente",
        visible: false,
        width: 150,
        minWidth: 140,
        compareType: "number",
        filter: { type: "number", min: 0, step: 0.01, placeholder: "0" },
        render: function (row, ctx) {
          return ctx.esc(formatCurrency(row.total_pendente));
        },
      },
      {
        key: "categoria",
        label: "Categoria",
        visible: false,
        width: 160,
        minWidth: 140,
        filter: { type: "text", placeholder: "Filtrar" },
      },
      {
        key: "centro",
        label: "Centro",
        visible: false,
        width: 180,
        minWidth: 150,
        filter: { type: "text", placeholder: "Filtrar" },
      },
      {
        key: "descricao",
        label: "Descricao",
        visible: false,
        width: 260,
        minWidth: 220,
        filter: { type: "text", placeholder: "Filtrar" },
      },
    ],
    onAfterRender: function (api) {
      setupQuickCreateAdvanced(api.root);
    },
    onResize: function (api) {
      setupQuickCreateAdvanced(api.root);
    },
  });

  window.FinanceiroCadernoComprasGrid = grid;
})();
