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
  var currentMonth = String(config.currentMonth || selectedMonth || "").trim();
  var defaultDate = String(config.defaultDate || "").trim();
  var categoriasOptions = Array.isArray(config.categorias) ? config.categorias : [];
  var centrosOptions = Array.isArray(config.centros) ? config.centros : [];
  var rows = utils.parseJsonScript("financeiro-compras-mes-data");
  var monthNavForm = document.getElementById("mes-navegacao-form");
  var monthNavInput = document.getElementById("month-nav-input");
  var monthNavLabelMonth = document.querySelector(".month-nav-month");
  var monthNavLabelYear = document.querySelector(".month-nav-year");
  var monthNavPrevLink = document.querySelector('.month-nav-btn[aria-label="Mes anterior"]');
  var monthNavNextLink = document.querySelector('.month-nav-btn[aria-label="Proximo mes"]');
  var monthRequestInFlight = false;

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

  function getMonthUrl(targetMonth) {
    var url = new window.URL(window.location.href);
    if (targetMonth) {
      url.searchParams.set("mes", targetMonth);
    } else {
      url.searchParams.delete("mes");
    }
    return url;
  }

  function updateSummaryChip(key, value) {
    if (!key) {
      return;
    }
    var node = document.querySelector('[data-finance-summary-key="' + key + '"]');
    if (!node) {
      return;
    }
    if (key === "total_compras") {
      node.textContent = String(value) + " compra(s) no mes";
      return;
    }
    node.textContent = value;
  }

  function applySummary(summary) {
    if (!summary || typeof summary !== "object") {
      return;
    }
    if (summary.total_mes !== undefined) {
      updateSummaryChip("total_mes", formatCurrency(summary.total_mes));
    }
    if (summary.total_pago !== undefined) {
      updateSummaryChip("total_pago", formatCurrency(summary.total_pago));
    }
    if (summary.total_pendente !== undefined) {
      updateSummaryChip("total_pendente", formatCurrency(summary.total_pendente));
    }
    if (summary.total_compras !== undefined) {
      updateSummaryChip("total_compras", String(summary.total_compras));
    }
  }

  function syncSummaryFromGrid() {
    if (!grid || typeof grid.getRows !== "function") {
      return;
    }
    var allRows = grid.getRows();
    var totalMes = 0;
    var totalPago = 0;
    var totalPendente = 0;

    (Array.isArray(allRows) ? allRows : []).forEach(function (row) {
      totalMes += toMoneyNumber(row.total_itens);
      totalPago += toMoneyNumber(row.total_pago);
      totalPendente += toMoneyNumber(row.total_pendente);
    });

    applySummary({
      total_mes: totalMes,
      total_pago: totalPago,
      total_pendente: totalPendente,
      total_compras: Array.isArray(allRows) ? allRows.length : 0,
    });
  }

  function monthLabel(isoMonth) {
    var match = /^(\d{4})-(\d{2})$/.exec(isoMonth);
    if (!match) {
      return "";
    }
    return match[2] + "/" + match[1];
  }

  function monthDisplayParts(isoMonth) {
    var match = /^(\d{4})-(\d{2})$/.exec(isoMonth);
    if (!match) {
      return { month: "", year: "" };
    }
    var dateValue = new Date(Number(match[1]), Number(match[2]) - 1, 1);
    var monthName = new Intl.DateTimeFormat("pt-BR", { month: "long" }).format(dateValue);
    return {
      month: monthName ? monthName.charAt(0).toUpperCase() + monthName.slice(1) : "",
      year: match[1],
    };
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

  function updateMonthNavigation(payload) {
    if (!payload || typeof payload !== "object") {
      return;
    }

    if (payload.selected_month !== undefined) {
      selectedMonth = String(payload.selected_month || "").trim();
      monthText = monthLabel(selectedMonth);
      config.selectedMonth = selectedMonth;
      if (monthNavInput) {
        monthNavInput.value = selectedMonth;
      }
      var monthField = root.querySelector('.datagrid-create-form [name="selected_month"]');
      if (monthField) {
        monthField.value = selectedMonth;
      }
    }

    if (payload.current_month !== undefined) {
      currentMonth = String(payload.current_month || currentMonth || "").trim();
      config.currentMonth = currentMonth;
    }

    if (payload.quick_create_date !== undefined) {
      defaultDate = String(payload.quick_create_date || "").trim();
      config.defaultDate = defaultDate;
      var dateField = root.querySelector('.datagrid-create-form [name="data"]');
      if (dateField) {
        dateField.value = defaultDate;
      }
    }

    if (payload.prev_month && monthNavPrevLink) {
      monthNavPrevLink.href = "?mes=" + encodeURIComponent(payload.prev_month);
    }
    if (payload.next_month && monthNavNextLink) {
      monthNavNextLink.href = "?mes=" + encodeURIComponent(payload.next_month);
    }

    var display = monthDisplayParts(selectedMonth);
    if (monthNavLabelMonth) {
      monthNavLabelMonth.textContent = display.month;
    }
    if (monthNavLabelYear) {
      monthNavLabelYear.textContent = display.year;
    }
  }

  function setMonthLoading(isLoading) {
    monthRequestInFlight = !!isLoading;
    root.setAttribute("aria-busy", isLoading ? "true" : "false");
    if (monthNavForm) {
      monthNavForm.setAttribute("aria-busy", isLoading ? "true" : "false");
    }
    if (monthNavInput) {
      monthNavInput.disabled = !!isLoading;
    }
    if (monthNavPrevLink) {
      monthNavPrevLink.setAttribute("aria-disabled", isLoading ? "true" : "false");
    }
    if (monthNavNextLink) {
      monthNavNextLink.setAttribute("aria-disabled", isLoading ? "true" : "false");
    }
  }

  function applyMonthPayload(payload, options) {
    if (!payload || payload.ok === false) {
      throw payload || {};
    }

    if (window.FinanceiroComprasModal && typeof window.FinanceiroComprasModal.close === "function") {
      window.FinanceiroComprasModal.close();
    }

    updateMonthNavigation(payload);
    grid.setRows(Array.isArray(payload.rows) ? payload.rows : []);
    applySummary(payload.summary || {});

    if (window.FinanceiroComprasModal && typeof window.FinanceiroComprasModal.syncRows === "function") {
      window.FinanceiroComprasModal.syncRows(Array.isArray(payload.rows) ? payload.rows : []);
    }

    if (!options || options.pushHistory !== false) {
      window.history.pushState({ mes: selectedMonth }, "", getMonthUrl(selectedMonth));
    }
  }

  function loadMonthData(targetMonth, options) {
    var normalizedMonth = String(targetMonth || currentMonth || "").trim();
    if (!normalizedMonth || normalizedMonth === selectedMonth || monthRequestInFlight) {
      return Promise.resolve();
    }

    setMonthLoading(true);
    return fetch(getMonthUrl(normalizedMonth), {
      method: "GET",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    })
      .then(parseJsonResponse)
      .then(function (payload) {
        applyMonthPayload(payload, options);
      })
      .finally(function () {
        setMonthLoading(false);
      });
  }

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
    var createMessage = createForm.querySelector(".datagrid-create-message");
    var createFieldNames = ["nome", "data", "item_nome", "item_valor", "descricao", "categoria", "centro_custo"];

    var createToggleRow = createForm.querySelector("[data-finance-create-toggle-row]");
    if (!createToggleRow) {
      createToggleRow = document.createElement("div");
      createToggleRow.className = "finance-create-toggle-row";
      createToggleRow.setAttribute("data-finance-create-toggle-row", "1");
      createForm.insertBefore(createToggleRow, createForm.firstChild);
    }

    var createToggle = createToggleRow.querySelector("[data-finance-create-toggle]");
    if (!createToggle) {
      createToggle = document.createElement("button");
      createToggle.type = "button";
      createToggle.className = "finance-create-toggle";
      createToggle.setAttribute("data-finance-create-toggle", "1");
      createToggleRow.appendChild(createToggle);
    }

    var createPanel = createForm.querySelector("[data-finance-create-panel]");
    if (!createPanel) {
      createPanel = document.createElement("div");
      createPanel.className = "finance-create-panel";
      createPanel.setAttribute("data-finance-create-panel", "1");
      createForm.appendChild(createPanel);
    }
    if (fieldsHost.parentNode !== createPanel) {
      createPanel.appendChild(fieldsHost);
    }
    if (createMessage && createMessage.parentNode !== createPanel) {
      createPanel.appendChild(createMessage);
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

    ["nome", "data", "item_nome", "item_valor"].forEach(function (fieldName) {
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
      var labelEl = fieldEl.querySelector("span");
      if (labelEl) {
        labelEl.style.display = "none";
      }
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

    function hasCreateValue() {
      return createFieldNames.some(function (fieldName) {
        var input = createForm.querySelector("[name='" + fieldName + "']");
        if (!input) {
          return false;
        }
        var value = input.value;
        if (fieldName === "data") {
          return String(value || "").trim() !== "" && String(value || "").trim() !== defaultDate;
        }
        return value !== null && value !== undefined && String(value).trim() !== "";
      });
    }

    function setCreateOpen(isOpen) {
      createPanel.classList.toggle("is-collapsed", !isOpen);
      createPanel.classList.toggle("is-open", isOpen);
      createToggle.textContent = isOpen ? "Ocultar nova compra" : "Nova compra";
      createToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
    }

    if (createToggle.dataset.initialized !== "1") {
      setCreateOpen(hasCreateValue());
      createToggle.dataset.initialized = "1";
    }

    if (createToggle.dataset.bound !== "1") {
      createToggle.dataset.bound = "1";
      createToggle.addEventListener("click", function () {
        var isOpen = createToggle.getAttribute("aria-expanded") === "true";
        setCreateOpen(!isOpen);
        if (isOpen) {
          return;
        }
        var firstBasicInput = createPanel.querySelector("[name='nome']");
        if (firstBasicInput) {
          firstBasicInput.focus();
        }
      });
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
        { name: "item_nome", label: "Item", type: "text", placeholder: "Item inicial" },
        { name: "item_valor", label: "Valor", type: "text", placeholder: "0,00" },
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
            applySummary(payload.summary);
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

  function bindMonthNavigation() {
    if (!monthNavForm || !monthNavInput) {
      return;
    }

    monthNavForm.addEventListener("click", function (event) {
      var link = event.target.closest(".month-nav-btn[href]");
      if (!link || !monthNavForm.contains(link) || monthRequestInFlight) {
        return;
      }
      event.preventDefault();
      var href = link.getAttribute("href") || "";
      var targetUrl = new window.URL(href, window.location.href);
      var targetMonth = targetUrl.searchParams.get("mes") || "";
      loadMonthData(targetMonth)
        .catch(function () {
          window.location.href = targetUrl.toString();
        });
    });

    monthNavInput.addEventListener("change", function () {
      var targetMonth = String(monthNavInput.value || "").trim();
      if (!targetMonth || targetMonth === selectedMonth || monthRequestInFlight) {
        return;
      }
      loadMonthData(targetMonth)
        .catch(function () {
          window.location.href = getMonthUrl(targetMonth).toString();
        });
    });

    window.addEventListener("popstate", function () {
      var currentUrl = new window.URL(window.location.href);
      var targetMonth = currentUrl.searchParams.get("mes") || currentMonth;
      if (!targetMonth || targetMonth === selectedMonth || monthRequestInFlight) {
        return;
      }
      loadMonthData(targetMonth, { pushHistory: false })
        .catch(function () {
          window.location.reload();
        });
    });
  }

  bindMonthNavigation();
  window.FinanceiroCadernoComprasGrid = grid;
  window.FinanceiroCadernoSyncSummary = syncSummaryFromGrid;
})();
