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
  var contratosOptions = Array.isArray(tableConfig.contratos) ? tableConfig.contratos : [];
  var classificacoesOptions = Array.isArray(tableConfig.classificacoes) ? tableConfig.classificacoes : [];
  var colaboradoresOptions = Array.isArray(tableConfig.colaboradores) ? tableConfig.colaboradores : [];
  var grid = null;

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

  function renderStatusCell(row, ctx) {
    return ctx.statusBadge(row.status, row.status_label);
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
    defaultSort: { col: "ultimo_status_evento_em", dir: "desc" },
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
            { name: "nome", label: "Nome", type: "text", placeholder: "Novo trabalho", required: true },
            { name: "data_registro", label: "Data", type: "date", value: defaultDate },
            { name: "descricao", label: "Descricao", type: "textarea", placeholder: "Descricao resumida", wide: true },
            { name: "setor", label: "Setor", type: "text", placeholder: "Utilidades" },
            { name: "solicitante", label: "Solicitante", type: "text", placeholder: "Supervisor" },
            { name: "responsavel", label: "Responsavel", type: "text", placeholder: "Equipe tecnica" },
            { name: "horas_dia", label: "Horas/dia", type: "number", min: 0.25, step: 0.25, placeholder: "8.00" },
            {
              name: "colaborador_ids",
              label: "Colaboradores",
              type: "select",
              options: colaboradoresOptions,
              multiple: true,
              size: 5,
              wide: true,
            },
            {
              name: "contrato",
              label: "Contrato",
              type: "select",
              options: [{ value: "", label: "Contrato" }].concat(contratosOptions),
            },
            {
              name: "classificacao",
              label: "Classificacao",
              type: "select",
              options: [{ value: "", label: "Classificacao" }].concat(classificacoesOptions),
            },
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
        key: "ultimo_status_evento_em",
        label: "Ultimo status em",
        visible: false,
        compareType: "date",
      },
      {
        key: "total_horas",
        label: "Horas",
        visible: true,
        width: 120,
        minWidth: 110,
        compareType: "number",
        filter: { type: "number", min: 0, step: 0.01, placeholder: "0" },
        render: function (row, ctx) {
          var value = row.total_horas || "0.00";
          return ctx.slotBadge(value, "h");
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
        key: "colaboradores",
        label: "Colaboradores",
        visible: false,
        width: 220,
        minWidth: 180,
        filter: { type: "text", placeholder: "Filtrar" },
      },
      {
        key: "total_colaboradores",
        label: "Equipe",
        visible: false,
        width: 120,
        minWidth: 110,
        compareType: "number",
        filter: { type: "number", min: 0, step: 1, placeholder: "0" },
        render: function (row, ctx) {
          return ctx.slotBadge(row.total_colaboradores || 0, "pessoas");
        },
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
      setupQuickCreateAdvanced(api.root);
      setupDescriptionMarquees(api.root);
    },
    onResize: function (api) {
      setupQuickCreateAdvanced(api.root);
      setupDescriptionMarquees(api.root);
    },
  });

  function setupQuickCreateAdvanced(scopeEl) {
    if (!canManage || !scopeEl) {
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

    var contratoSelect = createForm.querySelector("[name='contrato']");
    if (contratoSelect && !contratoSelect.id) {
      contratoSelect.id = "quick-contrato-select";
    }
    var classificacaoSelect = createForm.querySelector("[name='classificacao']");
    if (classificacaoSelect && !classificacaoSelect.id) {
      classificacaoSelect.id = "quick-classificacao-select";
    }

    var advancedFieldNames = [
      "descricao",
      "setor",
      "solicitante",
      "responsavel",
      "horas_dia",
      "colaborador_ids",
      "contrato",
      "classificacao",
    ];
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
      if (!fieldEl.id) {
        fieldEl.id = "radar-trabalho-quick-advanced-" + fieldName;
      }
      advancedFields.push(fieldEl);
    });
    if (!advancedFields.length) {
      return;
    }

    var basicFieldNames = ["nome", "data_registro"];
    basicFieldNames.forEach(function (fieldName) {
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
    if (!advancedPanel.id) {
      advancedPanel.id = "radar-trabalho-quick-advanced-panel";
    }
    advancedFields.forEach(function (fieldEl) {
      fieldEl.style.removeProperty("order");
      fieldEl.classList.remove("is-advanced-hidden");
      fieldEl.classList.remove("is-advanced-open");
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
    if (descricaoTextarea) {
      syncDescricaoHeight();
      if (descricaoTextarea.dataset.autogrowBound !== "1") {
        descricaoTextarea.dataset.autogrowBound = "1";
        descricaoTextarea.addEventListener("input", syncDescricaoHeight);
      }
    }

    var toggle = toggleRow.querySelector("[data-radar-create-advanced-toggle]");
    if (!toggle) {
      toggle = document.createElement("button");
      toggle.type = "button";
      toggle.className = "radar-create-advanced-toggle";
      toggle.setAttribute("data-radar-create-advanced-toggle", "1");
      toggleRow.appendChild(toggle);
    }
    toggle.setAttribute("aria-controls", advancedPanel.id);

    function hasAdvancedValue() {
      return advancedFieldNames.some(function (fieldName) {
        var input = createForm.querySelector("[name='" + fieldName + "']");
        if (!input) {
          return false;
        }
        var value = input.value;
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

    function submitQuickSaveAndCollapse() {
      createForm.dataset.collapseAdvancedOnReset = "1";
      var submitButton = createForm.querySelector("[data-dg-create-submit]");
      if (typeof createForm.requestSubmit === "function") {
        if (submitButton) {
          createForm.requestSubmit(submitButton);
        } else {
          createForm.requestSubmit();
        }
        return;
      }
      createForm.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    }

    var shouldOpen = toggle.getAttribute("aria-expanded") === "true";
    if (toggle.dataset.initialized !== "1") {
      shouldOpen = shouldOpen || hasAdvancedValue();
      toggle.dataset.initialized = "1";
    }
    setAdvancedOpen(shouldOpen);

    if (toggle.dataset.bound === "1") {
      return;
    }
    toggle.dataset.bound = "1";
    toggle.addEventListener("click", function () {
      var isCurrentlyOpen = toggle.getAttribute("aria-expanded") === "true";
      setAdvancedOpen(!isCurrentlyOpen);
      if (!isCurrentlyOpen) {
        var firstAdvancedInput = advancedPanel.querySelector("input, select, textarea");
        if (firstAdvancedInput) {
          firstAdvancedInput.focus();
        }
      }
    });

    if (createForm.dataset.advancedEnterBound === "1") {
      return;
    }
    createForm.dataset.advancedEnterBound = "1";
    advancedPanel.addEventListener("keydown", function (event) {
      if (
        event.key !== "Enter" ||
        event.altKey ||
        event.ctrlKey ||
        event.metaKey ||
        event.isComposing
      ) {
        return;
      }
      var target = event.target;
      if (!target) {
        return;
      }
      var isDescricaoTextarea = target.tagName === "TEXTAREA" && target.name === "descricao";
      if (isDescricaoTextarea && event.shiftKey) {
        window.setTimeout(syncDescricaoHeight, 0);
        return;
      }
      if (isDescricaoTextarea) {
        event.preventDefault();
        submitQuickSaveAndCollapse();
        return;
      }
      if (target.tagName === "TEXTAREA" || event.shiftKey) {
        return;
      }
      event.preventDefault();
      submitQuickSaveAndCollapse();
    });

    createForm.addEventListener("reset", function () {
      syncDescricaoHeight();
      if (createForm.dataset.collapseAdvancedOnReset !== "1") {
        return;
      }
      createForm.dataset.collapseAdvancedOnReset = "";
      window.setTimeout(function () {
        setAdvancedOpen(false);
      }, 0);
    });
    createForm.addEventListener("submit", function () {
      if (createForm.dataset.collapseAdvancedOnReset !== "1") {
        return;
      }
      window.setTimeout(function () {
        if (createForm.dataset.collapseAdvancedOnReset === "1") {
          createForm.dataset.collapseAdvancedOnReset = "";
        }
      }, 5000);
    });
  }
})();
