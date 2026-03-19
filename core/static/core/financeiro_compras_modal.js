(function () {
  if (!window.SAASDataGrid || !window.SAASDataGrid.utils) {
    return;
  }

  var utils = window.SAASDataGrid.utils;
  var root = document.getElementById("financeiro-compras-grid");
  if (!root) {
    return;
  }

  var rowsById = {};
  var modalEl = null;
  var modalBodyEl = null;
  var modalGrid = null;
  var statusRequestInFlight = false;
  var modalState = {
    isOpen: false,
    compraId: "",
    detalheUrl: "",
    compraData: null,
    requestToken: 0,
  };
  var MODAL_GRID_ID = "financeiro-itens-modal-grid";

  function syncRowsIndex(nextRows) {
    rowsById = {};
    (Array.isArray(nextRows) ? nextRows : []).forEach(function (row) {
      var rowId = String((row && row.id) || "");
      if (rowId) {
        rowsById[rowId] = row;
      }
    });
  }

  syncRowsIndex(utils.parseJsonScript("financeiro-compras-mes-data"));

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

  function parseCompraItensFromHtml(htmlText) {
    var parser = new window.DOMParser();
    var doc = parser.parseFromString(htmlText || "", "text/html");
    var dataScript = doc.getElementById("financeiro-compra-itens-data");
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

  function getCookie(name) {
    var value = "; " + document.cookie;
    var parts = value.split("; " + name + "=");
    if (parts.length === 2) {
      return parts.pop().split(";").shift();
    }
    return "";
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
    return fetch(url, {
      method: "POST",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRFToken": getCookie("csrftoken"),
      },
      body: data,
    }).then(parseJsonResponse);
  }

  function buildGridShell() {
    return (
      '<div class="datagrid datagrid-modal-atividades" id="' + MODAL_GRID_ID + '">' +
      '<div class="datagrid-create" data-dg-create hidden></div>' +
      '<div class="datagrid-toolbar"><div class="datagrid-summary"><span data-dg-summary>Carregando...</span><button class="btn btn-ghost btn-compact" type="button" data-dg-clear-filters>Limpar filtros</button></div><details class="datagrid-column-picker" data-dg-column-picker><summary class="btn btn-ghost btn-compact">Colunas</summary><div class="datagrid-column-picker-body" data-dg-column-picker-body></div></details></div>' +
      '<div class="table-wrap datagrid-wrap"><table class="table datagrid-table financeiro-compras-table" data-dg-table><thead data-dg-head></thead><tbody data-dg-body><tr><td class="muted" colspan="1">Carregando dados...</td></tr></tbody></table></div>' +
      '<div class="datagrid-pagination datagrid-pagination-modal"><label class="field datagrid-page-size"><span>Linhas por pagina</span><select data-dg-page-size><option value="10" selected>10</option></select></label><div class="datagrid-pager-actions"><button class="btn btn-ghost btn-compact" type="button" data-dg-prev-page>Anterior</button><span class="muted" data-dg-page-indicator>Pagina 1 de 1</span><button class="btn btn-ghost btn-compact" type="button" data-dg-next-page>Proxima</button></div></div>' +
      "</div>"
    );
  }

  function buildCompraSubtitle(row) {
    var parts = [];
    if (row.data_label && row.data_label !== "-") {
      parts.push("Data " + row.data_label);
    }
    if (row.status_label) {
      parts.push("Status " + row.status_label);
    }
    if (row.categoria) {
      parts.push("Categoria " + row.categoria);
    }
    if (row.centro) {
      parts.push("Centro " + row.centro);
    }
    if (!parts.length && row.descricao) {
      return row.descricao;
    }
    if (row.descricao) {
      parts.unshift(row.descricao);
    }
    return parts.join(" - ") || "Itens da compra.";
  }

  function buildCompraFooter(row, rowsCount) {
    var parts = [];
    parts.push(String(rowsCount || row.itens_count || 0) + " item(ns)");
    parts.push("Total " + formatCurrency(row.total_itens));
    parts.push("Pago " + formatCurrency(row.total_pago));
    parts.push("Pendente " + formatCurrency(row.total_pendente));
    return parts.join(" - ");
  }

  function renderGridHeader() {
    var row = modalState.compraData || {};
    var title = utils.escHtml(row.nome || row.descricao || "Itens da compra");
    var description = utils.escHtml(buildCompraSubtitle(row));
    var detailUrl = utils.escHtml(modalState.detalheUrl || "#");
    return (
      '<div class="io-card-head radar-modal-grid-head">' +
      '<div class="radar-modal-grid-head-main">' +
      '<h2 class="io-title"><a class="financeiro-modal-title-link" href="' +
      detailUrl +
      '" target="_self">' +
      title +
      "</a></h2>" +
      '<p class="muted" data-financeiro-modal-subtitle>' +
      description +
      "</p>" +
      "</div>" +
      '<div class="radar-modal-grid-head-actions financeiro-modal-head-actions">' +
      '<button type="button" class="financeiro-modal-close" data-radar-work-modal-close aria-label="Fechar modal" title="Fechar">&times;</button>' +
      "</div>" +
      "</div>"
    );
  }

  function renderGridFooter(rowsCount) {
    var row = modalState.compraData || {};
    return (
      '<footer class="radar-modal-grid-foot">' +
      '<span class="radar-modal-grid-foot-label">Resumo da compra</span>' +
      '<span class="radar-modal-grid-foot-value" data-financeiro-modal-summary>' +
      utils.escHtml(buildCompraFooter(row, rowsCount)) +
      "</span>" +
      "</footer>"
    );
  }

  function setModalState(message, isError) {
    if (!modalBodyEl) {
      return;
    }
    modalBodyEl.innerHTML =
      '<section class="io-card radar-table-card">' +
      renderGridHeader() +
      '<div class="radar-work-modal-state' + (isError ? " is-error" : "") + '">' +
      utils.escHtml(message) +
      "</div>" +
      renderGridFooter(0) +
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
      '<section class="radar-work-modal-dialog" role="dialog" aria-modal="true" aria-label="Itens da compra">' +
      '<div class="radar-work-modal-body" data-radar-work-modal-body></div>' +
      "</section>";
    document.body.appendChild(modalEl);
    modalBodyEl = modalEl.querySelector("[data-radar-work-modal-body]");
  }

  function syncModalMeta() {
    if (!modalBodyEl || !modalState.compraData) {
      return;
    }
    var subtitleEl = modalBodyEl.querySelector("[data-financeiro-modal-subtitle]");
    if (subtitleEl) {
      subtitleEl.textContent = buildCompraSubtitle(modalState.compraData);
    }
    var summaryEl = modalBodyEl.querySelector("[data-financeiro-modal-summary]");
    if (summaryEl) {
      var rowsCount = modalGrid ? modalGrid.getRows().length : (modalState.compraData.itens_count || 0);
      summaryEl.textContent = buildCompraFooter(modalState.compraData, rowsCount);
    }
  }

  function applyCompraPatch(compraPatch) {
    if (!compraPatch || !modalState.compraData) {
      return;
    }
    Object.keys(compraPatch).forEach(function (key) {
      modalState.compraData[key] = compraPatch[key];
    });
    rowsById[String(modalState.compraData.id || modalState.compraId)] = modalState.compraData;
    if (window.FinanceiroCadernoComprasGrid && typeof window.FinanceiroCadernoComprasGrid.updateRow === "function") {
      window.FinanceiroCadernoComprasGrid.updateRow(String(modalState.compraData.id || modalState.compraId), compraPatch);
    }
    if (typeof window.FinanceiroCadernoSyncSummary === "function") {
      window.FinanceiroCadernoSyncSummary();
    }
    syncModalMeta();
  }

  function updateItemStatus(rowId) {
    if (!modalState.detalheUrl || !modalGrid || !rowId || statusRequestInFlight) {
      return;
    }
    statusRequestInFlight = true;
    var data = new FormData();
    data.set("action", "toggle_item_pago");
    data.set("item_id", rowId);
    postFormData(modalState.detalheUrl, data)
      .then(function (payload) {
        if (!payload || !payload.ok || !payload.row) {
          throw payload || {};
        }
        modalGrid.updateRow(String(payload.row.id || rowId), payload.row);
        applyCompraPatch(payload.compra || {});
      })
      .catch(function () {
        // Mantem a interacao silenciosa; a tela detalhada segue disponivel.
      })
      .finally(function () {
        statusRequestInFlight = false;
      });
  }

  function initializeGrid(itemRows) {
    modalGrid = window.SAASDataGrid.create({
      rootId: MODAL_GRID_ID,
      storageKey: "financeiro-compra-itens:modal:v1:" + modalState.compraId,
      rows: itemRows,
      pageSize: 10,
      pageSizeOptions: [10],
      defaultSort: { col: "id", dir: "asc" },
      noRowsText: "Nenhum item cadastrado nesta compra.",
      summaryFormatter: function (total) {
        return total + " item(ns) encontrado(s)";
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
            return ctx.esc(row.nome || "-");
          },
        },
        {
          key: "total",
          label: "Total",
          visible: true,
          width: 130,
          minWidth: 120,
          compareType: "number",
          filter: { type: "number", min: 0, step: 0.01, placeholder: "0" },
          render: function (row, ctx) {
            return ctx.esc(formatCurrency(row.total));
          },
        },
        {
          key: "pago_status",
          label: "Status",
          visible: true,
          width: 150,
          minWidth: 140,
          filter: {
            type: "select",
            options: [
              { value: "pago", label: "Pago" },
              { value: "pendente", label: "Pendente" },
            ],
          },
          render: function (row, ctx) {
            return (
              '<button class="financeiro-item-status-trigger js-financeiro-item-status" type="button" data-row-id="' +
              ctx.esc(row.id) +
              '" aria-label="Alternar status do item">' +
              ctx.statusBadge(row.pago_status || "pendente", row.pago_label || "Pendente") +
              "</button>"
            );
          },
        },
      ],
    });
  }

  function renderGrid(itemRows) {
    if (!modalBodyEl) {
      return;
    }
    modalBodyEl.innerHTML =
      '<section class="io-card radar-table-card">' +
      renderGridHeader() +
      buildGridShell() +
      renderGridFooter(itemRows.length) +
      "</section>";
    initializeGrid(itemRows);
    if (!modalGrid) {
      setModalState("Nao foi possivel montar a grade de itens.", true);
    }
  }

  function loadAndRenderItens() {
    setModalState("Carregando itens da compra...", false);
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
        var itemRows = parseCompraItensFromHtml(htmlText);
        if (itemRows === null) {
          setModalState("Nao foi possivel ler os dados dos itens.", true);
          return;
        }
        renderGrid(itemRows);
      })
      .catch(function () {
        if (!modalState.isOpen || requestToken !== modalState.requestToken) {
          return;
        }
        setModalState("Nao foi possivel carregar os itens agora.", true);
      });
  }

  function openModal(rowData) {
    if (!rowData || !rowData.detalhe_url) {
      return;
    }
    ensureModal();
    modalState.isOpen = true;
    modalState.compraId = String(rowData.id || "");
    modalState.detalheUrl = String(rowData.detalhe_url || "");
    modalState.compraData = rowData;
    modalState.requestToken = (modalState.requestToken || 0) + 1;

    modalEl.hidden = false;
    modalEl.classList.add("is-open");
    document.body.classList.add("radar-work-modal-open");

    loadAndRenderItens();
  }

  function closeModal() {
    if (!modalEl) {
      return;
    }
    modalState.isOpen = false;
    modalState.compraId = "";
    modalState.detalheUrl = "";
    modalState.compraData = null;
    modalState.requestToken = (modalState.requestToken || 0) + 1;
    modalGrid = null;
    modalEl.classList.remove("is-open");
    modalEl.hidden = true;
    document.body.classList.remove("radar-work-modal-open");
  }

  function getCompraDataFromRow(rowEl) {
    if (!rowEl) {
      return null;
    }
    var rowId = String(rowEl.getAttribute("data-row-id") || "");
    if (window.FinanceiroCadernoComprasGrid && typeof window.FinanceiroCadernoComprasGrid.getRowById === "function") {
      var gridRow = window.FinanceiroCadernoComprasGrid.getRowById(rowId);
      if (gridRow) {
        return gridRow;
      }
    }
    return rowsById[rowId] || null;
  }

  function bindEvents() {
    root.addEventListener("click", function (event) {
      var titleLink = event.target.closest(".radar-row-link[href]");
      if (titleLink && root.contains(titleLink)) {
        return;
      }
      if (event.target.closest("button, input, select, textarea, summary, details, [role='button']")) {
        return;
      }
      var rowEl = event.target.closest("tbody tr[data-row-id]");
      if (!rowEl || !root.contains(rowEl)) {
        return;
      }
      var rowData = getCompraDataFromRow(rowEl);
      if (!rowData) {
        return;
      }
      event.preventDefault();
      openModal(rowData);
    });
  }

  ensureModal();
  modalEl.addEventListener("click", function (event) {
    var closeTarget = event.target.closest("[data-radar-work-modal-close]");
    if (closeTarget) {
      event.preventDefault();
      closeModal();
      return;
    }
    var statusTrigger = event.target.closest(".js-financeiro-item-status");
    if (statusTrigger && modalEl.contains(statusTrigger)) {
      event.preventDefault();
      updateItemStatus(statusTrigger.getAttribute("data-row-id") || "");
    }
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && modalState.isOpen) {
      closeModal();
    }
  });

  bindEvents();
  window.FinanceiroComprasModal = {
    close: closeModal,
    syncRows: syncRowsIndex,
  };
})();
