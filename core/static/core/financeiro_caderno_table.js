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
  var rows = utils.parseJsonScript("financeiro-compras-mes-data");

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
  });

  window.FinanceiroCadernoComprasGrid = grid;
})();
