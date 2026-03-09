(function (global) {
  function byId(id) {
    return document.getElementById(id);
  }

  function safeText(value) {
    if (value === null || value === undefined) {
      return "";
    }
    return String(value);
  }

  function escHtml(value) {
    return safeText(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function normalizeText(value) {
    return safeText(value).trim().toLowerCase();
  }

  function parseJsonScript(id) {
    var el = byId(id);
    if (!el) {
      return [];
    }
    try {
      var parsed = JSON.parse(el.textContent || "[]");
      return Array.isArray(parsed) ? parsed : [];
    } catch (e) {
      return [];
    }
  }

  function statusBadge(status, label) {
    var cssSuffix = normalizeText(status).toLowerCase() || "pendente";
    return '<span class="badge badge-' + escHtml(cssSuffix) + '">' + escHtml(label || status || "-") + "</span>";
  }

  function slotBadge(value, suffix) {
    var text = safeText(value);
    if (suffix) {
      text += " " + suffix;
    }
    return '<span class="slot-badge slot-badge-compact">' + escHtml(text) + "</span>";
  }

  function decorateRows(rows) {
    return (Array.isArray(rows) ? rows : []).map(function (row, index) {
      var clone = Object.assign({}, row);
      clone._index = index;
      return clone;
    });
  }

  function buildStorageKey(root, config) {
    var baseKey = (config && config.storageKey) || root.getAttribute("data-dg-storage-key") || root.id || "global";
    return "datagrid_columns_v1:" + baseKey;
  }

  function loadVisibleColumns(storageKey, columns) {
    var defaults = columns
      .filter(function (column) {
        return column.visible !== false;
      })
      .map(function (column) {
        return column.key;
      });
    try {
      var raw = window.localStorage.getItem(storageKey);
      if (!raw) {
        return defaults;
      }
      var parsed = JSON.parse(raw);
      if (!Array.isArray(parsed) || !parsed.length) {
        return defaults;
      }
      return parsed.filter(function (key) {
        return columns.some(function (column) {
          return column.key === key;
        });
      });
    } catch (e) {
      return defaults;
    }
  }

  function saveVisibleColumns(storageKey, visibleColumns) {
    try {
      window.localStorage.setItem(storageKey, JSON.stringify(visibleColumns));
    } catch (e) {
      // Sem persistencia.
    }
  }

  function toNumber(value) {
    var parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function inferCompareType(column) {
    if (!column) {
      return "text";
    }
    if (column.compareType) {
      return column.compareType;
    }
    var filterType = column.filter && column.filter.type;
    if (filterType === "number") {
      return "number";
    }
    if (filterType === "date") {
      return "date";
    }
    return "text";
  }

  function createDataGrid(config) {
    if (!config || !config.rootId) {
      return null;
    }

    var root = byId(config.rootId);
    if (!root) {
      return null;
    }

    var tableEl = root.querySelector("[data-dg-table]");
    var headEl = root.querySelector("[data-dg-head]");
    var bodyEl = root.querySelector("[data-dg-body]");
    var summaryEl = root.querySelector("[data-dg-summary]");
    var clearButton = root.querySelector("[data-dg-clear-filters]");
    var pageSizeSelect = root.querySelector("[data-dg-page-size]");
    var prevButton = root.querySelector("[data-dg-prev-page]");
    var nextButton = root.querySelector("[data-dg-next-page]");
    var pageIndicatorEl = root.querySelector("[data-dg-page-indicator]");
    var pickerBody = root.querySelector("[data-dg-column-picker-body]");

    if (
      !tableEl ||
      !headEl ||
      !bodyEl ||
      !summaryEl ||
      !clearButton ||
      !pageSizeSelect ||
      !prevButton ||
      !nextButton ||
      !pageIndicatorEl ||
      !pickerBody
    ) {
      return null;
    }

    var columns = (config.columns || []).slice();
    if (!columns.length) {
      return null;
    }

    var rawRows = Array.isArray(config.rows) ? config.rows : parseJsonScript(config.dataScriptId);
    var rows = decorateRows(rawRows);

    var initialPageSize = Number(config.pageSize || pageSizeSelect.value || 20);
    if (!Number.isFinite(initialPageSize) || initialPageSize <= 0) {
      initialPageSize = 20;
    }

    var state = {
      filters: {},
      sortCol: (config.defaultSort && config.defaultSort.col) || "",
      sortDir: (config.defaultSort && config.defaultSort.dir) || "asc",
      page: 1,
      pageSize: initialPageSize,
    };

    var storageKey = buildStorageKey(root, config);
    var visibleColumns = loadVisibleColumns(storageKey, columns);
    var mounted = false;

    function reindexRows() {
      rows.forEach(function (row, index) {
        row._index = index;
      });
    }

    function ensureMandatoryColumns() {
      columns.forEach(function (column) {
        if (column.fixed && visibleColumns.indexOf(column.key) === -1) {
          visibleColumns.push(column.key);
        }
      });
    }

    function findColumn(columnKey) {
      return columns.find(function (column) {
        return column.key === columnKey;
      });
    }

    function isColumnVisible(columnKey) {
      return visibleColumns.indexOf(columnKey) >= 0;
    }

    function visibleColumnCount() {
      var count = columns.filter(function (column) {
        return isColumnVisible(column.key);
      }).length;
      return count > 0 ? count : 1;
    }

    function columnStyle(column) {
      if (!column) {
        return "";
      }
      if (column.flex) {
        return "min-width:0;width:auto;";
      }
      var minWidth = column.minWidth || 140;
      var width = column.width || minWidth;
      return "min-width:" + Number(minWidth) + "px;width:" + Number(width) + "px;";
    }

    function applyColumnVisibility() {
      columns.forEach(function (column) {
        var hidden = !isColumnVisible(column.key);
        var nodes = root.querySelectorAll('[data-dg-col="' + column.key + '"]');
        nodes.forEach(function (node) {
          node.classList.toggle("is-hidden", hidden);
        });
      });
    }

    function getColumnValue(row, column) {
      if (!column) {
        return "";
      }
      if (typeof column.valueGetter === "function") {
        return column.valueGetter(row, column);
      }
      return row[column.key];
    }

    function getSortValue(row, column) {
      if (!column) {
        return "";
      }
      if (typeof column.sortValueGetter === "function") {
        return column.sortValueGetter(row, column);
      }
      return getColumnValue(row, column);
    }

    function getFilterValue(row, column) {
      if (!column) {
        return "";
      }
      if (typeof column.filterValueGetter === "function") {
        return column.filterValueGetter(row, column);
      }
      return getColumnValue(row, column);
    }

    function compareRows(a, b, columnKey, direction) {
      var column = findColumn(columnKey);
      if (!column) {
        return 0;
      }
      var factor = direction === "desc" ? -1 : 1;
      var type = inferCompareType(column);
      var av = getSortValue(a, column);
      var bv = getSortValue(b, column);

      if (type === "number") {
        var an = toNumber(av);
        var bn = toNumber(bv);
        if (an === bn) {
          return (a._index - b._index) * factor;
        }
        return (an - bn) * factor;
      }

      if (type === "date") {
        var ad = safeText(av);
        var bd = safeText(bv);
        if (ad === bd) {
          return (a._index - b._index) * factor;
        }
        return ad.localeCompare(bd, "pt-BR") * factor;
      }

      var as = normalizeText(av);
      var bs = normalizeText(bv);
      if (as === bs) {
        return (a._index - b._index) * factor;
      }
      return as.localeCompare(bs, "pt-BR") * factor;
    }

    function passFilters(row) {
      var keys = Object.keys(state.filters);
      for (var i = 0; i < keys.length; i += 1) {
        var key = keys[i];
        var expected = state.filters[key];
        if (!expected) {
          continue;
        }
        var column = findColumn(key);
        if (!column) {
          continue;
        }

        if (typeof column.filterFn === "function") {
          if (!column.filterFn(row, expected, column)) {
            return false;
          }
          continue;
        }

        var value = getFilterValue(row, column);
        var filterType = (column.filter && column.filter.type) || "text";
        if (filterType === "select" || filterType === "date") {
          if (safeText(value) !== safeText(expected)) {
            return false;
          }
          continue;
        }
        if (filterType === "number") {
          if (toNumber(value) !== toNumber(expected)) {
            return false;
          }
          continue;
        }
        if (normalizeText(value).indexOf(normalizeText(expected)) === -1) {
          return false;
        }
      }
      return true;
    }

    function getProcessedRows() {
      var filtered = rows.filter(passFilters);
      if (state.sortCol) {
        filtered.sort(function (a, b) {
          return compareRows(a, b, state.sortCol, state.sortDir);
        });
      }
      return filtered;
    }

    function summaryText(total) {
      if (typeof config.summaryFormatter === "function") {
        return config.summaryFormatter(total, state);
      }
      return total + " registro(s) encontrado(s)";
    }

    function noRowsText() {
      return config.noRowsText || "Nenhum registro encontrado com os filtros atuais.";
    }

    function renderNoRows() {
      bodyEl.innerHTML = '<tr><td colspan="' + visibleColumnCount() + '" class="muted">' + escHtml(noRowsText()) + "</td></tr>";
    }

    function renderCell(row, column) {
      var ctx = {
        esc: escHtml,
        text: safeText,
        normalize: normalizeText,
        statusBadge: statusBadge,
        slotBadge: slotBadge,
        state: state,
      };
      if (typeof column.render === "function") {
        return column.render(row, ctx);
      }
      return escHtml(getColumnValue(row, column));
    }

    function rowAttrs(row) {
      if (typeof config.rowAttrs === "function") {
        return config.rowAttrs(row) || "";
      }
      return "";
    }

    function rowClass(row) {
      if (typeof config.rowClass === "function") {
        return config.rowClass(row) || "";
      }
      return "";
    }

    function renderRows(pageRows) {
      if (!pageRows.length) {
        renderNoRows();
        applyColumnVisibility();
        return;
      }
      bodyEl.innerHTML = pageRows
        .map(function (row) {
          var rowId = escHtml(row.id || "");
          var attrs = rowAttrs(row);
          var classes = rowClass(row);
          var classToken = classes ? ' class="' + escHtml(classes) + '"' : "";
          var attrToken = attrs ? " " + attrs : "";
          var cols = columns
            .map(function (column) {
              var cellClasses = ["datagrid-cell"];
              if (column.cellClass) {
                cellClasses.push(column.cellClass);
              }
              var style = columnStyle(column);
              var styleToken = style ? ' style="' + escHtml(style) + '"' : "";
              return (
                '<td data-dg-col="' +
                escHtml(column.key) +
                '" class="' +
                escHtml(cellClasses.join(" ")) +
                '"' +
                styleToken +
                ">" +
                renderCell(row, column) +
                "</td>"
              );
            })
            .join("");
          return '<tr data-row-id="' + rowId + '"' + classToken + attrToken + ">" + cols + "</tr>";
        })
        .join("");
      applyColumnVisibility();
    }

    function syncSortIndicators() {
      var sortButtons = root.querySelectorAll(".js-dg-sort");
      sortButtons.forEach(function (button) {
        var col = button.dataset.col;
        var indicator = button.querySelector(".datagrid-sort-indicator");
        if (!indicator) {
          return;
        }
        if (state.sortCol !== col) {
          indicator.textContent = "^v";
          button.classList.remove("is-sorted");
          return;
        }
        button.classList.add("is-sorted");
        indicator.textContent = state.sortDir === "asc" ? "^" : "v";
      });
    }

    function closeHeaderFilters() {
      var opened = root.querySelectorAll(".datagrid-head-cell.is-filter-open");
      opened.forEach(function (cell) {
        cell.classList.remove("is-filter-open");
      });
    }

    function openHeaderFilter(columnKey, initialChar) {
      if (!columnKey) {
        return;
      }
      var column = findColumn(columnKey);
      if (!column) {
        return;
      }
      if (column.filter === false) {
        return;
      }
      closeHeaderFilters();
      var cell = root.querySelector('.datagrid-head-cell[data-dg-col="' + columnKey + '"]');
      if (!cell) {
        return;
      }
      cell.classList.add("is-filter-open");
      var input = cell.querySelector(".js-dg-filter");
      if (!input) {
        return;
      }
      input.focus();
      if (initialChar && input.tagName === "INPUT" && input.type === "text") {
        input.value = initialChar;
        state.filters[columnKey] = initialChar;
        state.page = 1;
        renderTable();
      } else if (typeof input.select === "function" && input.tagName === "INPUT" && input.type !== "date") {
        input.select();
      }
    }

    function filterType(column) {
      if (!column || column.filter === false) {
        return "none";
      }
      if (column.filter && column.filter.type) {
        return column.filter.type;
      }
      return "text";
    }

    function renderFilterInput(column) {
      var type = filterType(column);
      var col = escHtml(column.key);
      var placeholder = escHtml((column.filter && column.filter.placeholder) || "Filtrar");

      if (type === "none") {
        return "";
      }

      if (type === "select") {
        var options = ((column.filter && column.filter.options) || [])
          .map(function (option) {
            return '<option value="' + escHtml(option.value) + '">' + escHtml(option.label) + "</option>";
          })
          .join("");
        return '<select class="datagrid-filter-input js-dg-filter" data-col="' + col + '"><option value="">Todos</option>' + options + "</select>";
      }

      if (type === "number") {
        var min = column.filter && column.filter.min !== undefined ? ' min="' + escHtml(column.filter.min) + '"' : "";
        var step = column.filter && column.filter.step !== undefined ? ' step="' + escHtml(column.filter.step) + '"' : ' step="1"';
        return '<input class="datagrid-filter-input js-dg-filter" type="number" data-col="' + col + '"' + min + step + ' placeholder="' + placeholder + '">';
      }

      if (type === "date") {
        return '<input class="datagrid-filter-input js-dg-filter" type="date" data-col="' + col + '">';
      }

      return '<input class="datagrid-filter-input js-dg-filter" type="text" data-col="' + col + '" placeholder="' + placeholder + '">';
    }

    function renderHeader() {
      var headHtml =
        "<tr>" +
        columns
          .map(function (column) {
            var col = escHtml(column.key);
            var headerStyle = columnStyle(column);
            var styleToken = headerStyle ? ' style="' + escHtml(headerStyle) + '"' : "";
            var label = escHtml(column.label || column.key);
            var sortable = column.sortable !== false;
            var hasFilter = filterType(column) !== "none";

            var labelNode = hasFilter
              ? '<button class="datagrid-head-label js-dg-filter-trigger" type="button" data-col="' + col + '">' + label + "</button>"
              : '<span class="datagrid-head-label is-static">' + label + "</span>";

            var sortNode = sortable
              ? '<button class="datagrid-sort-btn js-dg-sort" type="button" data-col="' +
                col +
                '" aria-label="Ordenar ' +
                label +
                '"><span class="datagrid-sort-indicator">^v</span></button>'
              : '<span class="datagrid-sort-btn is-disabled"><span class="datagrid-sort-indicator"></span></span>';

            var filterNode = hasFilter
              ? '<div class="datagrid-head-filter" data-col="' + col + '">' + renderFilterInput(column) + "</div>"
              : "";

            return (
              '<th data-dg-col="' +
              col +
              '"' +
              styleToken +
              ">" +
              '<div class="datagrid-head-cell" data-dg-col="' +
              col +
              '">' +
              labelNode +
              sortNode +
              filterNode +
              "</div></th>"
            );
          })
          .join("") +
        "</tr>";
      headEl.innerHTML = headHtml;
    }

    function syncColumnPicker() {
      pickerBody.innerHTML = columns
        .filter(function (column) {
          return !column.fixed;
        })
        .map(function (column) {
          var checked = isColumnVisible(column.key) ? "checked" : "";
          return (
            '<label class="datagrid-column-option">' +
            '<input type="checkbox" class="js-dg-column-toggle" data-col="' +
            escHtml(column.key) +
            '" ' +
            checked +
            ">" +
            "<span>" +
            escHtml(column.label || column.key) +
            "</span>" +
            "</label>"
          );
        })
        .join("");

      var toggles = pickerBody.querySelectorAll(".js-dg-column-toggle");
      toggles.forEach(function (toggle) {
        toggle.addEventListener("change", function () {
          var col = toggle.dataset.col;
          if (!col) {
            return;
          }
          if (toggle.checked) {
            if (!isColumnVisible(col)) {
              visibleColumns.push(col);
            }
          } else {
            visibleColumns = visibleColumns.filter(function (item) {
              return item !== col;
            });
          }
          ensureMandatoryColumns();
          saveVisibleColumns(storageKey, visibleColumns);
          renderTable();
        });
      });
    }

    function bindHeaderActions() {
      var sortButtons = root.querySelectorAll(".js-dg-sort");
      sortButtons.forEach(function (button) {
        button.addEventListener("click", function (event) {
          event.stopPropagation();
          var col = button.dataset.col;
          var column = findColumn(col);
          if (!col || !column || column.sortable === false) {
            return;
          }
          if (state.sortCol === col) {
            if (state.sortDir === "asc") {
              state.sortDir = "desc";
            } else {
              state.sortCol = "";
              state.sortDir = "asc";
            }
          } else {
            state.sortCol = col;
            state.sortDir = "asc";
          }
          state.page = 1;
          renderTable();
        });
      });

      var filterTriggers = root.querySelectorAll(".js-dg-filter-trigger");
      filterTriggers.forEach(function (trigger) {
        trigger.addEventListener("click", function (event) {
          event.stopPropagation();
          openHeaderFilter(trigger.dataset.col);
        });

        trigger.addEventListener("keydown", function (event) {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            openHeaderFilter(trigger.dataset.col);
            return;
          }
          if (event.key === "Escape") {
            event.preventDefault();
            closeHeaderFilters();
            return;
          }
          if (event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
            var col = trigger.dataset.col;
            var cell = trigger.closest(".datagrid-head-cell");
            var input = cell ? cell.querySelector(".js-dg-filter") : null;
            if (input && input.tagName === "INPUT" && input.type === "text") {
              event.preventDefault();
              openHeaderFilter(col, event.key);
            }
          }
        });
      });

      var filterPanels = root.querySelectorAll(".datagrid-head-filter");
      filterPanels.forEach(function (panel) {
        panel.addEventListener("click", function (event) {
          event.stopPropagation();
        });
      });

      root.addEventListener("keydown", function (event) {
        if (event.key === "Escape") {
          closeHeaderFilters();
        }
      });

      document.addEventListener("click", function (event) {
        if (!root.contains(event.target)) {
          closeHeaderFilters();
          return;
        }
        if (!event.target.closest(".datagrid-head-cell")) {
          closeHeaderFilters();
        }
      });
    }

    function bindFilterActions() {
      var filterInputs = root.querySelectorAll(".js-dg-filter");
      filterInputs.forEach(function (input) {
        var eventName = input.tagName === "SELECT" ? "change" : "input";
        input.addEventListener(eventName, function () {
          var col = input.dataset.col;
          if (!col) {
            return;
          }
          state.filters[col] = safeText(input.value).trim();
          state.page = 1;
          renderTable();
        });

        input.addEventListener("keydown", function (event) {
          if (event.key === "Escape") {
            event.preventDefault();
            closeHeaderFilters();
            var col = input.dataset.col;
            var trigger = root.querySelector('.js-dg-filter-trigger[data-col="' + col + '"]');
            if (trigger) {
              trigger.focus();
            }
          }
        });
      });

      clearButton.addEventListener("click", function () {
        var currentInputs = root.querySelectorAll(".js-dg-filter");
        currentInputs.forEach(function (input) {
          input.value = "";
          if (input.dataset && input.dataset.col) {
            state.filters[input.dataset.col] = "";
          }
        });
        state.page = 1;
        closeHeaderFilters();
        renderTable();
      });
    }

    function bindPaginationActions() {
      pageSizeSelect.addEventListener("change", function () {
        var value = Number(pageSizeSelect.value || 20);
        state.pageSize = value > 0 ? value : 20;
        state.page = 1;
        renderTable();
      });

      prevButton.addEventListener("click", function () {
        state.page -= 1;
        renderTable();
      });

      nextButton.addEventListener("click", function () {
        state.page += 1;
        renderTable();
      });
    }

    function ensurePageSizeOptions() {
      var options = Array.isArray(config.pageSizeOptions) && config.pageSizeOptions.length
        ? config.pageSizeOptions
        : [10, 20, 50, 100];
      pageSizeSelect.innerHTML = options
        .map(function (option) {
          return '<option value="' + escHtml(option) + '">' + escHtml(option) + "</option>";
        })
        .join("");
      var exact = options.some(function (option) {
        return Number(option) === Number(state.pageSize);
      });
      if (!exact) {
        var extra = document.createElement("option");
        extra.value = String(state.pageSize);
        extra.textContent = String(state.pageSize);
        pageSizeSelect.appendChild(extra);
      }
      pageSizeSelect.value = String(state.pageSize);
    }

    function renderTable() {
      var processedRows = getProcessedRows();
      var total = processedRows.length;
      var totalPages = Math.max(1, Math.ceil(total / state.pageSize));
      if (state.page > totalPages) {
        state.page = totalPages;
      }
      if (state.page < 1) {
        state.page = 1;
      }

      var start = (state.page - 1) * state.pageSize;
      var end = start + state.pageSize;
      var pageRows = processedRows.slice(start, end);

      renderRows(pageRows);
      summaryEl.textContent = summaryText(total);
      pageIndicatorEl.textContent = "Pagina " + state.page + " de " + totalPages;
      prevButton.disabled = state.page <= 1;
      nextButton.disabled = state.page >= totalPages;
      syncSortIndicators();

      if (typeof config.onAfterRender === "function") {
        config.onAfterRender(api, pageRows, processedRows);
      }
    }

    function getRowById(id) {
      var idText = safeText(id);
      return rows.find(function (row) {
        return safeText(row.id) === idText;
      });
    }

    var api = {
      root: root,
      table: tableEl,
      columns: columns,
      getState: function () {
        return JSON.parse(JSON.stringify(state));
      },
      getRows: function () {
        return rows.slice();
      },
      getRowById: getRowById,
      refresh: renderTable,
      setRows: function (nextRows) {
        rows = decorateRows(nextRows || []);
        state.page = 1;
        renderTable();
      },
      updateRow: function (id, patch) {
        var row = getRowById(id);
        if (!row) {
          return;
        }
        Object.keys(patch || {}).forEach(function (key) {
          row[key] = patch[key];
        });
        renderTable();
      },
      removeRow: function (id) {
        rows = rows.filter(function (row) {
          return safeText(row.id) !== safeText(id);
        });
        reindexRows();
        renderTable();
      },
      swapRows: function (idA, idB) {
        var rowA = getRowById(idA);
        var rowB = getRowById(idB);
        if (!rowA || !rowB) {
          return;
        }
        var indexA = rowA._index;
        rowA._index = rowB._index;
        rowB._index = indexA;
        if (rowA.ordem !== undefined && rowB.ordem !== undefined) {
          var ordemA = rowA.ordem;
          rowA.ordem = rowB.ordem;
          rowB.ordem = ordemA;
        }
        renderTable();
      },
      closeFilters: closeHeaderFilters,
      openFilter: openHeaderFilter,
    };

    ensureMandatoryColumns();
    ensurePageSizeOptions();
    renderHeader();
    syncColumnPicker();
    bindHeaderActions();
    bindFilterActions();
    bindPaginationActions();
    applyColumnVisibility();
    renderTable();

    mounted = true;

    window.addEventListener("resize", function () {
      if (!mounted) {
        return;
      }
      if (typeof config.onResize === "function") {
        config.onResize(api);
      }
    });

    if (typeof config.onReady === "function") {
      config.onReady(api);
    }

    return api;
  }

  global.SAASDataGrid = {
    create: createDataGrid,
    utils: {
      byId: byId,
      safeText: safeText,
      escHtml: escHtml,
      normalizeText: normalizeText,
      parseJsonScript: parseJsonScript,
      statusBadge: statusBadge,
      slotBadge: slotBadge,
    },
  };
})(window);
