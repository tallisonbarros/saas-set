(function () {
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
      var raw = el.textContent || "[]";
      var data = JSON.parse(raw);
      return Array.isArray(data) ? data : [];
    } catch (e) {
      return [];
    }
  }

  function buildStorageKey(tableEl) {
    var radarId = tableEl && tableEl.dataset ? tableEl.dataset.radarId : "";
    return "radar_table_columns_v2:" + (radarId || "global");
  }

  function loadVisibleColumns(storageKey, columns) {
    var defaults = columns.filter(function (column) {
      return column.visible !== false;
    }).map(function (column) {
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

  function statusBadge(status, statusLabel) {
    var normalized = normalizeText(status).toUpperCase();
    var cssSuffix = normalized ? normalized.toLowerCase() : "pendente";
    return '<span class="badge badge-' + cssSuffix + '">' + escHtml(statusLabel || status || "-") + "</span>";
  }

  function compareRows(a, b, sortCol, sortDir) {
    var direction = sortDir === "desc" ? -1 : 1;
    var av = a[sortCol];
    var bv = b[sortCol];

    if (sortCol === "total_atividades") {
      var an = Number(av || 0);
      var bn = Number(bv || 0);
      if (an === bn) {
        return (a._index - b._index) * direction;
      }
      return (an - bn) * direction;
    }

    if (sortCol === "data_registro") {
      var ad = safeText(av);
      var bd = safeText(bv);
      if (ad === bd) {
        return (a._index - b._index) * direction;
      }
      return ad.localeCompare(bd, "pt-BR") * direction;
    }

    var as = normalizeText(av);
    var bs = normalizeText(bv);
    if (as === bs) {
      return (a._index - b._index) * direction;
    }
    return as.localeCompare(bs, "pt-BR") * direction;
  }

  function renderNoRows(tbody, colspan) {
    tbody.innerHTML = '<tr><td colspan="' + colspan + '" class="muted">Nenhum trabalho encontrado com os filtros atuais.</td></tr>';
  }

  function initRadarTable() {
    var tableEl = byId("radar-trabalhos-table");
    var tbody = byId("radar-trabalhos-body");
    var summaryEl = byId("radar-table-summary");
    var pageIndicatorEl = byId("radar-page-indicator");
    var prevButton = byId("radar-prev-page");
    var nextButton = byId("radar-next-page");
    var pageSizeSelect = byId("radar-page-size");
    var clearFiltersButton = byId("radar-clear-filters");
    var pickerBody = byId("radar-column-picker-body");

    if (!tableEl || !tbody || !summaryEl || !pageIndicatorEl || !prevButton || !nextButton || !pageSizeSelect || !clearFiltersButton || !pickerBody) {
      return;
    }

    var columns = [
      { key: "nome", label: "Nome", visible: true, fixed: true },
      { key: "descricao", label: "Descricao", visible: false },
      { key: "status", label: "Status", visible: true },
      { key: "classificacao", label: "Classificacao", visible: false },
      { key: "contrato", label: "Contrato", visible: false },
      { key: "data_registro", label: "Data registro", visible: true },
      { key: "responsavel", label: "Responsavel", visible: false },
      { key: "setor", label: "Setor", visible: false },
      { key: "solicitante", label: "Solicitante", visible: false },
      { key: "total_atividades", label: "Atividades", visible: false },
    ];

    var rows = parseJsonScript("radar-trabalhos-data").map(function (row, index) {
      var cloned = Object.assign({}, row);
      cloned._index = index;
      return cloned;
    });

    var state = {
      filters: {},
      sortCol: "",
      sortDir: "asc",
      page: 1,
      pageSize: Number(pageSizeSelect.value || 20),
    };

    var storageKey = buildStorageKey(tableEl);
    var visibleColumns = loadVisibleColumns(storageKey, columns);

    function ensureMandatoryColumns() {
      columns.forEach(function (column) {
        if (column.fixed && visibleColumns.indexOf(column.key) === -1) {
          visibleColumns.push(column.key);
        }
      });
    }

    ensureMandatoryColumns();

    function isColumnVisible(columnKey) {
      return visibleColumns.indexOf(columnKey) >= 0;
    }

    function visibleColumnCount() {
      var count = columns.filter(function (column) {
        return isColumnVisible(column.key);
      }).length;
      return count > 0 ? count : 1;
    }

    function applyColumnVisibility() {
      columns.forEach(function (column) {
        var hidden = !isColumnVisible(column.key);
        var nodes = tableEl.querySelectorAll('[data-col="' + column.key + '"]');
        nodes.forEach(function (node) {
          node.classList.toggle("is-hidden", hidden);
        });
      });
    }

    function syncSortIndicators() {
      var sortButtons = tableEl.querySelectorAll(".js-radar-sort");
      sortButtons.forEach(function (button) {
        var indicator = button.querySelector(".radar-sort-indicator");
        if (!indicator) {
          return;
        }
        if (state.sortCol !== button.dataset.col) {
          indicator.textContent = "^v";
          button.classList.remove("is-sorted");
          return;
        }
        button.classList.add("is-sorted");
        indicator.textContent = state.sortDir === "asc" ? "^" : "v";
      });
    }

    function closeHeaderFilters() {
      var opened = tableEl.querySelectorAll(".radar-head-cell.is-filter-open");
      opened.forEach(function (cell) {
        cell.classList.remove("is-filter-open");
      });
    }

    function openHeaderFilter(col, initialValue) {
      if (!col) {
        return;
      }
      closeHeaderFilters();
      var cell = tableEl.querySelector('.radar-head-cell[data-col="' + col + '"]');
      if (!cell) {
        return;
      }
      cell.classList.add("is-filter-open");
      var input = cell.querySelector(".js-radar-filter");
      if (!input) {
        return;
      }
      input.focus();
      if (initialValue && input.tagName === "INPUT" && input.type === "text") {
        input.value = initialValue;
        state.filters[col] = initialValue;
        state.page = 1;
        renderTable();
      } else if (typeof input.select === "function" && input.tagName === "INPUT" && input.type !== "date") {
        input.select();
      }
    }

    function passFilters(row) {
      var keys = Object.keys(state.filters);
      for (var i = 0; i < keys.length; i += 1) {
        var key = keys[i];
        var expected = state.filters[key];
        if (!expected) {
          continue;
        }
        if (key === "status") {
          if (safeText(row.status) !== expected) {
            return false;
          }
          continue;
        }
        if (key === "data_registro") {
          if (safeText(row.data_registro) !== expected) {
            return false;
          }
          continue;
        }
        if (key === "total_atividades") {
          if (Number(row.total_atividades || 0) !== Number(expected || 0)) {
            return false;
          }
          continue;
        }
        var value = normalizeText(row[key]);
        if (value.indexOf(normalizeText(expected)) === -1) {
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

    function renderRows(pageRows) {
      if (!pageRows.length) {
        renderNoRows(tbody, visibleColumnCount());
        applyColumnVisibility();
        setupDescriptionMarquees();
        return;
      }
      tbody.innerHTML = pageRows.map(function (row) {
        var nomeCell = escHtml(row.nome || "-");
        var descricaoCell = escHtml(row.descricao || "Sem descricao.");
        if (row.detalhe_url) {
          nomeCell = '<a class="radar-row-link" href="' + escHtml(row.detalhe_url) + '">' + nomeCell + "</a>";
        }
        return [
          '<tr data-row-id="' + escHtml(row.id) + '">',
          '<td data-col="nome" class="radar-col-nome">' +
            nomeCell +
            '<div class="radar-desc-marquee" title="' + descricaoCell + '">' +
              '<span class="radar-desc-marquee-text">' + descricaoCell + "</span>" +
            "</div>" +
          "</td>",
          '<td data-col="descricao" class="radar-cell-wrap">' + escHtml(row.descricao || "-") + "</td>",
          '<td data-col="status">' + statusBadge(row.status, row.status_label) + "</td>",
          '<td data-col="classificacao">' + escHtml(row.classificacao || "-") + "</td>",
          '<td data-col="contrato">' + escHtml(row.contrato || "-") + "</td>",
          '<td data-col="data_registro">' + escHtml(row.data_registro_label || "-") + "</td>",
          '<td data-col="responsavel">' + escHtml(row.responsavel || "-") + "</td>",
          '<td data-col="setor">' + escHtml(row.setor || "-") + "</td>",
          '<td data-col="solicitante">' + escHtml(row.solicitante || "-") + "</td>",
          '<td data-col="total_atividades"><span class="slot-badge slot-badge-compact">' + escHtml(row.total_atividades || 0) + " atividades</span></td>",
          "</tr>",
        ].join("");
      }).join("");
      applyColumnVisibility();
      setupDescriptionMarquees();
    }

    function setupDescriptionMarquees() {
      var marquees = tbody.querySelectorAll(".radar-desc-marquee");
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
      summaryEl.textContent = total + " trabalho(s) encontrado(s)";
      pageIndicatorEl.textContent = "Pagina " + state.page + " de " + totalPages;
      prevButton.disabled = state.page <= 1;
      nextButton.disabled = state.page >= totalPages;
      syncSortIndicators();
    }

    function syncColumnPicker() {
      pickerBody.innerHTML = columns.filter(function (column) {
        return !column.fixed;
      }).map(function (column) {
        var checked = isColumnVisible(column.key) ? "checked" : "";
        return (
          '<label class="radar-column-option">' +
          '<input type="checkbox" class="js-radar-column-toggle" data-col="' + escHtml(column.key) + '" ' + checked + ">" +
          "<span>" + escHtml(column.label) + "</span>" +
          "</label>"
        );
      }).join("");

      var toggles = pickerBody.querySelectorAll(".js-radar-column-toggle");
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

    function bindSortActions() {
      var sortButtons = tableEl.querySelectorAll(".js-radar-sort");
      sortButtons.forEach(function (button) {
        button.addEventListener("click", function (event) {
          event.stopPropagation();
          var col = button.dataset.col;
          if (!col) {
            return;
          }
          if (state.sortCol === col) {
            state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
          } else {
            state.sortCol = col;
            state.sortDir = "asc";
          }
          state.page = 1;
          renderTable();
        });
      });
    }

    function bindHeaderFilterTriggers() {
      var triggers = tableEl.querySelectorAll(".js-radar-filter-trigger");
      triggers.forEach(function (trigger) {
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
            var cell = trigger.closest(".radar-head-cell");
            var input = cell ? cell.querySelector(".js-radar-filter") : null;
            if (input && input.tagName === "INPUT" && input.type === "text") {
              event.preventDefault();
              openHeaderFilter(col, event.key);
            }
          }
        });
      });

      var filterPanels = tableEl.querySelectorAll(".radar-head-filter");
      filterPanels.forEach(function (panel) {
        panel.addEventListener("click", function (event) {
          event.stopPropagation();
        });
      });

      document.addEventListener("click", function (event) {
        if (!tableEl.contains(event.target)) {
          closeHeaderFilters();
          return;
        }
        if (!event.target.closest(".radar-head-cell")) {
          closeHeaderFilters();
        }
      });
    }

    function bindFilterActions() {
      var filterInputs = tableEl.querySelectorAll(".js-radar-filter");
      filterInputs.forEach(function (input) {
        var eventName = input.tagName === "SELECT" ? "change" : "input";
        input.addEventListener(eventName, function () {
          var col = input.dataset.col;
          if (!col) {
            return;
          }
          var value = safeText(input.value).trim();
          state.filters[col] = value;
          state.page = 1;
          renderTable();
        });

        input.addEventListener("keydown", function (event) {
          if (event.key === "Escape") {
            event.preventDefault();
            closeHeaderFilters();
            var col = input.dataset.col;
            var trigger = tableEl.querySelector('.js-radar-filter-trigger[data-col="' + col + '"]');
            if (trigger) {
              trigger.focus();
            }
          }
        });
      });

      clearFiltersButton.addEventListener("click", function () {
        var filterInputs = tableEl.querySelectorAll(".js-radar-filter");
        filterInputs.forEach(function (input) {
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

    window.addEventListener("resize", function () {
      setupDescriptionMarquees();
    });

    syncColumnPicker();
    bindSortActions();
    bindHeaderFilterTriggers();
    bindFilterActions();
    renderTable();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initRadarTable);
  } else {
    initRadarTable();
  }
})();
