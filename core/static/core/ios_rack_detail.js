(function () {
  function byId(id) {
    return document.getElementById(id);
  }

  function normalizeTag(value) {
    return (value || "").trim().replace(/\s+/g, "_").toUpperCase();
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function getCookie(name) {
    var value = "; " + document.cookie;
    var parts = value.split("; " + name + "=");
    return parts.length === 2 ? parts.pop().split(";").shift() : "";
  }

  function ensureInlineStyle() {
    if (document.getElementById("rack-module-editor-inline-style")) {
      return;
    }
    var style = document.createElement("style");
    style.id = "rack-module-editor-inline-style";
    style.textContent =
      ".field-dup{border-color:#d04b3b;box-shadow:0 0 0 1px rgba(208,75,59,.15)}" +
      ".row-toast{position:absolute;right:12px;top:50%;transform:translateY(-50%);background:#1f6f3d;color:#fff;padding:4px 8px;border-radius:999px;font-size:12px;opacity:0;transition:opacity .2s ease}" +
      ".channel-row.show-toast .row-toast{opacity:1}";
    document.head.appendChild(style);
  }

  var panel = byId("rack-module-panel");
  var notePanel = byId("rack-module-note");
  var panelBody = byId("rack-module-panel-body");
  var panelTitle = byId("rack-module-panel-title");
  var panelSubtitle = byId("rack-module-panel-subtitle");
  var moduleDataNode = byId("rack-module-editor-data");
  var channelTypesNode = byId("rack-channel-types-data");
  var vacantSlotsNode = byId("rack-vacant-slots-data");
  if (!panel || !panelBody || !moduleDataNode || !channelTypesNode || !vacantSlotsNode) {
    return;
  }

  ensureInlineStyle();

  var moduleData = JSON.parse(moduleDataNode.textContent || "{}");
  var channelTypes = JSON.parse(channelTypesNode.textContent || "[]");
  var vacantSlots = JSON.parse(vacantSlotsNode.textContent || "[]");
  var canManage = panel.dataset.canManage === "1";
  var moduleCards = Array.from(document.querySelectorAll(".js-rack-module-card"));
  var rackTrack = byId("rack-carousel-track");
  var activeModuleId = panel.dataset.selectedModuleId || "";
  var manageModal = byId("rack-module-manage-modal");
  var manageModalId = byId("rack-module-manage-id");
  var manageModalSlot = byId("rack-module-manage-slot");
  var manageModalNote = byId("rack-module-manage-note");
  var manageForm = byId("rack-module-manage-form");
  var deleteForm = byId("rack-module-delete-form");
  var deleteModalId = byId("rack-module-delete-id");

  function showRowToast(row, text) {
    if (!row) {
      return;
    }
    row.style.position = "relative";
    var toast = row.querySelector(".row-toast");
    if (!toast) {
      toast = document.createElement("span");
      toast.className = "row-toast";
      row.appendChild(toast);
    }
    toast.textContent = text || "Salvo";
    row.classList.add("show-toast");
    clearTimeout(row._toastTimer);
    row._toastTimer = setTimeout(function () {
      row.classList.remove("show-toast");
    }, 1200);
  }

  function refreshDupes(scope) {
    var inputs = Array.from((scope || document).querySelectorAll(".channel-tag-input"));
    var counts = {};
    inputs.forEach(function (input) {
      var value = (input.value || "").trim().toUpperCase();
      if (value) {
        counts[value] = (counts[value] || 0) + 1;
      }
    });
    inputs.forEach(function (input) {
      var value = (input.value || "").trim().toUpperCase();
      var isDup = value && counts[value] > 1;
      input.classList.toggle("field-dup", isDup);
      if (isDup) {
        input.setAttribute("title", "TAG repetida");
      } else {
        input.removeAttribute("title");
      }
    });
  }

  function markDirty(input) {
    var row = input.closest(".channel-row");
    if (row) {
      row.setAttribute("data-dirty", "1");
    }
  }

  function isRowCommissioned(row) {
    var statusButton = row ? row.querySelector(".channel-status-btn") : null;
    return !!(statusButton && statusButton.getAttribute("data-state") === "tested");
  }

  function updateStatusButton(button, commissioned) {
    if (!button) {
      return;
    }
    var isTested = !!commissioned;
    button.setAttribute("data-state", isTested ? "tested" : "pending");
    button.setAttribute("aria-pressed", isTested ? "true" : "false");
    button.textContent = isTested ? "Testado" : "Testar";
    button.classList.toggle("is-tested", isTested);
    button.classList.toggle("is-pending", !isTested);
  }

  function buildTypeOptions(selectedId) {
    return channelTypes.map(function (item) {
      return "<option value=\"" + escapeHtml(item.id) + "\"" +
        (String(item.id) === String(selectedId) ? " selected" : "") + ">" +
        escapeHtml(item.nome) + "</option>";
    }).join("");
  }

  function buildVacantOptions() {
    return "<option value=\"\">Selecione</option>" + vacantSlots.map(function (item) {
      return "<option value=\"" + escapeHtml(item.id) + "\">Slot " +
        escapeHtml(String(item.posicao).padStart(2, "0")) + "</option>";
    }).join("");
  }

  function openManageModal() {
    if (!manageModal || !activeModuleId || !moduleData[String(activeModuleId)]) {
      return;
    }
    var info = moduleData[String(activeModuleId)];
    if (manageModalId) {
      manageModalId.value = info.id;
    }
    if (deleteModalId) {
      deleteModalId.value = info.id;
    }
    if (manageModalSlot) {
      manageModalSlot.innerHTML = buildVacantOptions();
      manageModalSlot.disabled = !vacantSlots.length;
    }
    if (manageModalNote) {
      manageModalNote.classList.toggle("is-hidden", !!vacantSlots.length);
    }
    manageModal.hidden = false;
    manageModal.classList.add("is-open");
    document.body.classList.add("radar-export-modal-open");
  }

  function closeManageModal() {
    if (!manageModal) {
      return;
    }
    manageModal.classList.remove("is-open");
    manageModal.hidden = true;
    document.body.classList.remove("radar-export-modal-open");
  }

  function clearSelectedCards() {
    moduleCards.forEach(function (card) {
      card.classList.remove("slot-selected");
    });
  }

  function syncPanelDock(card) {
    if (!panel) {
      return;
    }
    var activeCard = card || document.querySelector(".js-rack-module-card.slot-selected");
    if (!activeCard) {
      panel.style.removeProperty("--rack-panel-dock-x");
      return;
    }
    var panelRect = panel.getBoundingClientRect();
    var cardRect = activeCard.getBoundingClientRect();
    var rawOffset = (cardRect.left + (cardRect.width / 2)) - panelRect.left;
    var margin = 40;
    var maxOffset = Math.max(margin, panel.clientWidth - margin);
    var clampedOffset = Math.min(Math.max(rawOffset, margin), maxOffset);
    panel.style.setProperty("--rack-panel-dock-x", clampedOffset + "px");
  }

  function syncSelectedCard(moduleId) {
    clearSelectedCards();
    var selectedCard = null;
    moduleCards.forEach(function (card) {
      if (String(card.dataset.moduleId) === String(moduleId)) {
        card.classList.add("slot-selected");
        selectedCard = card;
      }
    });
    syncPanelDock(selectedCard);
  }

  function postInlineSave(row) {
    if (!row || !activeModuleId) {
      return;
    }
    var data = new FormData();
    data.set("action", "inline_update_channel");
    data.set("module_id", activeModuleId);
    data.set("channel_id", row.getAttribute("data-channel-id"));
    data.set("tag", row.querySelector(".channel-tag-input").value || "");
    data.set("descricao", row.querySelector(".channel-desc-input").value || "");
    data.set("tipo", row.querySelector(".channel-type-select").value || "");
    if (isRowCommissioned(row)) {
      data.set("comissionado", "on");
    }
    fetch(window.location.pathname + window.location.search, {
      method: "POST",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRFToken": getCookie("csrftoken")
      },
      body: data
    }).then(function () {
      row.removeAttribute("data-dirty");
      showRowToast(row, "Salvo");
    });
  }

  function bindPaste(inputs, normalizeFn) {
    inputs.forEach(function (input) {
      input.addEventListener("paste", function (event) {
        var text = (event.clipboardData || window.clipboardData).getData("text");
        if (!text || text.indexOf("\n") === -1 || text.indexOf("\t") !== -1) {
          return;
        }
        event.preventDefault();
        var lines = text.split(/\r?\n/);
        var all = Array.from(inputs);
        var startIndex = all.indexOf(input);
        lines.forEach(function (line, idx) {
          var target = all[startIndex + idx];
          if (!target || !(line || "").trim()) {
            return;
          }
          target.value = normalizeFn ? normalizeFn(line) : line;
          markDirty(target);
        });
        refreshDupes(panelBody);
      });
    });
  }

  function bindGridPaste(inputs) {
    inputs.forEach(function (input) {
      input.addEventListener("paste", function (event) {
        var text = (event.clipboardData || window.clipboardData).getData("text");
        if (!text || text.indexOf("\t") === -1) {
          return;
        }
        event.preventDefault();
        var rows = text.split(/\r?\n/);
        var allRows = Array.from(panelBody.querySelectorAll(".channel-row[data-channel-id]"));
        var startRow = input.closest(".channel-row");
        var startIndex = allRows.indexOf(startRow);
        var startInTag = input.classList.contains("channel-tag-input");
        rows.forEach(function (rowText, rowOffset) {
          var cols = rowText.split("\t");
          var rowEl = allRows[startIndex + rowOffset];
          if (!rowEl || !cols.some(function (cell) { return (cell || "").trim(); })) {
            return;
          }
          var tagInput = rowEl.querySelector(".channel-tag-input");
          var descInput = rowEl.querySelector(".channel-desc-input");
          if (startInTag) {
            if (tagInput && cols[0] !== undefined) {
              tagInput.value = normalizeTag(cols[0]);
              markDirty(tagInput);
            }
            if (descInput && cols[1] !== undefined) {
              descInput.value = cols[1];
              markDirty(descInput);
            }
          } else if (descInput && cols[0] !== undefined) {
            descInput.value = cols[0];
            markDirty(descInput);
          }
        });
        refreshDupes(panelBody);
      });
    });
  }

  function bindEditorInteractions(moduleInfo) {
    var scope = panelBody;
    var tagInputs = Array.from(scope.querySelectorAll(".channel-tag-input"));
    var descInputs = Array.from(scope.querySelectorAll(".channel-desc-input"));
    var typeSelects = Array.from(scope.querySelectorAll(".channel-type-select"));
    var statusButtons = Array.from(scope.querySelectorAll(".channel-status-btn"));
    if (!canManage) {
      refreshDupes(scope);
      return;
    }

    tagInputs.forEach(function (input) {
      input.value = normalizeTag(input.value);
      input.addEventListener("input", function () {
        input.value = normalizeTag(input.value);
        markDirty(input);
        refreshDupes(scope);
      });
      input.addEventListener("blur", function () {
        input.value = normalizeTag(input.value);
        refreshDupes(scope);
        postInlineSave(input.closest(".channel-row"));
      });
      input.addEventListener("keydown", function (event) {
        if (event.key === "Enter") {
          event.preventDefault();
          postInlineSave(input.closest(".channel-row"));
        }
      });
    });

    descInputs.forEach(function (input) {
      input.addEventListener("input", function () { markDirty(input); });
      input.addEventListener("blur", function () { postInlineSave(input.closest(".channel-row")); });
      input.addEventListener("keydown", function (event) {
        if (event.key === "Enter") {
          event.preventDefault();
          postInlineSave(input.closest(".channel-row"));
        }
      });
    });

    typeSelects.forEach(function (input) {
      input.addEventListener("change", function () {
        markDirty(input);
        postInlineSave(input.closest(".channel-row"));
      });
    });

    statusButtons.forEach(function (button) {
      updateStatusButton(button, button.getAttribute("data-state") === "tested");
      button.addEventListener("click", function () {
        var willBeTested = button.getAttribute("data-state") !== "tested";
        updateStatusButton(button, willBeTested);
        markDirty(button);
        postInlineSave(button.closest(".channel-row"));
      });
    });

    bindPaste(tagInputs, normalizeTag);
    bindPaste(descInputs, null);
    bindGridPaste(tagInputs.concat(descInputs));
    refreshDupes(scope);

    var channelForm = scope.querySelector(".rack-module-channel-form");
    if (channelForm) {
      channelForm.addEventListener("submit", function (event) {
        event.preventDefault();
        var dirtyRows = Array.from(channelForm.querySelectorAll(".channel-row[data-dirty='1']"));
        if (!dirtyRows.length) {
          return;
        }
        var data = new FormData();
        data.set("action", "bulk_update_channels");
        data.set("module_id", moduleInfo.id);
        dirtyRows.forEach(function (row) {
          var channelId = row.getAttribute("data-channel-id");
          data.append("channel_id", channelId);
          data.set("tag_" + channelId, row.querySelector(".channel-tag-input").value || "");
          data.set("descricao_" + channelId, row.querySelector(".channel-desc-input").value || "");
          data.set("tipo_" + channelId, row.querySelector(".channel-type-select").value || "");
          if (isRowCommissioned(row)) {
            data.set("comissionado_" + channelId, "on");
          }
        });
        fetch(window.location.pathname + window.location.search, {
          method: "POST",
          headers: {
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRFToken": getCookie("csrftoken")
          },
          body: data
        }).then(function () {
          dirtyRows.forEach(function (row) {
            row.removeAttribute("data-dirty");
            showRowToast(row, "Salvo");
          });
        });
      });
    }

    var manageForm = scope.querySelector(".rack-module-manage-form");
    if (manageForm) {
      manageForm.addEventListener("submit", function (event) {
        event.preventDefault();
        var data = new FormData();
        data.set("action", "update_selected_module");
        data.set("module_id", moduleInfo.id);
        var slotSelect = manageForm.querySelector("select[name='slot_id']");
        if (slotSelect && slotSelect.value) {
          data.set("slot_id", slotSelect.value);
        }
        fetch(window.location.pathname + window.location.search, {
          method: "POST",
          headers: {
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRFToken": getCookie("csrftoken")
          },
          body: data
        }).then(function () {
          window.location.href = window.location.pathname + "?module=" + moduleInfo.id + "#rack-module-panel";
        });
      });
    }

    var deleteForm = scope.querySelector(".rack-module-delete-form");
    if (deleteForm) {
      deleteForm.addEventListener("submit", function (event) {
        event.preventDefault();
        if (!window.confirm("Excluir modulo? Esta acao nao pode ser desfeita.")) {
          return;
        }
        var data = new FormData();
        data.set("action", "delete_selected_module");
        data.set("module_id", moduleInfo.id);
        fetch(window.location.pathname + window.location.search, {
          method: "POST",
          headers: {
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRFToken": getCookie("csrftoken")
          },
          body: data
        }).then(function () {
          window.location.href = window.location.pathname;
        });
      });
    }
  }

  function renderPanel(moduleId) {
    var info = moduleData[String(moduleId)];
    if (!info) {
      return;
    }
    activeModuleId = String(info.id);
    if (notePanel) {
      notePanel.classList.add("is-hidden");
    }
    panel.classList.remove("is-hidden");
    if (panelTitle) {
      panelTitle.textContent = info.display_name;
    }
    if (panelSubtitle) {
      panelSubtitle.textContent = [
        "SLOT " + String(info.slot_pos).padStart(2, "0"),
        info.model_name,
        info.type_name
      ].filter(Boolean).join(" | ");
    }
    panelBody.innerHTML =
      "<div class=\"rack-module-toolbar\">" +
        "<span class=\"rack-module-toolbar-pill\">SLOT " + escapeHtml(String(info.slot_pos).padStart(2, "0")) + "</span>" +
        "<span class=\"rack-module-toolbar-pill\">" + escapeHtml(info.type_name || "-") + "</span>" +
        "<span class=\"rack-module-toolbar-pill\">" + escapeHtml(info.channels.length) + " canais</span>" +
      "</div>" +
      "<form class=\"channel-form rack-module-channel-form\">" +
        "<div class=\"channel-row channel-row-head\" aria-hidden=\"true\">" +
          "<div class=\"channel-index\">Canal</div>" +
          "<div class=\"channel-col\">Tag</div>" +
          "<div class=\"channel-col\">Descricao</div>" +
          "<div class=\"channel-col\">Tipo</div>" +
          "<div class=\"channel-col\">Status</div>" +
        "</div>" +
        info.channels.map(function (channel) {
          return "<div class=\"channel-row\" data-channel-id=\"" + escapeHtml(channel.id) + "\">" +
            "<div class=\"channel-index\">Canal " + escapeHtml(channel.indice) + "</div>" +
            "<label class=\"field channel-field channel-field-tag\"><span>Tag</span><input type=\"text\" class=\"channel-tag-input\" value=\"" + escapeHtml(channel.tag) + "\" placeholder=\"TAG\"" + (canManage ? "" : " disabled") + "></label>" +
            "<label class=\"field channel-field channel-field-desc\"><span>Descricao</span><input type=\"text\" class=\"channel-desc-input\" value=\"" + escapeHtml(channel.descricao) + "\"" + (canManage ? "" : " disabled") + "></label>" +
            "<label class=\"field channel-field channel-field-type\"><span>Tipo</span><select class=\"channel-type-select\"" + (canManage ? "" : " disabled") + ">" + buildTypeOptions(channel.tipo_id) + "</select></label>" +
            "<button class=\"channel-status-btn channel-field-status " + (channel.comissionado ? "is-tested" : "is-pending") + "\" type=\"button\" data-state=\"" + (channel.comissionado ? "tested" : "pending") + "\"" + (canManage ? "" : " disabled") + ">" + (channel.comissionado ? "Testado" : "Testar") + "</button>" +
          "</div>";
        }).join("") +
        (canManage
          ? "<div class=\"form-actions\"><button class=\"btn btn-primary\" type=\"submit\">Salvar ajustes</button></div>"
          : "<div class=\"form-actions\"><span class=\"muted\">Modo somente leitura.</span></div>") +
      "</form>" +
      (canManage
        ? "<div class=\"rack-module-manage-launch\"><button class=\"btn btn-outline\" type=\"button\" data-rack-module-manage-open>Gerenciar modulo</button></div>"
        : "");

    syncSelectedCard(info.id);
    window.requestAnimationFrame(function () {
      syncPanelDock();
    });
    bindEditorInteractions(info);

    try {
      var url = new URL(window.location.href);
      url.searchParams.set("module", info.id);
      url.hash = "rack-module-panel";
      window.history.replaceState({}, "", url.toString());
    } catch (error) {}
  }

  moduleCards.forEach(function (card) {
    card.addEventListener("click", function () {
      renderPanel(card.dataset.moduleId);
    });
    var menu = card.querySelector(".slot-menu-btn");
    if (menu) {
      menu.addEventListener("click", function (event) {
        event.preventDefault();
        event.stopPropagation();
        renderPanel(card.dataset.moduleId);
        panel.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    }
  });

  if (activeModuleId && moduleData[String(activeModuleId)]) {
    renderPanel(activeModuleId);
  }

  if (rackTrack) {
    rackTrack.addEventListener("scroll", function () {
      syncPanelDock();
    }, { passive: true });
  }

  window.addEventListener("resize", function () {
    syncPanelDock();
  });

  if (panelBody) {
    panelBody.addEventListener("click", function (event) {
      var openButton = event.target.closest("[data-rack-module-manage-open]");
      if (!openButton) {
        return;
      }
      event.preventDefault();
      openManageModal();
    });
  }

  if (manageModal) {
    Array.from(document.querySelectorAll("[data-rack-module-manage-close]")).forEach(function (button) {
      button.addEventListener("click", function () {
        closeManageModal();
      });
    });
  }

  if (manageForm) {
    manageForm.addEventListener("submit", function (event) {
      event.preventDefault();
      var data = new FormData();
      data.set("action", "update_selected_module");
      data.set("module_id", manageModalId ? manageModalId.value : "");
      if (manageModalSlot && manageModalSlot.value) {
        data.set("slot_id", manageModalSlot.value);
      }
      fetch(window.location.pathname + window.location.search, {
        method: "POST",
        headers: {
          "X-Requested-With": "XMLHttpRequest",
          "X-CSRFToken": getCookie("csrftoken")
        },
        body: data
      }).then(function () {
        closeManageModal();
        window.location.href = window.location.pathname + "?module=" + (manageModalId ? manageModalId.value : "") + "#rack-module-panel";
      });
    });
  }

  if (deleteForm) {
    deleteForm.addEventListener("submit", function (event) {
      event.preventDefault();
      if (!window.confirm("Excluir modulo? Esta acao nao pode ser desfeita.")) {
        return;
      }
      var data = new FormData();
      data.set("action", "delete_selected_module");
      data.set("module_id", deleteModalId ? deleteModalId.value : "");
      fetch(window.location.pathname + window.location.search, {
        method: "POST",
        headers: {
          "X-Requested-With": "XMLHttpRequest",
          "X-CSRFToken": getCookie("csrftoken")
        },
        body: data
      }).then(function () {
        closeManageModal();
        window.location.href = window.location.pathname;
      });
    });
  }
})();
