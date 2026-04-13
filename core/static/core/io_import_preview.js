(function () {
  function byId(id) {
    return document.getElementById(id);
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  var payloadNode = byId("io-import-preview-payload");
  if (!payloadNode) {
    return;
  }

  var previewPayload = JSON.parse(payloadNode.textContent || "{}");

  function syncScrollState(root) {
    var track = root.querySelector("[data-preview-track]");
    var scrollUi = root.querySelector("[data-preview-scroll-ui]");
    var scrollBall = root.querySelector("[data-preview-scroll-ball]");
    if (!track || !scrollUi || !scrollBall) {
      return;
    }
    var maxScroll = Math.max(0, track.scrollWidth - track.clientWidth);
    var ballWidth = scrollBall.offsetWidth || 0;
    var usable = Math.max(0, scrollUi.clientWidth - ballWidth);
    var progress = maxScroll > 0 ? (track.scrollLeft / maxScroll) : 0;
    scrollUi.classList.toggle("is-hidden", maxScroll <= 2);
    scrollBall.style.transform = "translate(" + (usable * progress) + "px, -50%)";
    var carousel = track.closest(".rack-carousel");
    if (carousel) {
      var left = track.scrollLeft;
      var epsilon = 2;
      carousel.classList.toggle("can-scroll-left", left > epsilon);
      carousel.classList.toggle("can-scroll-right", (left + track.clientWidth) < (track.scrollWidth - epsilon));
    }
  }

  function bindScrollUi(root) {
    var track = root.querySelector("[data-preview-track]");
    var scrollUi = root.querySelector("[data-preview-scroll-ui]");
    var scrollBall = root.querySelector("[data-preview-scroll-ball]");
    if (!track || !scrollUi || !scrollBall) {
      return;
    }

    var dragging = false;

    function setFromPointer(clientX) {
      var maxScroll = Math.max(0, track.scrollWidth - track.clientWidth);
      var rect = scrollUi.getBoundingClientRect();
      var ballWidth = scrollBall.offsetWidth || 0;
      var usable = Math.max(0, rect.width - ballWidth);
      var raw = clientX - rect.left - (ballWidth / 2);
      var clamped = Math.min(Math.max(raw, 0), usable);
      var progress = usable > 0 ? (clamped / usable) : 0;
      track.scrollLeft = progress * maxScroll;
    }

    track.addEventListener("scroll", function () {
      syncScrollState(root);
      syncPanelDock(root);
    }, { passive: true });

    window.addEventListener("resize", function () {
      syncScrollState(root);
      syncPanelDock(root);
    });

    scrollUi.addEventListener("pointerdown", function (event) {
      dragging = true;
      if (scrollUi.setPointerCapture) {
        scrollUi.setPointerCapture(event.pointerId);
      }
      setFromPointer(event.clientX);
    });

    scrollUi.addEventListener("pointermove", function (event) {
      if (!dragging) {
        return;
      }
      setFromPointer(event.clientX);
    });

    ["pointerup", "pointercancel", "lostpointercapture"].forEach(function (eventName) {
      scrollUi.addEventListener(eventName, function () {
        dragging = false;
      });
    });

    syncScrollState(root);
  }

  function clearSelected(root) {
    Array.from(root.querySelectorAll(".js-import-preview-card")).forEach(function (card) {
      card.classList.remove("slot-selected");
      var shell = card.closest(".rack-slot-shell");
      if (shell) {
        shell.classList.remove("slot-active");
      }
    });
  }

  function syncPanelDock(root) {
    var panel = root.querySelector("[data-preview-panel]");
    var activeCard = root.querySelector(".js-import-preview-card.slot-selected");
    if (!panel || !activeCard) {
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

  function buildMetaPills(moduleData) {
    var parts = [];
    if (moduleData.type_name) {
      parts.push("<span class=\"module-meta-pill\">" + escapeHtml(moduleData.type_name) + "</span>");
    }
    parts.push("<span class=\"module-meta-pill\">Slot " + escapeHtml(String(moduleData.slot_pos).padStart(2, "0")) + "</span>");
    parts.push("<span class=\"module-meta-pill\">" + escapeHtml(String(moduleData.channels.length)) + " canais</span>");
    if (moduleData.source && moduleData.source !== "-") {
      parts.push("<span class=\"module-meta-pill\">Origem " + escapeHtml(moduleData.source) + "</span>");
    }
    return parts.join("");
  }

  function buildChannelRows(channels) {
    return channels.map(function (channel) {
      return "<div class=\"rack-channel-row\" title=\"" + escapeHtml(channel.descricao || "") + "\">" +
        "<span>" + escapeHtml(channel.canal) + "</span>" +
        "<span>" + escapeHtml(channel.tag || "-") + "</span>" +
        "<span>" + escapeHtml(channel.tipo || "-") + "</span>" +
      "</div>";
    }).join("");
  }

  function renderPanel(root, moduleId) {
    var rackKey = root.getAttribute("data-rack-key") || "";
    var moduleData = (previewPayload[rackKey] || {})[moduleId];
    if (!moduleData) {
      return;
    }

    clearSelected(root);
    Array.from(root.querySelectorAll(".js-import-preview-card")).forEach(function (card) {
      if (String(card.getAttribute("data-module-id")) === String(moduleId)) {
        card.classList.add("slot-selected");
        var shell = card.closest(".rack-slot-shell");
        if (shell) {
          shell.classList.add("slot-active");
        }
      }
    });

    var note = root.querySelector("[data-preview-note]");
    var panel = root.querySelector("[data-preview-panel]");
    var title = root.querySelector("[data-preview-panel-title]");
    var subtitle = root.querySelector("[data-preview-panel-subtitle]");
    var meta = root.querySelector("[data-preview-panel-meta]");
    var rowsWrap = root.querySelector("[data-preview-list]");
    var rowsContainer = root.querySelector("[data-preview-rows]");
    var emptyState = root.querySelector("[data-preview-empty]");

    if (note) {
      note.classList.add("is-hidden");
    }
    if (panel) {
      panel.classList.remove("is-hidden");
    }
    if (title) {
      title.textContent = moduleData.display_name;
    }
    if (subtitle) {
      subtitle.textContent = [moduleData.brand, moduleData.type_name].filter(Boolean).join(" - ") || "Canais previstos do modulo.";
    }
    if (meta) {
      meta.innerHTML = buildMetaPills(moduleData);
    }

    if (!moduleData.channels.length) {
      if (rowsContainer) {
        rowsContainer.innerHTML = "";
      }
      if (rowsWrap) {
        rowsWrap.classList.add("is-hidden");
        rowsWrap.style.display = "none";
      }
      if (emptyState) {
        emptyState.classList.remove("is-hidden");
        emptyState.style.display = "";
      }
    } else {
      if (rowsContainer) {
        rowsContainer.innerHTML = buildChannelRows(moduleData.channels);
      }
      if (emptyState) {
        emptyState.classList.add("is-hidden");
        emptyState.style.display = "none";
      }
      if (rowsWrap) {
        rowsWrap.classList.remove("is-hidden");
        rowsWrap.style.display = "";
      }
    }

    syncPanelDock(root);
  }

  Array.from(document.querySelectorAll("[data-import-preview-root]")).forEach(function (root) {
    bindScrollUi(root);

    Array.from(root.querySelectorAll(".js-import-preview-card")).forEach(function (card) {
      card.addEventListener("click", function () {
        renderPanel(root, card.getAttribute("data-module-id"));
      });
    });

    var selectedModuleId = root.getAttribute("data-selected-module-id") || "";
    if (selectedModuleId) {
      renderPanel(root, selectedModuleId);
    }
  });

  var leaveModal = document.querySelector("[data-io-import-leave-modal]");
  var leaveCopy = document.querySelector("[data-io-import-leave-copy]");
  var leaveCancel = document.querySelector("[data-io-import-leave-cancel]");
  var leaveConfirm = document.querySelector("[data-io-import-leave-confirm]");

  function closeLeaveModal() {
    if (!leaveModal) {
      return;
    }
    leaveModal.classList.remove("is-open");
    leaveModal.setAttribute("aria-hidden", "true");
  }

  function openLeaveModal(pendingCount, href) {
    if (!leaveModal || !leaveCopy || !leaveConfirm) {
      return;
    }
    var label = pendingCount === 1 ? "1 rack para revisao" : String(pendingCount) + " racks para revisao";
    leaveCopy.textContent = "Voce ainda possui " + label + ". Deseja continuar para a pagina do modulo de IOs agora?";
    leaveConfirm.setAttribute("href", href || leaveConfirm.getAttribute("href") || "#");
    leaveModal.classList.add("is-open");
    leaveModal.setAttribute("aria-hidden", "false");
  }

  if (leaveModal) {
    leaveModal.addEventListener("click", function (event) {
      if (event.target === leaveModal) {
        closeLeaveModal();
      }
    });
  }

  if (leaveCancel) {
    leaveCancel.addEventListener("click", function () {
      closeLeaveModal();
    });
  }

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") {
      closeLeaveModal();
    }
  });

  Array.from(document.querySelectorAll("[data-open-io-module]")).forEach(function (link) {
    link.addEventListener("click", function (event) {
      var pendingCount = parseInt(link.getAttribute("data-pending-racks") || "0", 10) || 0;
      if (pendingCount <= 0) {
        return;
      }
      event.preventDefault();
      openLeaveModal(pendingCount, link.getAttribute("href"));
    });
  });
})();
