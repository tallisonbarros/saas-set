(function () {
  function byId(id) {
    return document.getElementById(id);
  }

  function getCookie(name) {
    var value = "; " + document.cookie;
    var parts = value.split("; " + name + "=");
    return parts.length === 2 ? parts.pop().split(";").shift() : "";
  }

  var modal = byId("io-import-modal");
  var openButtons = Array.from(document.querySelectorAll("[data-io-import-open]"));
  var closeButtons = Array.from(document.querySelectorAll("[data-io-import-close]"));
  var form = modal ? modal.querySelector("[data-io-import-form]") : null;
  var errorBox = modal ? modal.querySelector("[data-io-import-error]") : null;
  var processing = modal ? modal.querySelector("[data-io-import-processing]") : null;
  var submitButton = modal ? modal.querySelector("[data-io-import-submit]") : null;
  var titleNode = modal ? modal.querySelector("[data-io-import-title]") : null;
  var copyNode = modal ? modal.querySelector("[data-io-import-copy]") : null;
  var progressBar = modal ? modal.querySelector("[data-io-import-progress-bar]") : null;
  var progressLabel = modal ? modal.querySelector("[data-io-import-progress-label]") : null;
  var progressValue = modal ? modal.querySelector("[data-io-import-progress-value]") : null;
  var sheetMeta = modal ? modal.querySelector("[data-io-import-sheet-meta]") : null;
  var currentSheetNode = modal ? modal.querySelector("[data-io-import-current-sheet]") : null;
  var sheetCountNode = modal ? modal.querySelector("[data-io-import-sheet-count]") : null;
  var livePreview = modal ? modal.querySelector("[data-io-import-live-preview]") : null;
  var livePreviewGrid = modal ? modal.querySelector("[data-io-import-live-preview-grid]") : null;

  if (!modal || !form || !openButtons.length) {
    return;
  }

  var stepOrder = ["upload", "parse", "ai", "preview"];
  var statusPollTimer = null;
  var isSubmitting = false;

  function clearTimers() {
    if (statusPollTimer) {
      window.clearTimeout(statusPollTimer);
      statusPollTimer = null;
    }
  }

  function setError(message) {
    if (!errorBox) {
      return;
    }
    errorBox.textContent = message || "";
    errorBox.classList.toggle("is-visible", !!message);
  }

  function setStepState(stepName, state, label) {
    var step = modal.querySelector('[data-io-import-step="' + stepName + '"]');
    var status = modal.querySelector('[data-io-import-step-status="' + stepName + '"]');
    if (!step) {
      return;
    }
    step.classList.toggle("is-active", state === "active");
    step.classList.toggle("is-done", state === "done");
    if (status) {
      status.textContent = label || (state === "done" ? "Concluido" : state === "active" ? "Em processamento" : "Aguardando");
    }
  }

  function updateProgress(value, label) {
    var currentProgress = Math.max(0, Math.min(100, value));
    if (progressBar) {
      progressBar.style.width = currentProgress + "%";
    }
    if (progressValue) {
      progressValue.textContent = Math.round(currentProgress) + "%";
    }
    if (progressLabel && label) {
      progressLabel.textContent = label;
    }
  }

  function setSnapshots(items) {
    if (!livePreview || !livePreviewGrid) {
      return;
    }
    var snapshots = Array.isArray(items) ? items.filter(Boolean) : [];
    if (!snapshots.length) {
      livePreview.hidden = true;
      livePreviewGrid.classList.remove("is-static");
      livePreviewGrid.innerHTML = "";
      return;
    }

    var labels = snapshots.map(function (snapshot) {
      return snapshot.rack_name || "Rack em analise";
    }).filter(Boolean);
    var uniqueLabels = Array.from(new Set(labels));
    var itemsHtml = uniqueLabels.map(function (label) {
      return '<span class="io-import-live-preview-item">' + label + "</span>";
    }).join("");

    livePreview.hidden = false;
    if (uniqueLabels.join(" · ").length <= 56) {
      livePreviewGrid.classList.add("is-static");
      livePreviewGrid.innerHTML = itemsHtml;
      return;
    }
    livePreviewGrid.classList.remove("is-static");
    livePreviewGrid.innerHTML = itemsHtml + itemsHtml;
  }

  function setSheetMeta(currentSheet, processed, total, currentIndex) {
    if (!sheetMeta || !currentSheetNode || !sheetCountNode) {
      return;
    }
    var hasSheet = !!currentSheet;
    var hasCount = total > 0;
    if (!hasSheet && !hasCount) {
      sheetMeta.classList.remove("is-visible");
      currentSheetNode.textContent = "";
      sheetCountNode.textContent = "";
      return;
    }
    sheetMeta.classList.add("is-visible");
    currentSheetNode.textContent = currentSheet || "Preparando leitura";
    if (!hasCount) {
      sheetCountNode.textContent = "";
      return;
    }
    var visibleIndex = currentIndex || processed || 0;
    sheetCountNode.textContent = "Guias " + visibleIndex + "/" + total;
  }

  function renderProgressPayload(progress) {
    var payload = progress || {};
    var progressLabelText = payload.progress_label;
    if (!progressLabelText) {
      progressLabelText = payload.current_sheet && payload.sheets_total
        ? ("Guia " + (payload.current_sheet_index || payload.sheets_processed || 0) + " de " + payload.sheets_total)
        : (payload.title || "Processando importacao");
    }
    updateProgress(payload.percent || 0, progressLabelText);
    if (titleNode && payload.title) {
      titleNode.textContent = payload.title;
    }
    if (copyNode && payload.message) {
      copyNode.textContent = payload.message;
    }
    var steps = payload.steps || {};
    stepOrder.forEach(function (stepName) {
      var state = steps[stepName] || "idle";
      var label = state === "done" ? "Concluido" : state === "active" ? "Em processamento" : "Aguardando";
      setStepState(stepName, state, label);
    });
    setSheetMeta(
      payload.current_sheet || "",
      payload.sheets_processed || 0,
      payload.sheets_total || 0,
      payload.current_sheet_index || 0
    );
    setSnapshots(payload.snapshots || []);
  }

  function resetProcessingView() {
    clearTimers();
    isSubmitting = false;
    modal.classList.remove("is-processing");
    updateProgress(0, "Preparando upload");
    stepOrder.forEach(function (stepName) {
      setStepState(stepName, "idle", "Aguardando");
    });
    if (titleNode) {
      titleNode.textContent = "Analisando a planilha";
    }
    if (copyNode) {
      copyNode.textContent = "O arquivo foi recebido e a estrutura da importacao esta sendo organizada para revisao.";
    }
    if (processing) {
      processing.hidden = true;
      processing.style.display = "none";
    }
    setSheetMeta("", 0, 0);
    setSnapshots([]);
    form.hidden = false;
    form.style.display = "";
    if (submitButton) {
      submitButton.disabled = false;
    }
    setError("");
  }

  function openModal() {
    resetProcessingView();
    modal.hidden = false;
    modal.classList.add("is-open");
    document.body.classList.add("radar-export-modal-open");
  }

  function closeModal() {
    if (isSubmitting) {
      return;
    }
    modal.classList.remove("is-open");
    modal.hidden = true;
    document.body.classList.remove("radar-export-modal-open");
    resetProcessingView();
  }

  function startProcessingView() {
    isSubmitting = true;
    modal.classList.add("is-processing");
    setError("");
    form.hidden = true;
    form.style.display = "none";
    if (processing) {
      processing.hidden = false;
      processing.style.display = "";
    }
    if (submitButton) {
      submitButton.disabled = true;
    }
    renderProgressPayload({
      stage: "upload",
      percent: 6,
      title: "Arquivo recebido",
      message: "O arquivo foi recebido e a leitura inicial da planilha esta comecando.",
      steps: { upload: "active", parse: "idle", ai: "idle", preview: "idle" },
      snapshots: [],
      sheets_total: 0,
      sheets_processed: 0
    });
  }

  function finishProcessingView() {
    clearTimers();
    stepOrder.forEach(function (stepName) {
      setStepState(stepName, "done", "Concluido");
    });
    updateProgress(100, "Preview pronta");
    if (titleNode) {
      titleNode.textContent = "Analise concluida";
    }
    if (copyNode) {
      copyNode.textContent = "A previa foi gerada. Abrindo o resultado da importacao.";
    }
  }

  function redirectToResult(url) {
    finishProcessingView();
    window.setTimeout(function () {
      window.location.href = url;
    }, 500);
  }

  function pollJobStatus(statusUrl, redirectUrl) {
    function pollOnce() {
      fetch(statusUrl, {
        method: "GET",
        credentials: "same-origin",
        headers: {
          "X-Requested-With": "XMLHttpRequest"
        }
      }).then(function (response) {
        return response.text().then(function (rawText) {
          var payload = null;
          try {
            payload = rawText ? JSON.parse(rawText) : null;
          } catch (parseError) {
            console.error("IO import status returned non-JSON response", {
              status: response.status,
              contentType: response.headers.get("content-type"),
              body: (rawText || "").slice(0, 2000)
            });
            throw new Error("Nao foi possivel acompanhar a analise da planilha neste momento.");
          }
          if (!response.ok) {
            throw new Error((payload && payload.message) || "Falha ao acompanhar a importacao.");
          }
          return payload;
        });
      }).then(function (payload) {
        if (!payload || !payload.ok) {
          throw new Error((payload && payload.message) || "Nao foi possivel acompanhar a importacao.");
        }
        if (payload.progress) {
          renderProgressPayload(payload.progress);
        }
        if (payload.complete) {
          if (payload.failed && titleNode) {
            titleNode.textContent = "Analise interrompida";
          }
          if (payload.failed && copyNode) {
            copyNode.textContent = payload.message || "A analise nao conseguiu ser concluida. Abrindo os detalhes para revisao.";
          }
          redirectToResult(payload.redirect_url || redirectUrl);
          return;
        }
        statusPollTimer = window.setTimeout(pollOnce, 1800);
      }).catch(function (error) {
        resetProcessingView();
        setError(error && error.message ? error.message : "Falha ao acompanhar a analise da planilha.");
      });
    }

    statusPollTimer = window.setTimeout(pollOnce, 900);
  }

  openButtons.forEach(function (button) {
    button.addEventListener("click", openModal);
  });

  closeButtons.forEach(function (button) {
    button.addEventListener("click", closeModal);
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && modal.classList.contains("is-open")) {
      closeModal();
    }
  });

  form.addEventListener("submit", function (event) {
    event.preventDefault();
    var fileInput = form.querySelector('input[type="file"][name="arquivo"]');
    if (!fileInput || !fileInput.files || !fileInput.files.length) {
      setError("Selecione um arquivo para enviar.");
      return;
    }

    startProcessingView();

    fetch(form.action, {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRFToken": getCookie("csrftoken")
      },
      body: new FormData(form)
    }).then(function (response) {
      return response.text().then(function (rawText) {
        var payload = null;
        try {
          payload = rawText ? JSON.parse(rawText) : null;
        } catch (parseError) {
          console.error("IO import returned non-JSON response", {
            status: response.status,
            contentType: response.headers.get("content-type"),
            body: (rawText || "").slice(0, 2000)
          });
          throw new Error("O servidor nao conseguiu iniciar a analise agora. Tente novamente em instantes.");
        }
        if (!response.ok) {
          throw new Error((payload && payload.message) || "Falha ao enviar a planilha.");
        }
        return payload;
      });
    }).then(function (payload) {
      if (!payload || !payload.ok || !payload.redirect_url) {
        throw new Error((payload && payload.message) || "Nao foi possivel concluir a analise.");
      }
      if (payload.status_url) {
        pollJobStatus(payload.status_url, payload.redirect_url);
        return;
      }
      redirectToResult(payload.redirect_url);
    }).catch(function (error) {
      resetProcessingView();
      setError(error && error.message ? error.message : "Falha ao enviar a planilha para analise.");
    });
  });
})();
