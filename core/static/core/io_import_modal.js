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

  if (!modal || !form || !openButtons.length) {
    return;
  }

  var stepOrder = ["upload", "parse", "ai", "preview"];
  var stepText = {
    upload: "Arquivo recebido",
    parse: "Lendo a estrutura e localizando os sinais",
    ai: "IA correlacionando tipos, modulos e racks sugeridos",
    preview: "Montando a preview operacional final"
  };
  var progressTimers = [];
  var currentProgress = 0;
  var isSubmitting = false;

  function clearTimers() {
    progressTimers.forEach(function (timerId) {
      window.clearTimeout(timerId);
    });
    progressTimers = [];
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
    currentProgress = Math.max(0, Math.min(100, value));
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

  function resetProcessingView() {
    clearTimers();
    isSubmitting = false;
    modal.classList.remove("is-processing");
    updateProgress(0, "Preparando upload");
    stepOrder.forEach(function (stepName) {
      setStepState(stepName, "idle", "Aguardando");
    });
    if (titleNode) {
      titleNode.textContent = "Analisando planilha";
    }
    if (copyNode) {
      copyNode.textContent = "O arquivo foi recebido e o motor de importacao esta montando a estrutura dos racks sugeridos.";
    }
    if (processing) {
      processing.hidden = true;
      processing.style.display = "none";
    }
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
    updateProgress(6, "Arquivo enviado");
    setStepState("upload", "active", "Recebendo arquivo");

    progressTimers.push(window.setTimeout(function () {
      setStepState("upload", "done", "Concluido");
      setStepState("parse", "active", "Em processamento");
      updateProgress(28, "Lendo estrutura da planilha");
    }, 450));

    progressTimers.push(window.setTimeout(function () {
      setStepState("parse", "done", "Concluido");
      setStepState("ai", "active", "Em processamento");
      updateProgress(58, "Analisando com IA");
      if (titleNode) {
        titleNode.textContent = "Processamento inteligente em andamento";
      }
      if (copyNode) {
        copyNode.textContent = "A IA esta correlacionando colunas, sinais, agrupamentos fisicos e sugestoes de rack para gerar a preview.";
      }
    }, 1500));

    progressTimers.push(window.setTimeout(function () {
      setStepState("ai", "active", "Refinando sugestoes");
      updateProgress(76, "Consolidando sugestoes");
    }, 3600));

    progressTimers.push(window.setTimeout(function () {
      setStepState("ai", "done", "Concluido");
      setStepState("preview", "active", "Em processamento");
      updateProgress(90, "Gerando preview operacional");
      if (titleNode) {
        titleNode.textContent = "Quase pronto";
      }
      if (copyNode) {
        copyNode.textContent = "A estrutura final esta sendo organizada para abrir a preview dos racks sugeridos.";
      }
    }, 6200));
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
      copyNode.textContent = "A preview foi gerada. Abrindo o resultado da importacao.";
    }
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
          throw new Error(
            "O servidor retornou uma resposta inesperada (HTTP " +
            response.status +
            "). Verifique o log do Django ou a aba Network."
          );
        }
        if (!response.ok) {
          throw new Error((payload && payload.message) || ("Falha ao enviar a planilha (HTTP " + response.status + ")."));
        }
        return payload;
      });
    }).then(function (payload) {
      if (!payload || !payload.ok || !payload.redirect_url) {
        throw new Error((payload && payload.message) || "Nao foi possivel concluir a analise.");
      }
      finishProcessingView();
      window.setTimeout(function () {
        window.location.href = payload.redirect_url;
      }, 700);
    }).catch(function (error) {
      resetProcessingView();
      setError(error && error.message ? error.message : "Falha ao enviar a planilha para analise.");
    });
  });
})();
