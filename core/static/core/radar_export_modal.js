(function () {
  var openButton = document.getElementById("radar-export-open");
  var modal = document.getElementById("radar-export-modal");
  if (!openButton || !modal) {
    return;
  }

  var closeButtons = modal.querySelectorAll("[data-radar-export-close]");
  var submitButton = document.getElementById("radar-export-submit");
  var monthInput = document.getElementById("radar-export-month");
  var errorBox = document.getElementById("radar-export-error");
  var exportUrl = modal.getAttribute("data-export-url") || "";

  function setError(message) {
    if (!errorBox) {
      return;
    }
    if (!message) {
      errorBox.textContent = "";
      errorBox.style.display = "none";
      return;
    }
    errorBox.textContent = message;
    errorBox.style.display = "block";
  }

  function setBusy(isBusy) {
    if (!submitButton) {
      return;
    }
    submitButton.disabled = !!isBusy;
    submitButton.textContent = isBusy ? "Gerando..." : "Gerar relatorio";
  }

  function openModal() {
    modal.removeAttribute("hidden");
    modal.classList.add("is-open");
    document.body.classList.add("radar-export-modal-open");
    setError("");
    if (monthInput) {
      monthInput.focus();
    }
  }

  function closeModal() {
    modal.classList.remove("is-open");
    modal.setAttribute("hidden", "hidden");
    document.body.classList.remove("radar-export-modal-open");
    setBusy(false);
    setError("");
  }

  function parseFilename(contentDisposition) {
    var raw = String(contentDisposition || "");
    var utfMatch = raw.match(/filename\*=UTF-8''([^;]+)/i);
    if (utfMatch && utfMatch[1]) {
      try {
        return decodeURIComponent(utfMatch[1]);
      } catch (e) {
        return utfMatch[1];
      }
    }
    var simpleMatch = raw.match(/filename=\"?([^\";]+)\"?/i);
    return simpleMatch && simpleMatch[1] ? simpleMatch[1] : "";
  }

  function buildJsonError(response) {
    return response.text().then(function (text) {
      if (!text) {
        return { message: "Falha ao gerar relatorio." };
      }
      try {
        var payload = JSON.parse(text);
        return payload || {};
      } catch (e) {
        return { message: text };
      }
    });
  }

  function triggerDownload(blob, filename) {
    var anchor = document.createElement("a");
    var blobUrl = window.URL.createObjectURL(blob);
    anchor.href = blobUrl;
    anchor.download = filename || "relatorio_radar.pdf";
    document.body.appendChild(anchor);
    anchor.click();
    document.body.removeChild(anchor);
    window.URL.revokeObjectURL(blobUrl);
  }

  function submitExport() {
    if (!exportUrl) {
      setError("URL de exportacao indisponivel.");
      return;
    }
    var monthValue = monthInput ? String(monthInput.value || "").trim() : "";
    if (!/^\d{4}-\d{2}$/.test(monthValue)) {
      setError("Selecione um mes valido.");
      return;
    }
    setError("");
    setBusy(true);

    var url = new URL(exportUrl, window.location.origin);
    url.searchParams.set("mes", monthValue);

    fetch(url.toString(), {
      method: "GET",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    })
      .then(function (response) {
        if (!response.ok) {
          return buildJsonError(response).then(function (payload) {
            throw payload;
          });
        }
        var contentType = response.headers.get("Content-Type") || "";
        if (contentType.indexOf("application/pdf") === -1) {
          throw { message: "Resposta inesperada ao gerar PDF." };
        }
        return response.blob().then(function (blob) {
          var filename = parseFilename(response.headers.get("Content-Disposition"));
          triggerDownload(blob, filename || ("relatorio_radar_" + monthValue + ".pdf"));
          closeModal();
        });
      })
      .catch(function (error) {
        setError((error && error.message) || "Nao foi possivel gerar o relatorio.");
      })
      .finally(function () {
        setBusy(false);
      });
  }

  openButton.addEventListener("click", openModal);
  closeButtons.forEach(function (button) {
    button.addEventListener("click", closeModal);
  });
  modal.addEventListener("click", function (event) {
    var target = event.target;
    if (target && target.hasAttribute("data-radar-export-close")) {
      closeModal();
    }
  });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && modal.classList.contains("is-open")) {
      closeModal();
    }
  });
  if (submitButton) {
    submitButton.addEventListener("click", submitExport);
  }
})();
