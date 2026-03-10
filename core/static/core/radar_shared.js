(function (global) {
  function getCookie(name) {
    var value = "; " + document.cookie;
    var parts = value.split("; " + name + "=");
    if (parts.length === 2) {
      return parts.pop().split(";").shift();
    }
    return "";
  }

  function setNotice(messageId, message, level) {
    var box = document.getElementById(messageId);
    if (!box) {
      return;
    }
    box.textContent = message;
    box.className = "notice notice-" + (level || "info");
    box.style.display = "block";
  }

  function bindCadastro(options) {
    if (!options) {
      return;
    }
    var formCadastro = document.getElementById(options.formId);
    var input = document.querySelector(options.inputSelector);
    var select = options.selectId ? document.getElementById(options.selectId) : null;
    var details = document.getElementById(options.detailsId);
    if (!formCadastro || !input || !details) {
      return;
    }

    formCadastro.addEventListener("submit", function (event) {
      event.preventDefault();
      var submitButton = formCadastro.querySelector("button[type='submit']");
      if (submitButton) {
        submitButton.disabled = true;
      }
      var value = input.value.trim();
      if (!value) {
        setNotice(options.messageId || "cadastro-message", "Informe um nome valido.", "error");
        details.open = true;
        if (submitButton) {
          submitButton.disabled = false;
        }
        return;
      }
      var data = new FormData(formCadastro);
      if (!data.get(input.name)) {
        data.set(input.name, value);
      }
      fetch(window.location.pathname + window.location.search, {
        method: "POST",
        headers: {
          "X-Requested-With": "XMLHttpRequest",
          "X-CSRFToken": getCookie("csrftoken"),
        },
        body: data,
      })
        .then(function (resp) {
          return resp
            .text()
            .then(function (text) {
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
        })
        .then(function (payload) {
          if (select && payload && payload.id && payload.nome) {
            var exists = Array.from(select.options).some(function (opt) {
              return opt.value === String(payload.id);
            });
            if (!exists) {
              var option = document.createElement("option");
              option.value = payload.id;
              option.textContent = payload.nome;
              select.appendChild(option);
            }
            select.value = String(payload.id);
          }
          if (payload && payload.message) {
            setNotice(options.messageId || "cadastro-message", payload.message, payload.level);
          }
          if (payload && payload.ok) {
            input.value = "";
            details.open = false;
          } else {
            details.open = true;
          }
        })
        .catch(function (errPayload) {
          var message = (errPayload && errPayload.message) || "Nao foi possivel salvar agora.";
          setNotice(options.messageId || "cadastro-message", message, "error");
          details.open = true;
        })
        .finally(function () {
          if (submitButton) {
            submitButton.disabled = false;
          }
        });
    });
  }

  global.RadarShared = {
    getCookie: getCookie,
    bindCadastro: bindCadastro,
  };
})(window);
