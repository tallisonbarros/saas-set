(function () {
  if (!window.SAASDataGrid || !window.SAASDataGrid.utils) {
    return;
  }

  var utils = window.SAASDataGrid.utils;
  var root = document.getElementById("radar-atividades-grid");
  if (!root) {
    return;
  }

  var config = window.RadarAtividadesTableConfig || {};
  var canManage = !!config.canManage;
  var rows = utils.parseJsonScript("radar-atividades-data");
  var trabalhoId = root.getAttribute("data-dg-scope") || "global";

  function setupDescriptionMarquees(scopeEl) {
    if (!scopeEl) {
      return;
    }
    var marquees = scopeEl.querySelectorAll(".radar-desc-marquee");
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

  function setPageMessage(message, level) {
    var box = document.getElementById("cadastro-message");
    if (!box) {
      return;
    }
    box.textContent = message;
    box.className = "notice notice-" + (level || "info");
    box.style.display = "block";
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

  function postFormData(data) {
    var getCookie = window.RadarShared && window.RadarShared.getCookie
      ? window.RadarShared.getCookie
      : function () {
          return "";
        };
    return fetch(window.location.pathname + window.location.search, {
      method: "POST",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRFToken": getCookie("csrftoken"),
      },
      body: data,
    }).then(parseJsonResponse);
  }

  function payloadToRowPatch(payload) {
    return {
      nome: payload.nome || "",
      descricao: payload.descricao || "",
      status: payload.status || "",
      status_label: payload.status_label || payload.status || "",
      horas_trabalho: payload.horas_trabalho || "",
      inicio_execucao_display: payload.inicio_execucao_display || "",
      finalizada_display: payload.finalizada_display || "",
    };
  }

  function canReorderForState(state) {
    if (!state) {
      return false;
    }
    if (!state.sortCol) {
      return true;
    }
    return state.sortCol === "ordem" && state.sortDir === "asc";
  }

  var columns = [
    {
      key: "nome",
      label: "Nome",
      visible: true,
      fixed: true,
      flex: true,
      minWidth: 0,
      cellClass: "radar-col-nome",
      filter: { type: "text", placeholder: "Filtrar" },
      render: function (row, ctx) {
        var nome = ctx.esc(row.nome || "-");
        var descricao = ctx.esc(row.descricao || "Atividade sem descricao.");
        var label = nome;
        if (canManage) {
          label =
            '<button class="radar-row-link radar-link-btn js-editar-atividade" type="button" data-atividade-id="' +
            ctx.esc(row.id) +
            '">' +
            nome +
            "</button>";
        }
        return (
          label +
          '<div class="radar-desc-marquee" title="' +
          descricao +
          '">' +
          '<span class="radar-desc-marquee-text">' +
          descricao +
          "</span></div>"
        );
      },
    },
    {
      key: "descricao",
      label: "Descricao",
      visible: false,
      width: 260,
      minWidth: 220,
      cellClass: "radar-cell-wrap",
      filter: { type: "text", placeholder: "Filtrar" },
    },
    {
      key: "status",
      label: "Status",
      visible: true,
      width: 140,
      minWidth: 140,
      filter: {
        type: "select",
        options: [
          { value: "EXECUTANDO", label: "Executando" },
          { value: "PENDENTE", label: "Pendente" },
          { value: "FINALIZADA", label: "Finalizada" },
        ],
      },
      render: function (row, ctx) {
        return ctx.statusBadge(row.status, row.status_label);
      },
    },
    {
      key: "horas_trabalho",
      label: "Horas",
      visible: false,
      width: 120,
      minWidth: 120,
      compareType: "number",
      filter: { type: "number", min: 0, step: 0.1, placeholder: "0" },
      render: function (row, ctx) {
        var value = row.horas_trabalho || "";
        return value ? ctx.slotBadge(value, "h") : "-";
      },
    },
    {
      key: "inicio_execucao_display",
      label: "Inicio",
      visible: false,
      width: 170,
      minWidth: 150,
      filter: { type: "text", placeholder: "Filtrar" },
    },
    {
      key: "finalizada_display",
      label: "Finalizacao",
      visible: false,
      width: 170,
      minWidth: 150,
      filter: { type: "text", placeholder: "Filtrar" },
    },
    {
      key: "ordem",
      label: "Ordem",
      visible: false,
      width: 100,
      minWidth: 90,
      compareType: "number",
      filter: { type: "number", min: 0, step: 1, placeholder: "0" },
    },
  ];

  var grid = window.SAASDataGrid.create({
    rootId: "radar-atividades-grid",
    storageKey: "radar-atividades:v2:" + trabalhoId,
    rows: rows,
    pageSize: 20,
    pageSizeOptions: [10, 20, 50, 100],
    defaultSort: { col: "ordem", dir: "asc" },
    noRowsText: "Nenhuma atividade cadastrada.",
    summaryFormatter: function (total) {
      return total + " atividade(s) encontrada(s)";
    },
    create: canManage
      ? {
          enabled: true,
          submitIcon: true,
          submitAriaLabel: "Salvar atividade",
          submitPosition: "start",
          fields: [
            { name: "action", type: "hidden", value: "create_atividade" },
            { name: "nome", label: "Nome", type: "text", placeholder: "Nome da atividade", required: true },
            { name: "descricao", label: "Descricao", type: "text", placeholder: "Descricao resumida" },
          ],
          onSubmit: function (ctx) {
            return postFormData(ctx.formData)
              .then(function (payload) {
                if (!payload || !payload.ok || !payload.row) {
                  return { ok: false, message: "Nao foi possivel criar a atividade." };
                }
                return {
                  ok: true,
                  row: payload.row,
                  message: payload.message || "Atividade criada.",
                  level: payload.level || "success",
                };
              })
              .catch(function (err) {
                return {
                  ok: false,
                  message: (err && err.message) || "Nao foi possivel criar a atividade.",
                  level: "error",
                };
              });
          },
        }
      : { enabled: false },
    columns: columns,
    rowReorder: canManage
      ? {
          enabled: true,
          isEnabled: function (state) {
            return canReorderForState(state);
          },
          onMove: function (ctx) {
            var data = new FormData();
            data.set("action", "move_atividade_to");
            data.set("atividade_id", ctx.sourceId);
            data.set("target_atividade_id", ctx.targetId);
            return postFormData(data).then(function (payload) {
              if (!payload || !payload.ok || !payload.moved) {
                setPageMessage("Nao foi possivel mover a atividade.", "warning");
                return false;
              }
              return true;
            }).catch(function () {
              setPageMessage("Nao foi possivel mover a atividade.", "error");
              return false;
            });
          },
        }
      : { enabled: false },
    onAfterRender: function (api) {
      setupDescriptionMarquees(api.root);
    },
    onResize: function (api) {
      setupDescriptionMarquees(api.root);
    },
  });

  if (!grid) {
    return;
  }

  var editor = document.getElementById("editar-atividade");
  var updateForm = editor ? editor.querySelector("form.io-form") : null;
  var deleteForm = editor ? editor.querySelector("form#delete-atividade-form") : null;
  var cancelButton = document.getElementById("cancelar-edicao-atividade");

  function setField(form, name, value) {
    if (!form) {
      return;
    }
    var input = form.querySelector("[name='" + name + "']");
    if (input) {
      input.value = value || "";
    }
  }

  function hideEditor() {
    if (!editor) {
      return;
    }
    if (updateForm) {
      updateForm.reset();
      setField(updateForm, "atividade_id", "");
    }
    if (deleteForm) {
      setField(deleteForm, "atividade_id", "");
      var deleteButton = deleteForm.querySelector("button[type='submit']");
      if (deleteButton) {
        deleteButton.disabled = true;
      }
    }
    editor.style.display = "none";
  }

  function openEditorById(atividadeId) {
    if (!editor || !updateForm) {
      return;
    }
    var row = grid.getRowById(atividadeId);
    if (!row) {
      setPageMessage("Atividade nao encontrada.", "warning");
      return;
    }
    editor.style.display = "";
    setField(updateForm, "atividade_id", row.id);
    setField(updateForm, "nome", row.nome);
    setField(updateForm, "descricao", row.descricao);
    setField(updateForm, "horas_trabalho", row.horas_trabalho);
    setField(updateForm, "status", row.status);
    setField(updateForm, "inicio_execucao_display", row.inicio_execucao_display);
    setField(updateForm, "finalizada_display", row.finalizada_display);
    if (deleteForm) {
      setField(deleteForm, "atividade_id", row.id);
      var deleteButton = deleteForm.querySelector("button[type='submit']");
      if (deleteButton) {
        deleteButton.disabled = false;
      }
    }
    editor.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  root.addEventListener("click", function (event) {
    var editButton = event.target.closest(".js-editar-atividade");
    if (editButton) {
      event.preventDefault();
      openEditorById(editButton.dataset.atividadeId);
    }
  });

  if (cancelButton) {
    cancelButton.addEventListener("click", function () {
      hideEditor();
    });
  }

  if (updateForm) {
    updateForm.addEventListener("submit", function (event) {
      event.preventDefault();
      var data = new FormData(updateForm);
      var submitButton = updateForm.querySelector("button[type='submit']");
      if (submitButton) {
        submitButton.disabled = true;
      }
      postFormData(data)
        .then(function (payload) {
          if (payload && payload.ok) {
            grid.updateRow(payload.id, payloadToRowPatch(payload));
            hideEditor();
          }
        })
        .catch(function (errPayload) {
          var message = (errPayload && errPayload.message) || "Nao foi possivel salvar a atividade.";
          setPageMessage(message, "error");
        })
        .finally(function () {
          if (submitButton) {
            submitButton.disabled = false;
          }
        });
    });
  }

  if (deleteForm) {
    deleteForm.addEventListener("submit", function (event) {
      event.preventDefault();
      if (!confirm("Excluir atividade?")) {
        return;
      }
      var data = new FormData(deleteForm);
      var submitButton = deleteForm.querySelector("button[type='submit']");
      if (submitButton) {
        submitButton.disabled = true;
      }
      postFormData(data)
        .then(function (payload) {
          if (payload && payload.ok) {
            grid.removeRow(payload.id);
            hideEditor();
          }
        })
        .catch(function () {
          setPageMessage("Nao foi possivel excluir a atividade.", "error");
        })
        .finally(function () {
          if (submitButton) {
            submitButton.disabled = false;
          }
        });
    });
  }
})();
