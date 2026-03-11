(function (global) {
  function safeText(value) {
    if (value === null || value === undefined) {
      return "";
    }
    return String(value);
  }

  function norm(value) {
    return safeText(value).trim().toLowerCase();
  }

  function mountFromSelect(selectEl, options) {
    if (!selectEl || selectEl.tagName !== "SELECT" || !selectEl.multiple) {
      return null;
    }
    if (selectEl.dataset.colabPickerMounted === "1") {
      return null;
    }
    selectEl.dataset.colabPickerMounted = "1";

    var opts = options || {};
    var placeholder =
      safeText(opts.placeholder) ||
      safeText(selectEl.getAttribute("data-colab-picker-placeholder")) ||
      "Adicionar colaborador...";
    var emptyText =
      safeText(opts.emptyText) ||
      safeText(selectEl.getAttribute("data-colab-picker-empty")) ||
      "Sem colaboradores selecionados";
    var noResultsText =
      safeText(opts.noResultsText) ||
      safeText(selectEl.getAttribute("data-colab-picker-no-results")) ||
      "Nenhum colaborador encontrado";
    var maxVisibleFromAttr = Number(
      safeText(opts.maxVisibleChips) || safeText(selectEl.getAttribute("data-colab-picker-max-chips")) || ""
    );

    var wrapper = document.createElement("div");
    wrapper.className = "colab-picker";

    var chipsEl = document.createElement("div");
    chipsEl.className = "colab-picker-chips";

    var inputEl = document.createElement("input");
    inputEl.type = "text";
    inputEl.className = "colab-picker-input";
    inputEl.placeholder = placeholder;
    inputEl.autocomplete = "off";
    inputEl.spellcheck = false;

    var menuEl = document.createElement("div");
    menuEl.className = "colab-picker-menu";

    wrapper.appendChild(chipsEl);
    wrapper.appendChild(inputEl);
    wrapper.appendChild(menuEl);

    selectEl.style.display = "none";
    selectEl.setAttribute("aria-hidden", "true");
    selectEl.tabIndex = -1;
    if (selectEl.nextSibling) {
      selectEl.parentNode.insertBefore(wrapper, selectEl.nextSibling);
    } else {
      selectEl.parentNode.appendChild(wrapper);
    }

    var activeIndex = -1;
    var filteredItems = [];

    function getMaxVisibleChips() {
      if (Number.isFinite(maxVisibleFromAttr) && maxVisibleFromAttr > 0) {
        return Math.max(1, Math.floor(maxVisibleFromAttr));
      }
      return window.innerWidth <= 1024 ? 2 : 3;
    }

    function getOptions() {
      return Array.prototype.slice.call(selectEl.options || []);
    }

    function setSelected(optionValue, selected) {
      getOptions().forEach(function (option) {
        if (safeText(option.value) === safeText(optionValue)) {
          option.selected = !!selected;
        }
      });
      selectEl.dispatchEvent(new Event("change", { bubbles: true }));
    }

    function selectedOptions() {
      return getOptions().filter(function (option) {
        return option.selected;
      });
    }

    function renderChips() {
      chipsEl.innerHTML = "";
      var selected = selectedOptions();
      if (!selected.length) {
        var emptyEl = document.createElement("span");
        emptyEl.className = "colab-picker-empty";
        emptyEl.textContent = emptyText;
        chipsEl.appendChild(emptyEl);
        return;
      }
      var maxVisible = getMaxVisibleChips();
      var visible = selected.slice(0, maxVisible);
      var hiddenCount = Math.max(0, selected.length - visible.length);

      visible.forEach(function (option) {
        var chip = document.createElement("span");
        chip.className = "colab-picker-chip";
        chip.title = safeText(option.textContent);

        var label = document.createElement("span");
        label.className = "colab-picker-chip-label";
        label.textContent = safeText(option.textContent);

        var removeBtn = document.createElement("button");
        removeBtn.type = "button";
        removeBtn.className = "colab-picker-chip-remove";
        removeBtn.setAttribute("aria-label", "Remover colaborador");
        removeBtn.textContent = "x";
        removeBtn.addEventListener("click", function (event) {
          event.preventDefault();
          event.stopPropagation();
          setSelected(option.value, false);
          renderMenu();
          inputEl.focus();
        });

        chip.appendChild(label);
        chip.appendChild(removeBtn);
        chipsEl.appendChild(chip);
      });

      if (hiddenCount > 0) {
        var countChip = document.createElement("span");
        countChip.className = "colab-picker-chip is-counter";
        countChip.textContent = "+" + hiddenCount;
        countChip.title = hiddenCount + " colaborador(es) adicional(is)";
        chipsEl.appendChild(countChip);
      }
    }

    function filterAvailable(term) {
      var query = norm(term);
      return getOptions().filter(function (option) {
        if (option.selected) {
          return false;
        }
        if (!query) {
          return true;
        }
        return norm(option.textContent).indexOf(query) >= 0;
      });
    }

    function closeMenu() {
      activeIndex = -1;
      filteredItems = [];
      menuEl.classList.remove("is-open");
      menuEl.innerHTML = "";
    }

    function renderMenu() {
      filteredItems = filterAvailable(inputEl.value);
      activeIndex = filteredItems.length ? 0 : -1;
      menuEl.innerHTML = "";
      if (!filteredItems.length) {
        var emptyResult = document.createElement("div");
        emptyResult.className = "colab-picker-no-results";
        emptyResult.textContent = noResultsText;
        menuEl.appendChild(emptyResult);
        menuEl.classList.add("is-open");
        return;
      }
      filteredItems.forEach(function (option, index) {
        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "colab-picker-option";
        if (index === activeIndex) {
          btn.classList.add("is-active");
        }
        btn.textContent = safeText(option.textContent);
        btn.addEventListener("click", function (event) {
          event.preventDefault();
          setSelected(option.value, true);
          inputEl.value = "";
          renderChips();
          renderMenu();
          inputEl.focus();
        });
        menuEl.appendChild(btn);
      });
      menuEl.classList.add("is-open");
    }

    function focusActiveOption() {
      var optionButtons = menuEl.querySelectorAll(".colab-picker-option");
      optionButtons.forEach(function (el, index) {
        el.classList.toggle("is-active", index === activeIndex);
      });
    }

    wrapper.addEventListener("click", function () {
      inputEl.focus();
    });

    inputEl.addEventListener("focus", function () {
      renderMenu();
    });

    inputEl.addEventListener("input", function () {
      renderMenu();
    });

    inputEl.addEventListener("keydown", function (event) {
      if (event.key === "Escape") {
        closeMenu();
        return;
      }
      if (!menuEl.classList.contains("is-open")) {
        if (event.key === "ArrowDown" || event.key === "ArrowUp") {
          renderMenu();
          event.preventDefault();
        }
        return;
      }
      if (event.key === "ArrowDown") {
        event.preventDefault();
        if (filteredItems.length) {
          activeIndex = Math.min(filteredItems.length - 1, activeIndex + 1);
          focusActiveOption();
        }
        return;
      }
      if (event.key === "ArrowUp") {
        event.preventDefault();
        if (filteredItems.length) {
          activeIndex = Math.max(0, activeIndex - 1);
          focusActiveOption();
        }
        return;
      }
      if (event.key === "Enter") {
        if (!filteredItems.length || activeIndex < 0) {
          return;
        }
        event.preventDefault();
        var current = filteredItems[activeIndex];
        if (!current) {
          return;
        }
        setSelected(current.value, true);
        inputEl.value = "";
        renderChips();
        renderMenu();
        return;
      }
      if (event.key === "Backspace" && !inputEl.value) {
        var selected = selectedOptions();
        var lastSelected = selected[selected.length - 1];
        if (lastSelected) {
          setSelected(lastSelected.value, false);
          renderChips();
          renderMenu();
        }
      }
    });

    document.addEventListener("mousedown", function (event) {
      if (!wrapper.contains(event.target)) {
        closeMenu();
      }
    });
    window.addEventListener("resize", renderChips);

    selectEl.addEventListener("change", function () {
      renderChips();
    });

    var ownerForm = selectEl.form;
    if (ownerForm) {
      ownerForm.addEventListener("reset", function () {
        window.setTimeout(function () {
          inputEl.value = "";
          renderChips();
          closeMenu();
        }, 0);
      });
    }

    renderChips();
    closeMenu();

    return {
      root: wrapper,
      select: selectEl,
      refresh: function () {
        renderChips();
      },
    };
  }

  function mountAll(selector, options) {
    var query = selector || "select[data-colab-picker]";
    var nodes = document.querySelectorAll(query);
    nodes.forEach(function (node) {
      mountFromSelect(node, options);
    });
  }

  global.RadarColaboradoresPicker = {
    mountFromSelect: mountFromSelect,
    mountAll: mountAll,
  };
})(window);
