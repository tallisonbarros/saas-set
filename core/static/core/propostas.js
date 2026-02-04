(() => {
  const container = document.getElementById("proposal-listings");
  if (!container) {
    return;
  }

  const tabs = Array.from(document.querySelectorAll("[data-proposta-tab]"));
  const titleEl = document.querySelector("[data-proposta-title]");
  const subtitleEl = document.querySelector("[data-proposta-subtitle]");
  const summaryWrap = document.querySelector("[data-proposta-summary]");
  const url = container.dataset.propostasUrl;

  const MODE_CONFIG = {
    recebidas: {
      title: "Propostas recebidas",
      subtitle: "Propostas que chegaram para voce aprovar, responder ou acompanhar.",
      summaryLabels: {
        pendentes: "Pendentes para voce",
        execucao: "Em analise",
        total: "Total recebidas",
        finalizadas_90: "Finalizadas (90 dias)",
      },
    },
    enviadas: {
      title: "Propostas enviadas",
      subtitle: "Propostas que voce enviou para clientes e esta acompanhando o retorno.",
      summaryLabels: {
        pendentes: "Aguardando resposta",
        execucao: "Em negociacao",
        total: "Total enviadas",
        finalizadas_90: "Finalizadas (90 dias)",
      },
    },
  };

  const getQueryParams = () => {
    const params = new URLSearchParams(window.location.search);
    return {
      mode: params.get("mode") || params.get("tipo") || "",
      status: params.get("status") || "",
      q: params.get("q") || "",
    };
  };

  const saveModeToStorage = (mode) => {
    try {
      localStorage.setItem("propostas_mode", mode);
    } catch (error) {
      return;
    }
  };

  const loadModeFromStorage = () => {
    try {
      return localStorage.getItem("propostas_mode") || "";
    } catch (error) {
      return "";
    }
  };

  const resolveMode = (rawMode) => {
    if (rawMode === "enviadas") {
      return "enviadas";
    }
    return "recebidas";
  };

  const updateHeader = (mode) => {
    const config = MODE_CONFIG[mode];
    if (titleEl) {
      titleEl.textContent = config.title;
    }
    if (subtitleEl) {
      subtitleEl.textContent = config.subtitle;
    }
    if (!summaryWrap) {
      return;
    }
    Object.entries(config.summaryLabels).forEach(([key, value]) => {
      const labelEl = summaryWrap.querySelector(`[data-summary-label="${key}"]`);
      if (labelEl) {
        labelEl.textContent = value;
      }
    });
  };

  const updateSummaryValues = (summary) => {
    if (!summaryWrap || !summary) {
      return;
    }
    Object.entries(summary).forEach(([key, value]) => {
      const valueEl = summaryWrap.querySelector(`[data-summary-value="${key}"]`);
      if (valueEl) {
        valueEl.textContent = value ?? "0";
      }
    });
  };

  const setActiveTab = (mode) => {
    tabs.forEach((tab) => {
      const isActive = tab.dataset.propostaTab === mode;
      tab.classList.toggle("active", isActive);
      tab.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
  };

  const renderSkeleton = () => {
    container.classList.add("is-loading");
    container.innerHTML = `
      <div class="proposal-section">
        <div class="proposal-section-head">
          <div>
            <div class="proposal-section-title">Carregando</div>
            <div class="proposal-section-subtitle">Atualizando propostas...</div>
          </div>
        </div>
        <div class="proposal-grid">
          <div class="proposal-card proposal-card-skeleton"></div>
          <div class="proposal-card proposal-card-skeleton"></div>
        </div>
      </div>
      <div class="proposal-section">
        <div class="proposal-section-head">
          <div>
            <div class="proposal-section-title">Carregando</div>
            <div class="proposal-section-subtitle">Atualizando propostas...</div>
          </div>
        </div>
        <div class="proposal-grid">
          <div class="proposal-card proposal-card-skeleton"></div>
          <div class="proposal-card proposal-card-skeleton"></div>
          <div class="proposal-card proposal-card-skeleton"></div>
        </div>
      </div>
    `;
  };

  const renderError = () => {
    container.innerHTML = `
      <div class="proposal-card proposal-card-empty">
        <div class="proposal-title">Nao foi possivel carregar as propostas.</div>
        <div class="proposal-meta">Tente novamente em alguns instantes.</div>
      </div>
    `;
  };

  let state = {
    mode: resolveMode(container.dataset.mode || ""),
  };

  const syncUrl = (mode, status, q, push) => {
    const params = new URLSearchParams();
    if (mode) {
      params.set("mode", mode);
    }
    if (status) {
      params.set("status", status);
    }
    if (q) {
      params.set("q", q);
    }
    const query = params.toString();
    const nextUrl = query ? `${window.location.pathname}?${query}` : window.location.pathname;
    if (push) {
      window.history.pushState({ mode, status, q }, "", nextUrl);
    } else {
      window.history.replaceState({ mode, status, q }, "", nextUrl);
    }
  };

  const fetchData = async ({ mode }) => {
    if (!url) {
      return;
    }
    renderSkeleton();
    const params = new URLSearchParams();
    params.set("mode", mode);
    try {
      const response = await fetch(`${url}?${params.toString()}`, {
        headers: {
          "X-Requested-With": "XMLHttpRequest",
        },
      });
      if (!response.ok) {
        throw new Error("Fetch failed");
      }
      const data = await response.json();
      if (!data.ok) {
        throw new Error("Invalid response");
      }
      container.innerHTML = data.html;
      updateSummaryValues(data.summary);
      container.dataset.mode = mode;
    } catch (error) {
      renderError();
    } finally {
      container.classList.remove("is-loading");
    }
  };

  const setState = (nextState, options = {}) => {
    const { pushHistory = false, refresh = false } = options;
    state = { ...state, ...nextState };
    saveModeToStorage(state.mode);
    setActiveTab(state.mode);
    updateHeader(state.mode);
    syncUrl(state.mode, "", "", pushHistory);
    if (refresh) {
      fetchData(state);
    }
  };

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      const newMode = resolveMode(tab.dataset.propostaTab);
      if (newMode === state.mode) {
        return;
      }
      setState({ mode: newMode }, { pushHistory: true, refresh: true });
    });
  });

  window.addEventListener("popstate", () => {
    const { mode } = getQueryParams();
    const nextMode = resolveMode(mode || loadModeFromStorage());
    setState({ mode: nextMode }, { pushHistory: false, refresh: true });
  });

  const initialParams = getQueryParams();
  const initialMode = resolveMode(initialParams.mode || loadModeFromStorage() || state.mode);
  const shouldRefresh = initialMode !== state.mode;
  setState({ mode: initialMode }, { pushHistory: false, refresh: false });
  if (shouldRefresh) {
    fetchData({ mode: initialMode });
  }
})();
