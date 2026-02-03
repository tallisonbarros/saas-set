# Isolamento de Apps (visao geral)

Este documento descreve como o sistema organiza **apps isolados** e como criar novos apps sem misturar o core.

## Conceito

- Cada app tem um **registro no banco** (`App`), com `slug`, `nome`, `descricao`, `icon`, `theme_color` e `ativo`.
- Cada usuario recebe acesso via `PerfilUsuario.apps` (ManyToMany).
- Cada app possui **pasta propria** com `views.py`, `urls.py` e `templates/`.
- O painel mostra **apenas os apps liberados** para o usuario.

## Estrutura de pastas

Cada app vive em:

```
core/
  apps/
    app_milhao_bla/
      __init__.py
      views.py
      urls.py
      templates/
        core/
          apps/
            app_milhao_bla/
              dashboard.html
```

## Roteamento

No arquivo `saasset/urls.py`:

- Rota do app isolado:
  - `/apps/appmilhaobla/` -> `core/apps/app_milhao_bla/urls.py`
- Rota generica:
  - `/apps/<slug>/` -> `core.views.app_home`

Na `app_home`, se o `slug` for conhecido, redireciona para o app isolado. Assim:
- `/apps/appmilhaobla/` resolve direto
- `/apps/<slug>/` funciona como fallback

## Controle de acesso

Em cada `views.py` do app isolado:

- Busca o `App` pelo `slug`
- Verifica se o usuario tem permissao:
  - `request.user.is_staff` **ou**
  - usuario tem o app em `PerfilUsuario.apps`
- Se nao tiver permissao, retorna `HttpResponseForbidden`.

## Como criar um novo app

1. **Criar registro do App (admin ou painel "Aplicativos")**
   - Nome: `AppNutrien`
   - Slug: `appnutrien`
   - Icone: `NU`
   - Tema: `#6c8cff`

2. **Criar pasta do app**

```
core/apps/app_nutrien/
  __init__.py
  views.py
  urls.py
  templates/core/apps/app_nutrien/dashboard.html
```

3. **Adicionar rota em `saasset/urls.py`**

```
path('apps/appnutrien/', include('core.apps.app_nutrien.urls')),
```

4. **Adicionar redirecionamento na `app_home`**

```
if app.slug == "appnutrien":
    return redirect("app_nutrien_dashboard")
```

5. **Liberar acesso ao usuario**

- Em `Usuarios > Gerenciar`, preencher o campo `APPS` com o slug:
  - `appnutrien`

## Observacoes importantes

- Cada app pode ter **template, layout e logica totalmente diferentes**.
- Nenhuma regra do app precisa ficar no core.
- O app pode consumir dados de ingestao ou outras tabelas sem afetar o restante do sistema.

## Dica para evolucao futura (modo "sub-site")

Quando quiser isolar ainda mais:

- Crie um `base.html` exclusivo por app (ex: `core/apps/app_nutrien/templates/base_app.html`)
- Mantenha URLs do app fora do menu principal
- Posteriormente, pode mover para outro projeto sem quebrar o core, pois o app ja tem seu modulo.
