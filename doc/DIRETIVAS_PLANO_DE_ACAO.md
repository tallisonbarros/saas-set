# Diretivas de Plano de Execucao - SAAS-SET

## Objetivo
Padronizar planos de execucao tecnicos alinhados ao sistema atual do SAAS-SET.

## Atualizado em
06/03/2026

## Ativacao (flexivel)
Use este documento quando o pedido envolver:
- plano de acao
- roadmap de execucao
- fases de implementacao
- plano de handoff tecnico
- plano de evolucao de modulo/app

Exemplo de gatilho explicito:
`crie um plano de execucao com base no doc/DIRETIVAS_PLANO_DE_ACAO.md para ...`

## Realidade atual obrigatoria (contexto-base)
Todo plano deve partir destas premissas reais do projeto:

1. Stack e arquitetura
- Django `6.0.1` (monolito), app principal `core`.
- Config central em `saasset/settings.py` e rotas em `saasset/urls.py`.
- Banco via `dj_database_url` (default local: `sqlite:///db.sqlite3`).
- Static com `whitenoise`; media em `MEDIA_ROOT`.

2. Modulos e apps isolados
- Modulos centrais em `core/views.py` + templates `core/templates/core/...`.
- Apps isolados em `core/apps/<app>/` com `views.py`, `urls.py`, `templates`.
- Apps ativos hoje: `app_milhao_bla` e `app_rotas`.
- Acesso por app via `PerfilUsuario.apps` + regra de permissao em `core.views.app_home`.

3. Ingest e observabilidade
- Endpoint de ingest: `/api/ingest`.
- Modelos de ingest: `IngestRecord`, `IngestErrorLog`, `IngestRule`.
- Campos por app para ingest em `App`: `ingest_client_id`, `ingest_agent_id`, `ingest_source`.

4. Cobertura de testes atual
- Suites existentes: `core.tests`, `core.tests_app_rotas`, `core.tests_app_milhao_bla`.
- Comando de regressao recomendado:
`python manage.py test core.tests_app_rotas core.tests_app_milhao_bla core.tests --verbosity 1`

## Requisitos obrigatorios do plano gerado
1. Escrever em estilo de prompt operacional para Codex.
2. Dividir por fases focais e sequenciais.
3. A penultima fase deve ser de consolidacao total.
4. A ultima fase deve ser dedicada a testes finais.
5. Cada fase deve conter: `Escopo`, `Entregaveis`, `Testes`, `Criterio de saida`.
6. Cada fase deve terminar com decisao explicita: `go/no-go`.
7. Incluir riscos remanescentes por fase com mitigacao curta.
8. Exigir evidencia de teste por fase (`comando` + `resultado resumido`).
9. Priorizar commits pequenos por fase (`feat/test/docs/chore`).
10. Fechar com status final: `pronto para producao` ou `pendente`.

## Estrutura obrigatoria do plano
1. Contexto inicial
- Objetivo mensuravel.
- Escopo.
- Fora de escopo.
- Premissas e dependencias.
- Modulos/arquivos reais que serao tocados.

2. Fases focais de implementacao
- Sequencia pragmatica por impacto tecnico.
- Respeitar isolamento de apps (`core/apps/...`) quando a demanda for de app dedicado.
- Evitar mover regra de negocio de app isolado para `core/views.py` sem necessidade explicita.

3. Penultima fase (consolidacao total)
- Limpeza tecnica.
- Revisao de permissao/acesso.
- Revisao de rotas, templates e imports.
- Handoff operacional (como operar, monitorar e validar).

4. Ultima fase (testes finais)
- Replay de cenarios reais.
- Regressao direcionada nos modulos impactados.
- Verificacao de comandos Django de sanidade (`check`, testes relevantes).

5. Encerramento
- Riscos residuais.
- Decisao final de prontidao.

## Politica de execucao
1. Nao avancar de fase com teste vermelho.
2. Evitar mudancas fora de escopo.
3. Se fase critica falhar, registrar rollback/reversao segura.
4. Entregar ao fim de cada fase:
- diff resumido
- riscos remanescentes
- decisao go/no-go

## Comandos padrao para evidencia (usar conforme impacto)
- Sanidade geral:
`python manage.py check`
- Regressao principal do projeto:
`python manage.py test core.tests_app_rotas core.tests_app_milhao_bla core.tests --verbosity 1`
- Se houver alteracao de modelo:
`python manage.py makemigrations --check --dry-run`

## Template curto de fase (obrigatorio)
Use este formato em todas as fases:

`Fase X - <nome>`
- `Escopo:`
- `Entregaveis:`
- `Testes:`
- `Riscos remanescentes e mitigacao:`
- `Criterio de saida:`
- `Decisao: go/no-go`

## Observacoes finais
- Se faltarem detalhes no pedido, assumir defaults seguros e explicitar pendencias.
- Quando houver alteracao em app isolado, validar que `saasset/urls.py`, permissao e navegacao continuam coerentes.
- Se o plano tocar ingest, explicitar filtros por `client_id`, `agent_id` e `source` para evitar leitura fora de escopo.
