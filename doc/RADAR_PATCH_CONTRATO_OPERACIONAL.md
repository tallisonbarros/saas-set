# Contrato Funcional - Radar (Patch 1 + Patch 2)

## Regra oficial de status x agenda
- `inicio_execucao_em` e `finalizada_em` sao definidos exclusivamente pela agenda (`set_agenda_atividade`).
- `quick_status_atividade` e `update_atividade` alteram apenas o `status` manual.
- Sem dias na agenda: `inicio_execucao_em = null` e `finalizada_em = null`.
- Com dias na agenda: menor data marcada vira `inicio_execucao_em` e maior data marcada vira `finalizada_em`.

## Payloads AJAX

### `POST /radar-atividades/{radar_pk}/trabalhos/{pk}/` - `action=quick_status_atividade`
Request:
- `action`: `"quick_status_atividade"`
- `atividade_id`: inteiro
- `status`: `"PENDENTE" | "EXECUTANDO" | "FINALIZADA"`

Response (`200`):
- `ok`: boolean
- `id`, `nome`, `descricao`, `status`, `status_label`
- `horas_trabalho`
- `inicio_execucao_display`, `finalizada_display`
- `agenda_dias`: lista ISO (`YYYY-MM-DD`)
- `agenda_total_dias`: inteiro
- `ordem`

### `POST /radar-atividades/{radar_pk}/trabalhos/{pk}/` - `action=set_agenda_atividade`
Request:
- `action`: `"set_agenda_atividade"`
- `atividade_id`: inteiro
- `dias_execucao`: JSON array de datas ISO (`["2026-03-05", "2026-03-06"]`)

Validacoes:
- formato obrigatorio `YYYY-MM-DD`
- limite de 730 datas
- deduplicacao server-side

Response (`200`):
- mesmo contrato de `quick_status_atividade`, com `agenda_dias` atualizado

Erros (`400`):
- `{ "ok": false, "message": "..." }` para payload invalido/data invalida

### `POST /radar-atividades/{pk}/` - `action=create_trabalho`
Campos novos:
- `colaboradores`: texto livre separado por virgula/`;`/quebra de linha

Retorno `row` inclui:
- `colaboradores`: string normalizada
- `total_colaboradores`: inteiro

### `POST /radar-atividades/{radar_pk}/trabalhos/{pk}/` - `action=update_trabalho`
Campos novos:
- `colaboradores`: texto livre separado por virgula/`;`/quebra de linha

Regras:
- trim + normalizacao de espacos
- dedupe case-insensitive
- limite de 40 nomes
- maximo 120 chars por nome

## Integridade de dados
- `RadarTrabalhoColaborador`: `UniqueConstraint(trabalho, nome)`
- `RadarAtividadeDiaExecucao`: `UniqueConstraint(atividade, data_execucao)`

## Checklist de arquivos impactados
- `core/models.py`
- `core/views.py`
- `core/templates/core/radar_detail.html`
- `core/templates/core/radar_trabalho_detail.html`
- `core/static/core/radar_trabalhos_table.js`
- `core/static/core/radar_atividades_table.js`
- `core/static/core/styles.css`
- `core/tests.py`
- `core/migrations/0069_radaratividadediaexecucao_radartrabalhocolaborador_and_more.py`
