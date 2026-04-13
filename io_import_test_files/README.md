# Industrial I/O Import Test Files

## Visão geral

Este pacote contém uma base canônica e 10 planilhas Excel estruturalmente diferentes para testar importação de listas de I/O industriais.
Todas as 10 planilhas de teste representam exatamente o mesmo conjunto lógico de pontos da base canônica.
O conjunto atualizado contempla explicitamente cenários com múltiplos racks na mesma guia e também múltiplas guias em diferentes workbooks.

## Quantidade total de pontos lógicos

- Total: 120
- DI: 40
- DO: 32
- AI: 28
- AO: 12
- SPARE: 8

## Arquivos gerados

- `00_canonical_io.csv`: base canônica em CSV com todos os campos de normalização.
- `00_canonical_io.xlsx`: base canônica em Excel com todos os campos de validação.
- `01_io_flat_ptbr.xlsx`: planilha limpa em português com colunas explícitas.
- `02_io_flat_english.xlsx`: planilha linear em inglês com ordem e nomenclatura alteradas.
- `03_io_compact_location.xlsx`: layout compacto com localização combinada, índice e resumo por rack.
- `04_io_grouped_by_module.xlsx`: layout agrupado por módulo com múltiplos racks na mesma guia principal e abas auxiliares.
- `05_io_multisheet_by_panel.xlsx`: workbook com capa, legenda, resumo de racks e abas por painel.
- `06_io_two_header_levels.xlsx`: documento com título, revisão, cabeçalho em dois níveis e guias auxiliares.
- `07_io_noisy_export.xlsx`: exportação bagunçada com colunas irrelevantes, ruído e guia auxiliar.
- `08_io_decimal_comma_ptbr.xlsx`: planilha PT-BR com vírgula decimal.
- `09_io_minimal_headers.xlsx`: exportação com cabeçalhos curtos e pouco amigáveis.
- `10_io_mixed_real_world.xlsx`: planilha de campo com dados distribuídos em guias não óbvias, cada uma contendo múltiplos racks.
- `manifest.json`: metadados do pacote e características de cada arquivo.
- `generate_io_test_files.py`: script reprodutível para gerar todos os artefatos.

## Observações

- Os arquivos `.xlsx` foram gerados com `openpyxl` e não utilizam macros.
- Todos os formatos derivam da mesma base canônica; a variação está apenas na organização e apresentação.
- Todos os datasets gerados permanecem multi-rack na representação física dos canais, inclusive em guias únicas de vários formatos.
- O campo `point_uid` existe apenas na base canônica para rastreabilidade e validação.
