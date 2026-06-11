## Plataforma de Lab: `ls-lab` / `loadsmith-lab`

O Loadsmith terá uma plataforma auxiliar chamada **`ls-lab`**, também referida como **`loadsmith-lab`**.

O `ls-lab` deve ser tratado como um projeto separado do runtime principal do Loadsmith. Ele não faz parte da execução normal de pipelines em produção. Sua função é atuar como uma plataforma de laboratório, integração e validação de release.

O objetivo principal do `ls-lab` é conseguir validar uma versão específica do Loadsmith contra cenários reais ou simulados, incluindo dependências externas, plugins, bancos, arquivos, protocolo, eventos, estado e outputs esperados.

---

## Papel do `ls-lab`

O `ls-lab` será um **orquestrador de validação de release**.

Ele deve conseguir:

* baixar uma versão/tag específica do Loadsmith;
* usar código local quando em modo desenvolvimento;
* compilar o core e plugins quando necessário;
* montar ou reutilizar imagens Docker;
* subir dependências de teste em containers;
* executar pipelines de validação;
* capturar logs/eventos JSONL;
* validar saídas geradas;
* comparar resultados esperados;
* gerar relatório final da execução.

Exemplo conceitual:

```text
ls-lab
  ├── seleciona versão/tag do Loadsmith
  ├── baixa código ou usa checkout local
  ├── compila core/plugins ou usa imagem existente
  ├── monta imagem Docker se necessário
  ├── sobe dependências do cenário
  ├── executa casos de teste
  ├── coleta JSONL de controle/eventos
  ├── coleta outputs gerados
  ├── compara com outputs esperados
  └── gera relatório de validação
```

---

## Execução por tag

O `ls-lab` deve permitir validar uma versão específica do Loadsmith.

Exemplo:

```bash
ls-lab --tag 0.0.2 --select oracle
```

Comportamento esperado:

```text
1. Resolve a tag 0.0.2 do Loadsmith.
2. Baixa a release/código correspondente do GitHub ou registry configurado.
3. Compila o core/plugins ou localiza imagem Docker correspondente.
4. Sobe o ambiente necessário para os testes Oracle.
5. Executa os casos selecionados.
6. Valida resultado, logs, eventos, estado e outputs.
7. Gera relatório final.
```

O uso de tag permite validar releases de forma reproduzível.

---

## Execução via imagem Docker

O `ls-lab` deve conseguir executar casos usando uma imagem Docker do Loadsmith.

Isso permite validar o comportamento final da release em um ambiente mais próximo do real, sem depender apenas do binário local.

Exemplo conceitual:

```bash
ls-lab --image loadsmith/loadsmith:0.0.2 --select jdbc
```

Ou, usando tag e deixando o lab resolver a imagem:

```bash
ls-lab --tag 0.0.2 --select jdbc
```

O `ls-lab` deve preferir reutilizar imagens já existentes quando possível.

Regra desejada:

```text
Se a imagem necessária já existir localmente e estiver compatível com a tag/código selecionado,
o ls-lab não deve rebuildar sem necessidade.
```

Para forçar rebuild:

```bash
ls-lab --tag 0.0.2 --select oracle --no-cache
```

ou:

```bash
ls-lab --tag 0.0.2 --select oracle --rebuild
```

Decisão:

```text
Evitar rebuild desnecessário por padrão.
Permitir rebuild explícito com --no-cache ou --rebuild.
```

---

## Execução de todos os casos

O `ls-lab` deve ter um modo para executar todos os casos disponíveis.

Comando desejado:

```bash
ls-lab --all
```

Ou, para uma tag específica:

```bash
ls-lab --tag 0.0.2 --all
```

Comportamento esperado:

```text
1. Descobre todos os casos de lab disponíveis.
2. Resolve dependências necessárias.
3. Agrupa casos por ambiente quando possível.
4. Sobe containers necessários.
5. Executa todos os casos.
6. Reaproveita ambientes quando seguro.
7. Gera um relatório consolidado.
```

O `--all` deve ser usado para validação ampla antes de release.

---

## Seleção de casos

Além de `--all`, o `ls-lab` deve permitir selecionar subconjuntos.

Exemplos:

```bash
ls-lab --select oracle
ls-lab --select jdbc
ls-lab --select xlsx
ls-lab --select s3-parquet
ls-lab --select protocol-mismatch
```

Possíveis filtros futuros:

```bash
ls-lab --tag 0.0.2 --select oracle
ls-lab --tag 0.0.2 --select source:jdbc
ls-lab --tag 0.0.2 --select destination:s3_parquet
ls-lab --tag 0.0.2 --select failure
ls-lab --tag 0.0.2 --select smoke
ls-lab --tag 0.0.2 --select regression
```

---

## Execução com dependências reais

O `ls-lab` deve ser capaz de subir dependências reais em containers para validar plugins.

Exemplos:

```text
Oracle
PostgreSQL
MySQL
MinIO/S3 local
Azurite para Azure Blob Storage
servidor HTTP fake
servidor OAuth fake
SharePoint mock/simulador, se aplicável
```

Exemplo para Oracle:

```bash
ls-lab --tag 0.0.2 --select oracle
```

Fluxo esperado:

```text
1. Sobe container Oracle.
2. Aguarda readiness.
3. Executa scripts de bootstrap.
4. Cria tabelas e massa de teste.
5. Executa pipeline Loadsmith.
6. Valida rows lidas/escritas.
7. Valida schema.
8. Valida logs/eventos.
9. Derruba ambiente, exceto se configurado para preservar.
```

---

## Build e cache

O `ls-lab` deve ter comportamento inteligente de build/cache.

Objetivos:

* evitar rebuild desnecessário;
* acelerar execução local;
* permitir reprodutibilidade em release;
* permitir rebuild forçado quando necessário;
* diferenciar imagem local, imagem de tag e build de código local.

Comportamento desejado:

```text
Sem --no-cache:
  Reutiliza imagem/binário se compatível.

Com --no-cache:
  Rebuilda tudo que for necessário.

Com --rebuild:
  Rebuilda a imagem principal ou componentes selecionados.

Com --tag:
  Usa artefato/release referente à tag.

Sem --tag:
  Pode usar checkout local em modo desenvolvimento.
```

Exemplos:

```bash
ls-lab --select csv
ls-lab --select csv --rebuild
ls-lab --select csv --no-cache
ls-lab --tag 0.0.2 --select csv
ls-lab --tag 0.0.2 --all
```

---

## Casos de teste

O `ls-lab` deve suportar cenários como:

```text
happy path
  Pipeline executa com sucesso.

plugin failure
  Plugin falha durante leitura ou escrita.

invalid config
  Configuração inválida deve falhar na validação correta.

protocol mismatch
  Plugin usa versão incompatível do protocolo.

schema mismatch
  Source produz schema diferente do esperado.

cancel execution
  Core cancela execução e plugins encerram corretamente.

checkpoint recovery
  Pipeline retoma de estado anterior.

reject rows
  Linhas inválidas são desviadas para rejects.

backpressure
  Destination lento não deve explodir memória do core/source.

secret redaction
  Nenhum secret pode vazar em logs/eventos.

docker image execution
  Pipeline executa corretamente dentro da imagem Docker final.

tag compatibility
  Uma tag específica executa a suite esperada.
```

---

## Estrutura sugerida dos casos

Cada caso de lab pode conter:

```text
lab/
  cases/
    oracle-basic/
      case.yaml
      input/
      expected/
      bootstrap/
      docker-compose.yaml

    csv-to-parquet/
      case.yaml
      input/
      expected/

    protocol-mismatch/
      case.yaml
      plugins/
      expected/
```

Exemplo conceitual de `case.yaml`:

```yaml
case:
  name: oracle-basic
  description: Valida leitura JDBC Oracle e escrita em Parquet
  tags:
    - oracle
    - jdbc
    - smoke

runtime:
  image: loadsmith/loadsmith:0.0.2

services:
  - name: oracle
    type: docker
    compose_file: docker-compose.yaml
    readiness:
      type: tcp
      host: oracle
      port: 1521
      timeout_seconds: 120

bootstrap:
  scripts:
    - bootstrap/create_schema.sql
    - bootstrap/insert_data.sql

pipeline:
  file: pipeline.yaml

expect:
  status: success
  rows_read: 1000
  rows_written: 1000
  schema:
    - name: id
      type: int64
    - name: nome
      type: utf8
  events:
    - type: pipeline_started
    - type: schema_detected
    - type: pipeline_finished
```

---

## Relação com plugins

O `ls-lab` deve ajudar a validar plugins próprios e comunitários.

Um plugin pode publicar sua própria suite de lab:

```text
plugin-loadsmith-source-oracle/
  manifest.yaml
  lab/
    cases/
      basic-read/
      invalid-config/
      connection-failure/
      schema-inference/
```

Isso permite que o ecossistema tenha um caminho padronizado para testar compatibilidade.

---

## Relação com releases

Antes de uma release, o `ls-lab` deve executar uma matriz de validação.

Exemplo:

```bash
ls-lab --tag 0.0.2 --all
```

Matriz conceitual:

```text
Core 0.0.2
  x plugin csv
  x plugin jdbc
  x plugin oracle
  x plugin s3_parquet
  x plugin xlsx
  x plugins simulados de falha
  x casos de protocolo incompatível
  x casos de state/checkpoint
```

O objetivo é evitar quebrar:

* contrato JSONL do control plane;
* contrato Arrow IPC do data plane;
* handshake;
* capabilities;
* validação de configuração;
* lifecycle dos plugins;
* encerramento limpo;
* logs/eventos;
* state/checkpoints;
* compatibilidade com imagens Docker;
* compatibilidade entre core e plugins.

---

## Relação com observabilidade

O `ls-lab` deve capturar e validar eventos JSONL emitidos pelo core e pelos plugins.

Validações possíveis:

```text
- deve emitir pipeline_started
- deve emitir schema_detected antes do primeiro batch
- deve emitir progress a cada N batches
- deve emitir plugin_finished
- deve emitir pipeline_finished
- deve mascarar secrets nos logs
- deve encerrar plugins corretamente em erro
```

---

## Não objetivos do `ls-lab`

O `ls-lab` não deve ser:

* scheduler de produção;
* orquestrador tipo Airflow;
* substituto de Step Functions;
* runtime distribuído;
* ferramenta obrigatória para executar pipelines simples;
* dependência do core em produção.

Ele é uma plataforma de desenvolvimento, teste, validação e certificação de compatibilidade.

---

## Atualização na estrutura inicial do projeto

O `ls-lab` deve ficar separado do runtime principal. Pode viver no mesmo monorepo inicialmente, mas conceitualmente é um projeto separado.

Estrutura possível:

```text
loadsmith/
  crates/
    loadsmith-core/
    loadsmith-cli/
    loadsmith-protocol/
    loadsmith-transport/
    loadsmith-config/
    loadsmith-arrow/
    loadsmith-plugin-sdk/
  plugins/
    sources/
      jdbc/
      local-file/
      s3/
    destinations/
      local-parquet/
      s3-parquet/
      stdout/
    providers/
      file/
      s3/
      http/
      aws-secrets-manager/
  examples/
  docs/

loadsmith-lab/
  crates/
    ls-lab-cli/
    ls-lab-runner/
    ls-lab-docker/
    ls-lab-report/
  lab/
    cases/
      csv-to-parquet/
      jdbc-to-s3/
      oracle-basic/
      xlsx-with-rejects/
      plugin-failure/
      protocol-mismatch/
  docs/
```

---

## Atualização no MVP

Adicionar ao MVP do ecossistema:

* estrutura inicial do `loadsmith-lab`;
* comando `ls-lab --select <caso>`;
* comando `ls-lab --all`;
* suporte a execução por tag com `--tag`;
* suporte a imagem Docker;
* cache/reuso de imagem quando possível;
* opção de rebuild com `--rebuild` ou `--no-cache`;
* pelo menos uma suite local simples;
* pelo menos um caso com dependência em container;
* validação de mensagens JSONL esperadas;
* validação de status final;
* validação de output simples.

Exemplos:

```bash
ls-lab --select csv-to-parquet
ls-lab --select plugin-failure
ls-lab --select protocol-mismatch
ls-lab --tag 0.0.2 --select oracle
ls-lab --tag 0.0.2 --all
ls-lab --tag 0.0.2 --all --no-cache
```

---

## Atualização nas decisões consolidadas

Adicionar:

* O ecossistema terá uma plataforma auxiliar chamada **`ls-lab`** ou **`loadsmith-lab`**.
* O `ls-lab` será usado como laboratório e orquestrador de validação de release.
* O `ls-lab` deve ser capaz de executar validações por tag.
* O `ls-lab` deve ser capaz de usar imagens Docker.
* O `ls-lab` deve ser capaz de compilar/montar imagem quando necessário.
* O `ls-lab` deve evitar rebuild desnecessário por padrão.
* O `ls-lab` deve suportar `--all` para executar todos os casos disponíveis.
* O `ls-lab` deve suportar `--select` para executar subconjuntos.
* O `ls-lab` não será runtime de produção.
* O `ls-lab` deve validar core, plugins, protocolo, eventos, falhas, outputs e compatibilidade.
* O `ls-lab` deve ajudar na certificação de plugins próprios e comunitários.

---

## Atualização nas decisões em aberto

Adicionar:

* Definir se o nome oficial será `ls-lab`, `loadsmith-lab` ou ambos.
* Definir se o `loadsmith-lab` ficará em repositório separado desde o início.
* Definir formato exato dos casos de teste.
* Definir semântica final de `--select`.
* Definir semântica final de `--all`.
* Definir semântica final de `--tag`.
* Definir estratégia de cache de imagem.
* Definir diferença entre `--rebuild` e `--no-cache`.
* Definir como localizar releases/tags no GitHub.
* Definir como comparar outputs Arrow/Parquet.
* Definir como simular falhas de plugin.
* Definir como versionar suites de compatibilidade.
* Definir quais serviços reais entram na primeira matriz: Oracle, PostgreSQL, MySQL, MinIO etc.
* Definir se plugins comunitários poderão publicar suites `lab/` junto do plugin.
