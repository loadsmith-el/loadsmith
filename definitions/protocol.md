# Loadsmith — Protocolo Core/Plugin

## 1. Visão geral

Este documento define o contrato de comunicação entre o Loadsmith Core e os plugins. Toda comunicação é orientada a mensagens. Há três canais separados por finalidade:

| Canal | Direção | Formato | Propósito |
|---|---|---|---|
| stdin (fd0) | Core → Plugin | JSONL | Mensagens de controle do core para o plugin |
| stdout (fd1) | Plugin → Core | JSONL | Mensagens de controle do plugin para o core |
| fd3 | Varia por kind* | Apache Arrow IPC | Batches de dados tabulares |
| fd4 | Plugin → Core | JSONL | Logs e eventos estruturados |

*fd3 é de escrita para sources e de leitura para destinations.

`stderr` não é usado como canal de comunicação.

---

## 2. Formato das mensagens

Cada mensagem é um objeto JSON serializado em uma única linha, delimitado por `\n`.

Regras:
- Uma mensagem por linha
- Sem JSON multiline
- Sem JSON sem delimitação de linha
- O campo `type` é obrigatório em todas as mensagens
- Campos desconhecidos devem ser ignorados — garante compatibilidade futura

---

## 3. Versionamento

O protocolo usa **versionamento por inteiro ordinal**, começando em `1`. Incrementa a cada mudança incompatível.

A versão é negociada no handshake. Uma vez definida via `set_protocol_version`, vale para toda a execução.

---

## 4. Lifecycle

### 4.1 Negociação de protocolo

```
Core → Plugin   handshake
Plugin → Core   handshake_ack
Core → Plugin   set_protocol_version    (ou error se incompatível → processo encerrado)
```

### 4.2 Capabilities

```
Core → Plugin   capabilities_request
Plugin → Core   capabilities_response
```

### 4.3 Configuração

```
Core → Plugin   configure
Plugin → Core   configure_ack           (ok ou error → processo encerrado em caso de error)
```

### 4.4 Execução

```
Core → Plugin   start                   (pode carregar resume para sources incrementais)
Plugin → Core   schema                  (source: antes do primeiro batch)
Plugin → Core   ready                   (destination: pronto para receber batches)
                ... dados via fd3 ...
                ... progress, log e ping/pong ocorrem durante a execução ...
                ... checkpoint (source) e committed (destination) ocorrem no fd4 ...
Plugin → Core   finished
```

### 4.5 Cancelamento

Pode ocorrer a qualquer momento após `start`.

```
Core → Plugin   cancel
Plugin → Core   finished  (status: cancelled)
```

---

## 5. Referência de mensagens

### `handshake`

**Core → Plugin.** Primeira mensagem enviada pelo core após iniciar o processo do plugin.

```jsonl
{"type":"handshake"}
```

---

### `handshake_ack`

**Plugin → Core.** Resposta imediata ao `handshake`. O plugin declara as versões de protocolo que suporta, sua identidade e kind.

```jsonl
{"type":"handshake_ack","protocol_supported_versions":[1],"plugin_name":"loadsmith-source-csv","plugin_version":"0.1.0","kind":"source"}
```

Campos:

| Campo | Tipo | Descrição |
|---|---|---|
| `protocol_supported_versions` | `int[]` | Versões suportadas pelo plugin, em ordem crescente |
| `plugin_name` | `string` | Identificador único do plugin |
| `plugin_version` | `string` | Versão do binário do plugin |
| `kind` | `string` | `"source"`, `"destination"`, `"sink"`, `"parser"` ou `"provider"` |

---

### `set_protocol_version`

**Core → Plugin.** O core escolhe a versão a usar — a mais alta presente em `protocol_supported_versions` que o core também suporta. A partir deste momento, o protocolo da versão escolhida está em vigor.

Se não houver versão compatível, o core envia `error` e encerra o processo do plugin.

```jsonl
{"type":"set_protocol_version","protocol_version":1}
```

---

### `capabilities_request`

**Core → Plugin.** Solicita as capabilities do plugin.

```jsonl
{"type":"capabilities_request"}
```

---

### `capabilities_response`

**Plugin → Core.**

```jsonl
{"type":"capabilities_response","supports":["schema_inference","batch_read","incremental_state"]}
```

Capabilities definidas (extensível):

| Capability | Descrição |
|---|---|
| `schema_inference` | O plugin consegue inferir o schema da fonte |
| `batch_read` | Leitura em batches |
| `incremental_state` | Source: aceita um cursor de resume e reporta watermarks via `checkpoint` |
| `checkpointed_commit` | Destination: confirma durabilidade por chunk via `committed`, habilitando checkpoint intra-run |
| `staged_merge` | Destination: grava em staging e faz `MERGE` por PK no fim — exactly-once efetivo |

---

### `configure`

**Core → Plugin.** Envia o bloco de configuração do plugin — o conteúdo de `source.config`, `destination.config`, etc. O conteúdo de `config` é opaco para o core.

```jsonl
{"type":"configure","config":{...}}
```

---

### `configure_ack`

**Plugin → Core.** Resultado da validação interna da configuração.

Sucesso:

```jsonl
{"type":"configure_ack","status":"ok"}
```

Erro:

```jsonl
{"type":"configure_ack","status":"error","code":"INVALID_CONFIG","message":"campo 'path' é obrigatório"}
```

Se `status` for `error`, o core exibe `message` para o usuário e encerra a execução.

---

### `start`

**Core → Plugin.** Inicia a execução. Para um source incremental cujo pipeline
tem estado persistido de um run anterior, o core inclui `resume` com o watermark
opaco a retomar — o core nunca interpreta `cursor_value`, só o armazena e devolve.
A forma curta `{"type":"start"}` continua válida (campo opcional).

```jsonl
{"type":"start"}
{"type":"start","resume":{"cursor_value":"2026-06-09T08:00:00Z"}}
```

---

### `schema`

**Plugin → Core. Apenas sources.** Enviado antes do primeiro batch. Descreve o schema dos dados que serão produzidos.

```jsonl
{"type":"schema","fields":[{"name":"id","type":"int64"},{"name":"nome","type":"utf8"},{"name":"criado_em","type":"timestamp_ms"}]}
```

Campos de `fields`:

| Campo | Tipo | Descrição |
|---|---|---|
| `name` | `string` | Nome da coluna |
| `type` | `string` | Tipo Arrow: `int32`, `int64`, `float32`, `float64`, `utf8`, `bool`, `date32`, `timestamp_ms`, `binary` |

---

### `ready`

**Plugin → Core. Apenas destinations.** Enviado após `start`, indicando que o plugin está pronto para receber batches via fd3.

```jsonl
{"type":"ready"}
```

---

### `progress`

**Plugin → Core.** Enviado periodicamente durante a execução.

Source:

```jsonl
{"type":"progress","rows_read":10000,"batches_read":1}
```

Destination:

```jsonl
{"type":"progress","rows_written":10000,"batches_written":1}
```

---

### `log`

**Plugin → Core. Via fd4 (canal de eventos). Nunca via stdout.**

```jsonl
{"type":"log","level":"info","message":"conectado ao banco"}
```

Níveis: `trace`, `debug`, `info`, `warn`, `error`.

---

### `checkpoint`

**Source → Core. Via fd4 (canal de eventos).** O watermark mais alto da coluna
de cursor produzido até o batch `batch_seq`. `cursor_value` é opaco para o core.
O core só persiste esse valor depois que o destination confirma (via `committed`)
que o batch correspondente está durável — é essa porta de durabilidade que torna
o resume sem-gap (at-least-once; duplicatas de fronteira absorvidas por um
destination idempotente).

```jsonl
{"type":"checkpoint","cursor_value":"2026-06-09T08:00:00Z","batch_seq":42}
```

---

### `committed`

**Destination → Core. Via fd4 (canal de eventos).** Tudo até o batch `batch_seq`
(inclusive) está duravelmente gravado. Um destination que só fica durável no
final (ex.: staging + swap atômico) emite um único `committed` cobrindo todos os
batches no `finalize`; um destination com flush incremental emite ao longo do run.

```jsonl
{"type":"committed","batch_seq":42}
```

---

### `object_ready`

**Destination → Core. Via fd4 (canal de eventos).** Enviado por um destination de
arquivo quando um arquivo de staging é finalizado (footer escrito). O core
encaminha o path ao supervisor do sink, que o entrega — assim a entrega se
sobrepõe ao pump. Usar o canal de eventos (já drenado concorrentemente) é o que
permite entregar um chunk no instante em que ele fecha.

```jsonl
{"type":"object_ready","path":"/scratch/events.00000001.snappy.parquet"}
```

---

### `deliver_object`

**Core → Sink. Via canal de controle.** Uma por arquivo a entregar. O core fecha
o stdin do sink (EOF) quando todos os objetos foram entregues, sinalizando ao
sink para finalizar.

```jsonl
{"type":"deliver_object","path":"/scratch/events.00000001.snappy.parquet"}
```

---

### `object_delivered`

**Sink → Core. Via canal de controle.** Ack por objeto entregue. Alimenta o
ledger de entrega do core: se o sink cair, o core o respawna e reenvia apenas os
objetos ainda não confirmados. `deliver` deve ser idempotente.

```jsonl
{"type":"object_delivered","path":"/scratch/events.00000001.snappy.parquet"}
```

---

### `ping`

**Core → Plugin.** Healthcheck. Enviado quando o plugin fica silencioso por mais tempo do que o timeout de healthcheck configurado.

```jsonl
{"type":"ping"}
```

---

### `pong`

**Plugin → Core.** Resposta imediata ao `ping`.

```jsonl
{"type":"pong"}
```

Se o core não receber `pong` dentro de um prazo configurável após o `ping`, considera o plugin morto, encerra o processo e reporta timeout.

---

### `cancel`

**Core → Plugin.** Solicita cancelamento. O plugin deve encerrar o mais rápido possível e enviar `finished`.

```jsonl
{"type":"cancel","reason":"timeout"}
```

---

### `error`

**Core → Plugin.** Erro irrecuperável emitido pelo core antes ou durante o handshake. O processo é encerrado após este envio.

```jsonl
{"type":"error","code":"UNSUPPORTED_PROTOCOL_VERSION","message":"plugin suporta versões [2, 3], core suporta apenas [1]"}
```

---

### `finished`

**Plugin → Core.** Última mensagem enviada pelo plugin.

Sucesso (source):

```jsonl
{"type":"finished","status":"success","rows_read":21473000,"batches_read":2148}
```

Sucesso (destination):

```jsonl
{"type":"finished","status":"success","rows_written":21473000,"batches_written":2148}
```

Erro:

```jsonl
{"type":"finished","status":"error","code":"CONNECTION_FAILED","message":"falha ao conectar: connection refused"}
```

Cancelado:

```jsonl
{"type":"finished","status":"cancelled"}
```

---

## 6. Compatibilidade futura

Campos desconhecidos em qualquer mensagem devem ser ignorados. Novos campos opcionais podem ser adicionados dentro de uma mesma versão de protocolo.

A adição de novos tipos de mensagem obrigatórios, ou mudança de semântica de mensagens existentes, requer incremento da versão de protocolo.
