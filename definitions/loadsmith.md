# Loadsmith — Documento de Arquitetura Inicial

## 1. Visão

O **Loadsmith** é uma ferramenta moderna de **EL** — *Extract and Load* — criada para executar pipelines declarativos de ingestão e carga de dados.

A proposta é ser uma alternativa moderna ao Embulk, mantendo a ideia de configuração declarativa e plugins, mas com uma arquitetura mais limpa, extensível, performática e segura.

O Loadsmith deve ser construído em torno de um princípio central:

> **Core pequeno, estável e genérico; plugins especializados fazendo o trabalho específico.**

O objetivo não é criar uma ferramenta de transformação pesada, nem substituir dbt, Spark ou engines SQL. O Loadsmith deve focar em mover dados de uma origem para um destino com previsibilidade, streaming real, boa observabilidade e suporte a fontes/destinos modernos.

---

## 2. Objetivos

### 2.1 Objetivos principais

* Executar pipelines declarativos de ingestão e carga.
* Suportar fontes e destinos variados por meio de plugins.
* Usar streaming/batches para evitar carregar datasets inteiros em memória.
* Permitir plugins escritos em múltiplas linguagens.
* Separar claramente controle, dados e eventos/logs.
* Usar formatos simples e eficientes para comunicação:

  * **JSONL** para controle.
  * **Apache Arrow IPC** para dados.
* Usar **YAML** como formato canônico de configuração.
* Usar structs Rust tipadas como modelo interno de configuração.
* Ter uma arquitetura portável, começando por Linux, mas sem bloquear suporte futuro a Windows.
* Suportar configuração local e remota via **configuration providers**.
* Suportar resolução segura de secrets via providers.
* Ter boa observabilidade desde o início.

### 2.2 Não objetivos iniciais

* Não ser uma engine de transformação pesada.
* Não substituir dbt.
* Não criar uma DSL complexa para SQL, joins ou transformações analíticas.
* Não executar processamento distribuído no MVP.
* Não buscar compatibilidade 100% com Embulk.
* Não carregar plugins como bibliotecas dinâmicas dentro do processo principal.
* Não usar `stderr` como canal normal de logs.
* Não usar MessagePack no protocolo de controle inicial.
* Não aceitar JSON multiline ou JSON sem delimitação no protocolo de controle.

---

## 3. Motivação

A motivação principal vem das limitações observadas no Embulk e em seus plugins:

* plugins antigos ou desatualizados;
* dificuldade com drivers modernos;
* comportamento inconsistente de fetch e streaming;
* risco de estouro de memória;
* pouca previsibilidade em cargas grandes;
* dificuldade de evolução do ecossistema;
* baixa ergonomia para criar ou manter plugins;
* ausência de uma separação clara entre plano de controle e plano de dados.

Um caso concreto foi o comportamento de leitura em bancos via JDBC/MySQL, especialmente em relação a `fetch_rows`, cursor server-side e uso de memória. Esse tipo de problema reforça a necessidade de uma ferramenta com streaming real, contratos claros e comportamento previsível.

---

## 4. Arquitetura geral

A arquitetura do Loadsmith é baseada em três partes principais:

```text
Loadsmith Core
  ├── Configuração YAML
  ├── Resolução de templates
  ├── Resolução de providers/secrets
  ├── Modelo interno tipado em Rust
  ├── Orquestração de plugins
  ├── Controle de execução
  ├── Observabilidade
  └── Estado/checkpoints

Plugins
  ├── Sources
  ├── Destinations
  ├── Parsers
  └── Configuration/Secret Providers

Protocolos
  ├── Control plane: JSONL
  ├── Data plane: Apache Arrow IPC
  └── Event/log plane: JSONL/eventos estruturados
```

O core não deve conhecer detalhes internos de Oracle, MySQL, Snowflake, S3, Excel, CSV ou qualquer outra tecnologia específica. Esses detalhes pertencem aos plugins.

---

## 5. Core

O **Loadsmith Core** é o orquestrador da execução.

Responsabilidades:

* ler a configuração YAML;
* fazer parse seguro da configuração;
* resolver templates;
* resolver referências externas de configuração;
* resolver secrets;
* converter a configuração resolvida para structs Rust tipadas;
* validar o pipeline;
* descobrir plugins disponíveis;
* iniciar plugins como processos filhos;
* negociar protocolo e capabilities;
* coordenar fluxo entre source e destination;
* controlar lifecycle dos plugins;
* aplicar cancelamento, timeout e tratamento de falhas;
* receber eventos, progresso e logs;
* consolidar métricas;
* gerenciar estado/checkpoints quando aplicável.

O core deve permanecer pequeno e genérico.

---

## 6. Modelo de configuração

O formato canônico de configuração do Loadsmith será **YAML**.

YAML significa **YAML Ain’t Markup Language**.

A escolha por YAML foi feita porque pipelines reais tendem a conter listas grandes e estruturas repetidas, como:

* colunas de arquivos XLSX/CSV;
* transforms;
* validações;
* múltiplas abas;
* múltiplos outputs;
* regras de rejeição;
* políticas de schema drift;
* metadados;
* steps ordenados.

Esses casos são mais legíveis e mais fáceis de manter em YAML do que em TOML.

### 6.1 Modelo interno

Internamente, o Loadsmith não deve trabalhar com mapas dinâmicos soltos.

O modelo interno será composto por **structs Rust tipadas**, com serialização e desserialização via **Serde**.

Serde não é um formato. Serde é o framework/ecossistema padrão em Rust para serializar e desserializar structs.

Fluxo conceitual:

```text
pipeline.yaml
  -> parse seguro de YAML
  -> resolução de templates
  -> resolução de configuration providers
  -> resolução de secrets
  -> desserialização para structs Rust tipadas
  -> validação forte
  -> execução
```

### 6.2 Separação entre config do core e config dos plugins

O core valida os blocos que pertencem ao próprio Loadsmith:

```yaml
pipeline:
  name: vendas_mysql_to_s3

state:
  backend: s3
  path: s3://bucket/state.json
```

O core também conhece o envelope dos plugins:

```yaml
source:
  type: jdbc
  config:
    ...
```

Mas o conteúdo de `source.config`, `destination.config`, `parser.config` ou `provider.config` deve ser validado pelo plugin responsável.

Modelo conceitual em Rust:

```rust
#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    pub pipeline: PipelineMeta,
    pub source: PluginRef,
    pub destination: PluginRef,
    pub state: Option<StateConfig>,
    pub rejects: Option<RejectsConfig>,
}

#[derive(Debug, Deserialize)]
pub struct PipelineMeta {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PluginRef {
    #[serde(rename = "type")]
    pub plugin_type: String,

    pub config: serde_yaml::Value,
}
```

O ponto importante é que o core é tipado onde ele manda, e flexível onde precisa delegar ao plugin.

---

## 7. YAML controlado

O Loadsmith deve usar YAML como formato canônico, mas não deve aceitar YAML “livre e selvagem”.

Regras propostas:

* usar parser seguro;
* preferir compatibilidade com YAML 1.2 quando possível;
* validar tudo com schema interno;
* campos desconhecidos devem gerar erro;
* tipos devem ser validados pelas structs Rust;
* templates devem ser resolvidos antes da validação final, ou em fases bem definidas;
* anchors, aliases e merge keys devem ser evitados no MVP;
* strings ambíguas devem ser explicitamente strings;
* a configuração resolvida deve poder ser impressa com `--print-resolved-config`.

Exemplos de valores que devem ser tratados com cuidado:

```yaml
codigo: "00123"
data_referencia: "2026-06-05"
ativo: true
```

---

## 8. Plugins

Os plugins são executáveis separados, iniciados pelo core como processos filhos.

Essa decisão evita problemas de ABI, runtime, dependências nativas e compatibilidade entre linguagens.

ABI significa **Application Binary Interface**.

### 8.0 Descoberta de plugins

O Loadsmith gerencia plugins em um diretório dedicado.

Diretório padrão:

```text
~/.loadsmith/plugins/
```

Sobrescrito pela variável de ambiente:

```text
LOADSMITH_PLUGIN_PATH=/caminho/alternativo
```

Comandos de gerenciamento:

```bash
loadsmith plugin install ./meu-plugin-binario
loadsmith plugin list
loadsmith plugin remove jdbc
```

O comando `install` copia ou linka o binário para o diretório gerenciado. O core busca plugins somente nesse diretório — não faz varredura de PATH.

Para desenvolvimento local, é possível apontar para um diretório alternativo:

```bash
loadsmith run pipeline.yaml --plugin-dir ./plugins/
```

Essa separação garante que o ambiente de produção seja auditável e reproduzível: apenas binários explicitamente instalados são usados.

Plugins podem ser escritos em:

* Rust;
* Python;
* Go;
* Java;
* C#;
* Node;
* qualquer linguagem capaz de implementar o protocolo do Loadsmith.

### 8.1 Tipos principais de plugin

#### Source

Responsável por ler dados de uma origem.

Exemplos:

* JDBC;
* MySQL;
* PostgreSQL;
* Oracle;
* Snowflake;
* S3;
* arquivo local;
* SharePoint;
* APIs HTTP.

#### Destination

Responsável por escrever dados em um destino. Para bancos, grava via protocolo
nativo (consome Arrow direto). Para arquivos, escreve em um diretório de staging
local; a entrega remota fica a cargo de um **sink** (abaixo).

Exemplos:

* arquivo local/staging em Parquet, CSV, JSONL;
* stdout;
* PostgreSQL;
* Snowflake;
* Redshift.

#### Sink

Fase opcional de **entrega**, fora do data plane. Separa *formato* (o destination)
de *localização* (o sink), evitando a explosão N×M de plugins tipo `s3_parquet`.
Um destination de arquivo escreve os arquivos no staging e anuncia cada um pronto
via `object_ready` (fd4); o sink entrega cada arquivo finalizado ao destino remoto.
Só é válido com destinations que anunciam a capability `object_output`. O core é o
dono do ledger de entrega — se o sink cair, é respawnado e retoma de onde parou.

Exemplos:

* cópia local (`local-copy`);
* S3;
* Google Cloud Storage;
* sftp;
* e-mail.

#### Parser

Responsável por converter um formato bruto em dados tabulares.

Exemplos:

* CSV;
* JSON;
* Excel/XLSX;
* XML;
* Parquet.

#### Configuration Provider

Responsável por carregar configuração externa.

Exemplos:

* arquivo local;
* S3;
* Google Cloud Storage;
* Azure Blob Storage;
* HTTP/HTTPS;
* AWS Secrets Manager;
* variáveis de ambiente.

---

## 9. Comunicação entre core e plugins

A comunicação deve ser separada por finalidade.

### 9.1 Control plane

O plano de controle usa **JSONL**.

JSONL significa **JSON Lines**.
JSON significa **JavaScript Object Notation**.

Cada mensagem de controle é um objeto JSON serializado em uma única linha e delimitado por `\n`.

Exemplo:

```jsonl
{"type":"handshake","protocol_version":1,"plugin":"loadsmith-source-jdbc"}
{"type":"capabilities_request"}
{"type":"progress","rows_read":10000,"batches":1}
{"type":"finished","status":"success"}
```

O Loadsmith não considera JSON sem delimitação nem JSON multiline como formato de protocolo de controle. Sempre que o documento mencionar JSON no control plane, a decisão correta é **JSONL**.

Responsabilidades do control plane:

* handshake;
* capabilities;
* configuração;
* início de execução;
* cancelamento;
* schema;
* status;
* progresso;
* erro;
* finalização.

Exemplos conceituais de mensagens:

```text
Handshake
CapabilitiesRequest
CapabilitiesResponse
Configure
Start
Schema
Progress
LogEvent
Error
Finished
Cancel
```

### 9.2 Por que JSONL no controle

A escolha por JSONL favorece:

* simplicidade;
* debugabilidade;
* interoperabilidade entre linguagens;
* facilidade para logging;
* inspeção direta com `cat`, `tail`, `jq` e ferramentas comuns;
* menor barreira para criação de plugins comunitários.

Como o volume pesado de dados trafega pelo data plane usando Apache Arrow IPC, o overhead de JSONL no plano de controle é aceitável.

O MessagePack pode ser reavaliado futuramente se o control plane virar gargalo, mas não será o formato inicial.

### 9.3 Data plane

O plano de dados usa **Apache Arrow IPC**.

IPC significa **Inter-Process Communication**.

Responsável por transportar batches tabulares entre plugins.

O source deve produzir batches Arrow. O destination deve consumir batches Arrow.

### 9.4 Event/log plane

Logs e eventos devem trafegar por canal dedicado, não por `stderr`.

O formato natural para eventos/logs estruturados também será **JSONL**.

Eventos desejados:

* início de etapa;
* fim de etapa;
* progresso;
* linhas processadas;
* bytes lidos/escritos;
* warnings;
* erros estruturados;
* métricas.

---

## 10. Transporte local

No Linux, o desenho inicial pode usar:

```text
stdin/stdout -> controle JSONL
fd3          -> dados Arrow IPC
fd4          -> eventos/logs JSONL
```

Porém, isso deve ficar atrás de uma abstração de transporte.

A ideia é suportar Linux primeiro, mas sem impedir Windows no futuro.

Possíveis implementações:

```text
LinuxTransport
  -> pipes/file descriptors

WindowsTransport
  -> named pipes

FallbackTransport
  -> TCP local, apenas se necessário
```

TCP local não é a preferência, mas pode ser avaliado como fallback.

---

## 11. Templates

O Loadsmith suporta templates na configuração via um **parser de expressões interno**, sem dependência de engines externas como Jinja2, Liquid ou Tera.

A sintaxe usa `{{ }}` como delimitador. As expressões suportadas são:

* chamada de função: `{{ env('VAR') }}`
* acesso encadeado com argumento: `{{ aws.sm('/path').field }}`
* combinações: `{{ aws.sm('/dev/segredo').username }}`

Exemplos:

```yaml
url: "{{ env('DATABASE_URL') }}"
```

```yaml
username: "{{ aws.sm('/dev/segredo').username }}"
password: "{{ aws.sm('/dev/segredo').password }}"
```

O parser resolve expressões dentro de valores string do YAML. Ele não suporta loops, condicionais, filtros ou herança — esses recursos não fazem parte do escopo. O surface area é intencional e pequeno: chamada de função, acesso a campo por ponto, literal de string como argumento.

Se a necessidade crescer no futuro, a migração para MiniJinja é viável sem quebrar a sintaxe, já que o delimitador `{{ }}` é compatível.

---

## 12. Configuration Providers

Configuration providers permitem carregar a configuração a partir de fontes externas. Providers são plugáveis — a comunidade pode criar novos.

### 12.1 Formato de referência

A regra é: se o scheme URI identifica o provider de forma única, usa URI puro. Se o provider usa `https` por baixo (e o scheme não identifica sozinho), usa `provider|url`.

| Referência | Provider |
|---|---|
| `file:///opt/config.yml` | arquivo local |
| `s3://bucket/key` | S3 |
| `gs://bucket/key` | Google Cloud Storage |
| `https://exemplo.com/config.yml` | HTTP simples (download direto, sem auth especial) |
| `azure\|https://account.blob.core.windows.net/container/blob` | Azure Blob Storage |

`https://` puro significa HTTP download direto sem lógica proprietária. Qualquer provider que use HTTPS mas precise de autenticação ou lógica própria (Azure, SharePoint, OAuth etc.) usa o prefixo `provider|` explícito.

### 12.2 Uso

Via variável de ambiente:

```bash
LOADSMITH_CONFIG_REF="s3://bucket/path/config.yml"
LOADSMITH_CONFIG_REF="azure|https://account.blob.core.windows.net/container/config.yml"
```

Via flag CLI:

```bash
loadsmith run --config-ref "s3://bucket/path/config.yml"
```

### 12.3 Providers built-in previstos

* `file://` — arquivo local
* `s3://` — Amazon S3
* `gs://` — Google Cloud Storage
* `https://` / `http://` — HTTP simples
* `azure|https://` — Azure Blob Storage

Providers adicionais podem ser implementados como plugins seguindo o mesmo contrato.

---

## 13. Secrets

Secrets não devem ficar hardcoded na configuração.

Secrets são resolvidos **inline no template**, usando a mesma sintaxe `{{ }}` dos templates de configuração. Não há bloco separado de secrets — o valor é resolvido no lugar onde é referenciado.

```yaml
source:
  type: jdbc
  config:
    username: "{{ aws.sm('/dev/mysql').username }}"
    password: "{{ aws.sm('/dev/mysql').password }}"
```

```yaml
url: "{{ env('DATABASE_URL') }}"
```

Providers possíveis:

* `env('VAR')` — variável de ambiente;
* `aws.sm('/path').field` — AWS Secrets Manager;
* `aws.ssm('/path')` — AWS SSM Parameter Store;
* `file('/path').field` — arquivo local;
* Vault, futuramente;
* Azure Key Vault, futuramente;
* Google Secret Manager, futuramente.

Secrets nunca devem aparecer em logs, eventos ou na saída de `--print-resolved-config`. O core é responsável por mascarar qualquer valor resolvido via provider de secret.

---

## 14. Pipeline interno

Modelo conceitual simples:

```text
Source -> Parser -> Buffer -> Destination
```

Modelo possível no futuro:

```text
Source -> Parser -> Filter -> Buffer -> Destination
```

O cuidado principal é não transformar o Loadsmith em ferramenta de transformação pesada.

Transformações aceitáveis:

* parsing;
* normalização de tipos;
* inferência de schema;
* conversão de formatos;
* ajustes simples necessários para carga.

Transformações não desejadas no core:

* joins complexos;
* regras analíticas pesadas;
* DSL própria para SQL;
* engine relacional interna.

DSL significa **Domain-Specific Language**.

Para SQL complexo, o ideal é deixar o banco fazer:

```yaml
source:
  type: jdbc
  config:
    query: |
      select ...
      from ...
      join ...
```

---

## 15. Apache Arrow e batches

O Loadsmith deve usar batches tabulares como unidade de processamento.

Características desejadas:

* leitura em lotes;
* escrita em lotes;
* backpressure;
* baixo uso de memória;
* controle de tamanho de batch;
* schema explícito;
* compatibilidade com Parquet;
* interoperabilidade entre linguagens.

O formato escolhido para o data plane é:

```text
Apache Arrow IPC
```

---

## 16. Performance e memória

Requisitos importantes:

* não carregar tudo em memória;
* suportar datasets grandes;
* evitar estouro de heap/memória;
* permitir batch size configurável;
* controlar backpressure;
* permitir escrita incremental;
* expor métricas de throughput;
* permitir diagnóstico de gargalos.

Métricas desejadas:

* linhas lidas;
* linhas escritas;
* bytes lidos;
* bytes escritos;
* batches processados;
* tempo por etapa;
* velocidade média;
* velocidade recente;
* uso aproximado de memória, se possível.

---

## 17. Estado e checkpoints

O Loadsmith deve ter espaço para estado, principalmente para cargas incrementais.

**O core é o único dono do estado.** Plugins são ferramentas sem memória — eles executam, reportam progresso via JSONL, e encerram. Quem persiste, lê e gerencia o estado é sempre o core.

Casos de uso:

* último timestamp processado;
* último ID;
* offset;
* cursor;
* paginação;
* `search_after`;
* último arquivo processado;
* checkpoint de escrita.

O plugin pode reportar ao core o valor de estado que deve ser persistido (ex: último ID lido), mas a decisão de quando e como persistir é do core.

Possível configuração futura:

```yaml
state:
  backend: local
  path: .loadsmith/state
```

Ou:

```yaml
state:
  backend: s3
  path: s3://bucket/loadsmith/state/
```

Pontos ainda em aberto:

* como o plugin comunica ao core o valor de estado que deseja persistir;
* como garantir consistência em falha parcial;
* como fazer retry idempotente;
* formato exato do arquivo de estado.

---

## 18. Observabilidade

O Loadsmith deve ter observabilidade desde o início.

Saídas desejadas:

* logs humanos no terminal;
* logs estruturados em JSONL;
* eventos de progresso em JSONL;
* erros com contexto;
* métricas por etapa;
* resumo final da execução.

Exemplo de resumo final:

```text
Pipeline: mysql_to_s3
Status: success
Rows read: 21,473,000
Rows written: 21,473,000
Batches: 2,148
Duration: 00:18:42
Average throughput: 19,139 rows/s
Destination: s3://bucket/raw/table/
```

Possível suporte futuro:

* Prometheus;
* OpenTelemetry;
* CloudWatch;
* Datadog;
* arquivo JSONL;
* eventos para orquestradores externos.

---

## 19. Compatibilidade com Embulk

O Loadsmith é inspirado pelo Embulk, mas não precisa ser compatível com ele.

Compatibilidades desejáveis:

* configuração declarativa;
* conceito de input/output;
* templates;
* plugins;
* execução de pipelines.

Não objetivos:

* compatibilidade total com YAML de Embulk;
* compatibilidade com plugins de Embulk;
* replicar o runtime antigo;
* manter decisões arquiteturais problemáticas.

---

## 20. CLI

Comandos possíveis:

```bash
loadsmith run pipeline.yaml
loadsmith run --config-ref "s3|s3://bucket/config.yaml"
loadsmith validate pipeline.yaml
loadsmith plugins list
loadsmith plugins inspect jdbc
loadsmith capabilities jdbc
```

Comandos úteis para debug:

```bash
loadsmith run pipeline.yaml --log-level debug
loadsmith run pipeline.yaml --dry-run
loadsmith run pipeline.yaml --print-resolved-config
```

---

## 21. Estrutura inicial do projeto

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
```

Separação conceitual:

```text
loadsmith-protocol
  Tipos das mensagens de controle JSONL.

loadsmith-transport
  Abstração de pipes, file descriptors, named pipes etc.

loadsmith-plugin-sdk
  Helpers para criar plugins.

loadsmith-core
  Orquestração da execução.

loadsmith-cli
  Interface de linha de comando.

loadsmith-config
  Configuração YAML, templates e providers.

loadsmith-arrow
  Utilidades de Arrow IPC e schemas.
```

---

## 22. MVP

Um MVP realista deve validar a arquitetura sem tentar resolver tudo.

### 22.1 MVP mínimo

* CLI `loadsmith run`;
* leitura de YAML local;
* templates básicos;
* variáveis de ambiente;
* modelo interno em structs Rust;
* plugins como processos filhos;
* control plane com JSONL;
* data plane com Arrow IPC;
* canal dedicado para eventos/logs em JSONL;
* source simples;
* destination simples;
* logs e resumo final.

Plugins possíveis para MVP mínimo:

```text
source:
  local_csv

destination:
  local_parquet
  stdout
```

### 22.2 MVP mais útil para ambiente corporativo

* source JDBC;
* destination S3 Parquet;
* configuration provider local e S3;
* secrets via env e AWS Secrets Manager;
* batch size configurável;
* logs estruturados;
* resumo final;
* tratamento básico de erro.

Pipeline alvo:

```text
JDBC -> Arrow batches -> S3 Parquet
```

---

## 23. Decisões consolidadas

* Nome do projeto: **Loadsmith**.
* Identidade visual/conceitual: forja, bigorna, ferreiro.
* Ferramenta focada em EL.
* Não deve virar engine de transformação pesada.
* Core pequeno e genérico.
* Arquitetura plugin-first.
* Plugins como executáveis/processos separados.
* Configuração canônica em YAML.
* Modelo interno em structs Rust tipadas.
* Serde como camada de serialização/desserialização.
* Controle via JSONL.
* Dados via Apache Arrow IPC.
* Logs/eventos em canal dedicado.
* Logs/eventos estruturados em JSONL.
* Não usar `stderr` como canal normal de logs.
* Linux first.
* Windows depois, por abstração de transporte.
* TCP localhost não é preferência.
* Templates são necessários.
* Configuration providers devem ser plugáveis.
* Secrets providers são necessários.
* Plugins em outras linguagens devem ser possíveis.
* SQL complexo deve ficar no banco/query, não em DSL própria do Loadsmith.
* **Linguagem do core: Rust.**
* **Runtime assíncrono: Tokio.**
* **Templates via parser de expressões interno** — sem engine externa; sintaxe `{{ func(arg) }}` e `{{ obj.method(arg).field }}`; MiniJinja como opção de migração futura se necessário.
* **Descoberta de plugins via diretório gerenciado** — `~/.loadsmith/plugins/` por padrão; sobrescrito por `LOADSMITH_PLUGIN_PATH`; instalação via `loadsmith plugin install`; flag `--plugin-dir` para desenvolvimento local.
* **Core é o único dono do estado** — plugins reportam progresso e valores de estado ao core via JSONL, mas não persistem nada diretamente.
* **Protocolo JSONL formalizado** — versionamento por inteiro ordinal começando em `1`; handshake iniciado pelo core; shapes de todas as mensagens definidos em `definitions/protocol.md`.
* **Secrets resolvidos inline no template** — mesma sintaxe `{{ }}`, sem bloco separado; core é responsável por mascarar valores em logs e eventos.
* **Formato de referência de configuration providers** — URI puro quando o scheme identifica o provider (`s3://`, `gs://`, `file://`, `https://`); prefixo `provider|url` quando o provider usa HTTPS mas tem lógica própria (`azure|https://...`).
* **Multi-arch first** — imagens publicadas para `linux/amd64` e `linux/arm64` (AWS Graviton); a regra não é "sem TLS", é **sem código nativo/assembly por arquitetura**. O build é `cargo build`-nativo dentro de cada arch (sob QEMU), então crypto em C/assembly significaria build emulado lento e toolchain frágil (`cmake`/`perl`/`nasm`).
* **TLS via `rustls` + `rustls-rustcrypto`** — protocolo TLS pela camada madura `rustls` (decisão foundational); primitivas pelo provider puro-Rust `rustls-rustcrypto`. Banidos por serem crypto nativo: `native-tls`, `openssl-sys`, `ring`, `aws-lc-rs`. O provider é trocável via `CryptoProvider::install_default` (knob reversível, blast radius por-binário pela isolação de processo); vale uniformemente para todos os plugins de rede (postgres, mysql, s3).

---

## 24. Decisões em aberto

* Definir contrato formal de capabilities (extensão futura além das built-in).
* Definir como o plugin comunica ao core o valor de estado que deseja persistir.
* Definir formato do arquivo de estado e garantias em falha parcial.
* Definir retry e idempotência.
* Definir backpressure.
* Definir plugin JDBC genérico vs plugins nativos por banco.
* Definir empacotamento e distribuição de plugins.
* Definir validação de configuração por plugin (JSON Schema, contrato próprio, etc).
* Definir estratégia de testes de compatibilidade entre core e plugins.

---

## 25. Contrato inicial core/plugin

Ponto prioritário para aprofundar.

Fluxo conceitual:

```text
1. Core inicia plugin.
2. Plugin participa do handshake via JSONL.
3. Core verifica versão do protocolo.
4. Core pede capabilities.
5. Plugin responde capabilities.
6. Core envia configuração.
7. Plugin valida/prepara.
8. Core inicia execução.
9. Plugin source produz batches Arrow.
10. Plugin destination consome batches Arrow.
11. Plugins emitem progresso, logs e eventos em JSONL.
12. Core finaliza execução ou cancela em caso de erro.
```

Capabilities possíveis:

```yaml
plugin:
  name: jdbc
  kind: source
  protocol_version: 1
  supports:
    - schema_inference
    - batch_read
    - incremental_state
  config_schema:
    ...
```

Exemplo conceitual de mensagem JSONL:

```jsonl
{"type":"handshake","protocol_version":1,"plugin_name":"jdbc","plugin_version":"0.1.0"}
{"type":"capabilities_response","kind":"source","supports":["schema_inference","batch_read"]}
{"type":"progress","rows_read":10000,"batches_read":1}
{"type":"error","code":"JDBC_CONNECTION_FAILED","message":"failed to connect to database"}
{"type":"finished","status":"success"}
```

---

## 26. Perguntas para próxima etapa

1. O core será oficialmente em Rust?
2. Tokio será o runtime assíncrono padrão?
3. O primeiro source será `local_csv` ou `jdbc`?
4. O primeiro destination será `stdout`, `local_parquet` ou `s3_parquet`?
5. O MVP deve validar arquitetura ou já resolver um caso corporativo real?
6. O plugin SDK será parte do MVP?
7. Como os plugins serão encontrados?
8. Qual será o formato mínimo de uma mensagem de erro?
9. Como o core vai saber que um plugin é compatível?
10. Como versionar o protocolo JSONL?
11. Como separar mensagens de controle, eventos e logs?
12. O canal de eventos/logs será obrigatório desde o MVP ou pode começar junto do controle?

---

## 27. Frase de visão

> **Loadsmith é uma ferramenta moderna de EL, plugin-first, que orquestra extrações e cargas de dados usando configuração YAML, plugins isolados por processo, controle via JSONL e dados tabulares via Apache Arrow IPC.**

Outra formulação:

> **Um Embulk moderno, mais seguro, performático e extensível, feito para pipelines declarativos de ingestão de dados em ambientes locais, cloud e corporativos.**
