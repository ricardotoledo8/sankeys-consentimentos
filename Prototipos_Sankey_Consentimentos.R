# ============================================================
# EXPLICAÇÃO DO CODIGO --------------------------------------
# ============================================================
### Etapas do Processo

## 1. Consulta SQL
#Une as tabelas webhook_payload, empresa, link_consentimento e link_cliente.
#Retorna status de consentimentos com metadados (empresa, CPF/CNPJ, banco).

## 2. Análise de Últimos Status
# Identifica quais consentimentos iniciaram (STARTED).
# Calcula o último status de cada fluxo.
# Gera tabelas por empresa: totais, distribuições e percentuais.

## 3. Preparação de Fluxos Contínuos
# Normaliza dados (datas, empresas).
# Remove repetições consecutivas de status.
# Corta antes do primeiro STARTED.
# Resultado: cada (id_link, nm_empresa) vira um caminho sequencial.

## 4. Construção de Transições
# Para cada caminho, calcula pares status -> próximo status.
# Tabelas de transição global e por empresa.

## 5. Visualização Sankey
# build_nodes_links() cria nós e links (com contagens).
# make_sankey() monta gráfico interativo com networkD3.
# Cores fixas por status (palette_status).

## 6. Geração dos Sankeys
# Exporta HTML self-contained:
# sankey_global.html
# sankey_<empresa>.html

## 7. Dashboard Index
# Cria index.html listando todos os Sankeys.
#  Publicado via GitHub Pages para navegação web.



# ============================================================
# 0) Pacotes --------------------------------------------------
# ============================================================
pkgs <- c("DBI","dplyr","tidyr","ggplot2","ggalluvial",
          "scales","networkD3","htmlwidgets","htmltools")
to_install <- pkgs[!pkgs %in% installed.packages()[, "Package"]]
if (length(to_install)) install.packages(to_install)
invisible(lapply(pkgs, library, character.only = TRUE))


# Crie a conexão com o PostgreSQL na AWS Prod
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = "xx",
  host     = "xxxxx",
  port     = xx,
  user     = "xx",
  password = "xxxxx"   
)


# ============================================================
# 1) Consulta no Banco ----------------------------------------
# ============================================================
sql <- "
SELECT
  ewp.id_link,
  ewp.dt_insercao,
  ewp.desc_status,
  ee.nm_empresa,
  elc.nr_cpf,
  elc.nr_cnpj,
  elc.nm_banco,
  rlc.id_cliente_externo,
  rlc.id_conta_externo,
  rlc.tp_conta,
  rlc.dt_insercao AS dt_insercao_cliente
FROM iniciador.en_webhook_payload AS ewp
LEFT JOIN iniciador.re_link_consentimento_empresa AS rlce
  ON rlce.id_link = ewp.id_link
LEFT JOIN iniciador.en_empresa AS ee
  ON ee.id_empresa = rlce.id_empresa
LEFT JOIN iniciador.en_link_consentimento AS elc
  ON elc.id_link = ewp.id_link
LEFT JOIN iniciador.re_link_cliente AS rlc
  ON rlc.id_link = ewp.id_link
ORDER BY ewp.dt_insercao DESC
;"

Consentimentos <- dbGetQuery(con, sql)


# ============================================================
# 2) Análise de Últimos Status -------------------------------
# ============================================================
# Links que tiveram STARTED
started_ids <- Consentimentos %>%
  filter(desc_status == "STARTED") %>%
  distinct(id_link)

# Último status entre os que tiveram STARTED
latest_for_started <- Consentimentos %>%
  semi_join(started_ids, by = "id_link") %>%
  arrange(id_link, desc(dt_insercao)) %>%
  group_by(id_link) %>%
  summarise(
    nm_empresa  = first(replace_na(nm_empresa, "desconhecida")),
    last_status = first(desc_status),
    .groups = "drop"
  )

# Distribuição por empresa e status final
breakdown <- latest_for_started %>%
  count(nm_empresa, last_status, name = "qtde")

# Total iniciados por empresa
totais <- breakdown %>%
  group_by(nm_empresa) %>%
  summarise(started_id_links = sum(qtde), .groups = "drop")

# Tabela final com percentuais
tabela_final <- breakdown %>%
  left_join(totais, by = "nm_empresa") %>%
  mutate(pct = qtde / started_id_links) %>%
  pivot_wider(
    names_from  = last_status,
    values_from = c(qtde, pct),
    values_fill = list(qtde = 0, pct = 0),
    names_glue  = "{last_status}_{.value}"
  ) %>%
  relocate(started_id_links, .after = nm_empresa) %>%
  arrange(desc(started_id_links))


# ============================================================
# 3) Preparação de Fluxos Contínuos (Fluxo_clean é o fluxo limpo e sem duplicações) ------------------------
# ============================================================
prep_fluxo <- function(df) {
  df %>%
    mutate(
      nm_empresa  = tidyr::replace_na(nm_empresa, "desconhecida"), # troca NA por "desconhecida" para não quebrar agrupamentos e contagens.
      dt_insercao = as.POSIXct(dt_insercao, tz = attr(dt_insercao, "tzone")) # garante classe POSIXct. O tz = attr(..., "tzone") tenta preservar o fuso que já está na coluna. (Se não houver timezone, o R usa o default da sessão.)
    ) %>%
    arrange(id_link, nm_empresa, dt_insercao) %>%
    group_by(id_link, nm_empresa) %>% # Cada grupo  é tratado individualmente.
    filter(is.na(lag(desc_status)) | desc_status != lag(desc_status)) %>% # Explicação "Exp1" abaixo
    mutate(from_started = cumany(desc_status == "STARTED")) %>% # Explicação "Exp2"
    filter(from_started) %>% # Exp3
    ungroup()
}

Fluxo_clean <- prep_fluxo(Consentimentos)

# Exp1
# Remove repetições consecutivas do mesmo status no fluxo.
# O primeiro registro de cada grupo tem lag(...) = NA → passa no is.na(...).
# Se dois status iguais vierem seguidos (ex.: STARTED, STARTED), a segunda ocorrência é filtrada fora.
# Não remove repetições não consecutivas (ex.: STARTED, EXPIRED, STARTED), o que é desejado para manter a história.

# Exp2
# Cria uma flag booleana: vira TRUE a partir do primeiro STARTED e permanece TRUE nos registros seguintes daquele fluxo.
# cumany() é um “OU cumulativo”: primeira vez que a condição é verdadeira, tudo dali pra frente fica TRUE.

# Exp3
# Mantém apenas linhas a partir do STARTED (incluindo o próprio STARTED).
# Fluxos sem STARTED ficam com from_started == FALSE em todas as linhas → somem nessa filtragem (o que geralmente é o que queremos).



# ============================================================
# 4) Construção de Transições --------------------------------
# ============================================================
build_transitions <- function(fluxo_clean) {
  base <- fluxo_clean %>%
    group_by(id_link, nm_empresa) %>% # Cada fluxo (trilha de eventos) é tratado individualmente.
    arrange(dt_insercao, .by_group = TRUE) %>% # Ordena cronologicamente cada fluxo.
    mutate(next_status = lead(desc_status)) %>% # Para cada linha, olha o status da linha seguinte e cria a coluna next_status
    ungroup() %>% # Remove agrupamento (deixa tabela plana).
    filter(!is.na(next_status)) # Descarta a última linha de cada fluxo (porque não tem próximo status → next_status = NA).
  
  list(
    global  = base %>% count(source = desc_status, target = next_status, name = "value"), # agrega todas as transições do dataset
    empresa = base %>% count(nm_empresa, source = desc_status, target = next_status, name = "value")
  )
}

TR <- build_transitions(Fluxo_clean)
Transicoes_global  <- TR$global # tabela para para Sankey consolidado.
Transicoes_empresa <- TR$empresa # tabela para para Sankey por empresa


# ============================================================
# 5) Funções para Sankey -------------------------------------
# ============================================================
# Monta nós e links com contagem
build_nodes_links <- function(links_df) {
  out <- links_df %>% group_by(source) %>% summarise(out = sum(value), .groups = "drop")
  inc <- links_df %>% group_by(target) %>% summarise(inc = sum(value), .groups = "drop")
  
  all_nodes <- sort(unique(c(links_df$source, links_df$target)))
  nodes <- data.frame(name = all_nodes, stringsAsFactors = FALSE) %>%
    left_join(out, by = c("name" = "source")) %>%
    left_join(inc, by = c("name" = "target")) %>%
    mutate(out = replace_na(out, 0),
           inc = replace_na(inc, 0),
           flow = pmax(out, inc),
           label = paste0(name, " — ", flow))
  
  links <- links_df %>%
    mutate(source_id = match(source, nodes$name) - 1L,
           target_id = match(target, nodes$name) - 1L) %>%
    select(source_id, target_id, value)
  
  list(nodes = nodes, links = links)
}

# Cria Sankey
make_sankey <- function(links_df, title, color_map = NULL,
                        sinksRight = TRUE, nodeWidth = 30,
                        fontSize = 12, nodePadding = 40) {
  bl <- build_nodes_links(links_df)
  if (is.null(bl$nodes)) return(NULL)
  
  colorScale <- NULL
  if (!is.null(color_map)) {
    cm <- color_map[names(color_map) %in% bl$nodes$name]
    dom <- jsonlite::toJSON(unname(names(cm)))
    rng <- jsonlite::toJSON(unname(unname(cm)))
    colorScale <- paste0("d3.scaleOrdinal().domain(", dom, ").range(", rng, ")")
  }
  
  w <- sankeyNetwork(
    Links = bl$links, Nodes = bl$nodes,
    Source = "source_id", Target = "target_id",
    Value = "value", NodeID = "label",
    sinksRight = sinksRight, nodeWidth = nodeWidth,
    fontSize = fontSize, nodePadding = nodePadding,
    colourScale = colorScale
  )
  prependContent(w, tags$h3(title))
}


# ============================================================
# 6) Geração dos Sankeys -------------------------------------
# ============================================================
OUT_DIR <- "sankey_out"
dir.create(OUT_DIR, showWarnings = FALSE)

# Paleta manual (edite conforme necessário)
palette_status <- c(
  "STARTED"                     = "#E91E63",
  "AWAITING_AUTHORIZATION"      = "#C0CA33",
  "AWAITING_LGPD_AUTHORISATION" = "#8BC34A",
  "AUTHORISED"                  = "#FF9800",
  "EXPIRED"                     = "#9E9E9E",
  "REJECTED"                    = "#00BCD4",
  "ERROR"                       = "#43A047",
  "REVOKED"                     = "#7E57C2",
  "END"                         = "#BDBDBD"
)

# Global
w_global <- make_sankey(Transicoes_global, "Fluxo Global", palette_status)
saveWidget(w_global, file.path(OUT_DIR, "sankey_global.html"), selfcontained = TRUE)

# Por empresa
empresas <- sort(unique(Transicoes_empresa$nm_empresa))
for (e in empresas) {
  links_e <- Transicoes_empresa %>% filter(nm_empresa == e) %>% select(source, target, value)
  w_e <- make_sankey(links_e, paste("Fluxo —", e), palette_status)
  file_e <- file.path(OUT_DIR, paste0("sankey_", gsub("[^A-Za-z0-9_-]+","_", e), ".html"))
  saveWidget(w_e, file_e, selfcontained = TRUE)
}


# ============================================================
# 7) Index Bonito (Dashboard com Cards) ----------------------
# ============================================================
make_index_dashboard <- function(dir = ".", titulo = "Sankeys — Fluxos de Consentimento") {
  files <- list.files(dir, pattern = "^sankey_.*\\.html$", full.names = FALSE)
  files <- c("sankey_global.html", setdiff(files, "sankey_global.html"))
  
  to_title <- function(x) {
    x <- sub("^sankey_", "", x)
    x <- sub("\\.html$", "", x)
    gsub("[-_]+", " ", x)
  }
  
  items <- lapply(files, function(f) {
    name <- to_title(f)
    badge <- if (f == "sankey_global.html") '<span class="badge">Global</span>' else ""
    sprintf('<a class="card" href="%s" target="_blank"><div class="title">%s %s</div></a>', f, name, badge)
  })
  
  html <- paste0("<html><head><meta charset='utf-8'><title>", titulo, "</title></head><body>",
                 "<h1>", titulo, "</h1>", paste(items, collapse = "\n"), "</body></html>")
  
  writeLines(html, file.path(dir, "index.html"))
}

make_index_dashboard(OUT_DIR)


