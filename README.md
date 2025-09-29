# Pegada de Carbono: Cabotagem vs Vias Terrestres (MVP)

Este projeto é o **MVP** do Trabalho de Formatura *“Pegada de Carbono: Cabotagem vs Vias Terrestres”*.
O objetivo é estimar o consumo de combustível, custo e emissões de CO₂ no transporte de contêineres entre **São Paulo (origem fixa)** e um conjunto de **capitais brasileiras**, comparando o modal **rodoviário direto** com o **cabotagem (rodoviário + marítimo + portuário)**.
Além dos cálculos, o sistema gera um **mapa de calor** mostrando para quais destinos a cabotagem é mais vantajosa em relação ao transporte rodoviário.

---

## ✨ Destinos considerados no MVP

* Rio de Janeiro (RJ)
* Brasília (DF)
* Fortaleza (CE)
* Salvador (BA)
* Belo Horizonte (MG)
* Manaus (AM)
* Curitiba (PR)
* Recife (PE)
* Goiânia (GO)
* Porto Alegre (RS)

---

## ⚙️ Estrutura do MVP

1. **Entrada (configuração):**

   * Origem: São Paulo, SP
   * Portos: Santos (porto de origem) + porto mais próximo do destino
   * Distâncias: tabelas simplificadas (rodoviário + marítimo)
   * Parâmetros fixos: consumo médio de caminhão vazio/carregado, consumo de navio por contêiner-km, operações portuárias, preços de combustível e fatores de emissão de CO₂

2. **Cálculos por destino:**

   * **Rodoviário puro:** São Paulo → destino (km → litros → custo + CO₂)
   * **Cabotagem:**

     * SP → Porto de Santos (rodoviário)
     * Santos → Porto do destino (marítimo)
     * Porto do destino → cidade (rodoviário)
     * Operações portuárias
   * **Comparação:** diferença de custo e CO₂ entre cabotagem e rodoviário

3. **Saídas do MVP:**

   * `results.csv` → tabela com métricas por destino
   * `heatmap_cost.html` → mapa de calor comparando custo
   * `heatmap_co2.html` → mapa de calor comparando emissões

---

## 🛠️ Tecnologias

* **Python 3.12+**
* **Pandas** → manipulação de dados
* **Folium** → geração de mapas interativos
* **GeoPandas** (futuro) → manipulação de shapefiles para malhas maiores
* **Config em JSON** → parâmetros de consumo, distâncias e preços

---

## 🚀 Próximos Passos

* [ ] Implementar funções de cálculo por etapa (rodoviário, marítimo, portuário)
* [ ] Estruturar base mínima de distâncias rodoviárias e marítimas
* [ ] Gerar `results.csv` para os 10 destinos
* [ ] Renderizar mapas de calor (custo e CO₂)

---

## 📌 Limitações do MVP

* Distâncias aproximadas (tabelas fixas, sem API de rotas).
* Parâmetros médios de consumo e emissões simplificados.
* Apenas 1 origem (São Paulo) e porto fixo de saída (Santos).
