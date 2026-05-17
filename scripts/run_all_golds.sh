#!/usr/bin/env bash
# Run every gold flow sequentially. Waits for the silver runner to finish if it
# is still going, then keeps going through golds on failure.
set -u
cd "$(dirname "$0")/.."

# If the silver runner is still going, wait for it (best-effort).
while pgrep -f run_all_silvers.sh >/dev/null 2>&1; do
  echo "waiting for run_all_silvers.sh to finish..."
  sleep 10
done

FLOWS=(
  dados.gold.pa_indexadores_producao_rural.censo_2006_2017_despesas
  dados.gold.pa_indexadores_producao_rural.censo_2006_extracao_vegetal
  dados.gold.pa_indexadores_producao_rural.censo_2006_lavoura_permanente
  dados.gold.pa_indexadores_producao_rural.censo_2006_lavoura_temporaria
  dados.gold.pa_indexadores_producao_rural.censo_2006_lavoura_temporaria_2284
  dados.gold.pa_indexadores_producao_rural.censo_2017_extracao_vegetal
  dados.gold.pa_indexadores_producao_rural.censo_2017_lavoura_permanente
  dados.gold.pa_indexadores_producao_rural.censo_2017_lavoura_temporaria
  dados.gold.pa_indexadores_producao_rural.pam_lavoura_permanente
  dados.gold.pa_indexadores_producao_rural.pam_lavoura_temporaria
  dados.gold.pa_indexadores_producao_rural.pevs_extracao_vegetal
  dados.gold.pa_indexadores_custo_producao_rural.censo_2006_2017_despesas
  dados.gold.pa_indexadores_valor_producao_rural.censo_2006_2017_valor_producao
  dados.gold.pa_indexadores_valor_producao_rural.censo_2017_pessoal_ocupado_producao_rural
  dados.gold.pa_servicos_industria_comercio.pac_comercio
  dados.gold.pa_servicos_industria_comercio.pas_servicos
  dados.gold.pa_servicos_industria_comercio.pia_industrias
  dados.gold.br_despesas_familiares.pof_2018_despesas_familiares_situacao_domicilio
  dados.gold.brasil_despesas_familiares.pof_2018_despesas_familiares_situacao_domicilio
  dados.gold.br_servicos.pas_servicos
  dados.gold.pa_coeficientes_custo.preparacao_camada_custo
  dados.gold.br_coeficientes_consumo.preparacao_camada_consumo
  dados.gold.br_coeficientes_exportacao.preparacao_camada_exportacao
  dados.gold.br_coeficientes_investimento.preparacao_camada_investimento
  dados.gold.br_coeficientes_renda.preparacao_camada_renda
)

mkdir -p logs/gold_run
SUMMARY=logs/gold_run/_summary.txt
: > "$SUMMARY"

pass=0
fail=0
for mod in "${FLOWS[@]}"; do
  log="logs/gold_run/${mod//./_}.log"
  echo "=== $mod ==="
  start=$(date +%s)
  if uv run python -m "$mod" >"$log" 2>&1; then
    dur=$(( $(date +%s) - start ))
    echo "  OK  (${dur}s)  -> $log"
    echo "PASS  ${dur}s  $mod" >> "$SUMMARY"
    pass=$((pass+1))
  else
    rc=$?
    dur=$(( $(date +%s) - start ))
    echo "  FAIL rc=$rc (${dur}s)  -> $log"
    echo "FAIL  ${dur}s  rc=$rc  $mod" >> "$SUMMARY"
    fail=$((fail+1))
  fi
done

echo
echo "=========================================="
echo "Gold summary: $pass passed, $fail failed (of ${#FLOWS[@]})"
echo "=========================================="
cat "$SUMMARY"
