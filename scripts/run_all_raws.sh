#!/usr/bin/env bash
# Run every raw flow sequentially. Keeps going on failure; prints a summary.
set -u
cd "$(dirname "$0")/.."

FLOWS=(
  dados.raw.al_ibge_censoagro.tbl_1909_2006
  dados.raw.al_ibge_censoagro.tbl_1931_2006
  dados.raw.al_ibge_censoagro.tbl_2233_2006
  dados.raw.al_ibge_censoagro.tbl_2284_2006
  dados.raw.al_ibge_censoagro.tbl_2337_2006
  dados.raw.al_ibge_censoagro.tbl_2518_2006
  dados.raw.al_ibge_censoagro.tbl_2782_2006
  dados.raw.al_ibge_censoagro.tbl_6885_2017
  dados.raw.al_ibge_censoagro.tbl_6898_2017
  dados.raw.al_ibge_censoagro.tbl_6899_2017
  dados.raw.al_ibge_censoagro.tbl_6949_2017
  dados.raw.al_ibge_censoagro.tbl_6955_2017
  dados.raw.al_ibge_censoagro.tbl_6957_2017
  dados.raw.al_ibge_pac.tbl_1407
  dados.raw.al_ibge_pam.lavoura_permanente
  dados.raw.al_ibge_pam.lavoura_temporaria
  dados.raw.al_ibge_pevs.extracao_vegetal
  dados.raw.al_ibge_ppm.efetivo_rebanhos
  dados.raw.al_ibge_ppm.producao_aquicultura
  dados.raw.al_ibge_ppm.producao_origem_animal
  dados.raw.pa_me_comex_stat.ncm_exportacao
  dados.raw.br_csalpha_diretorios_brasil.cnae_2
  dados.raw.br_csalpha_diretorios_brasil.nomenclatura_comum_mercosul
  dados.raw.br_csalpha_diretorios_brasil.prodlist_industria
  dados.raw.br_csalpha_diretorios_brasil.prodlist_pesca
  dados.raw.br_ibge_pas.tbl_2715
  dados.raw.br_ibge_pia.tbl_1848
  dados.raw.br_ibge_pia.tbl_1849
  dados.raw.br_ibge_pia.tbl_1987
  dados.raw.br_ibge_pia.tbl_1988
  dados.raw.br_ibge_pof.tbl_2393
  dados.raw.br_ibge_pof.tbl_6715_2018
  dados.raw.br_ibge_pof.tbl_6970
  dados.raw.pa_rf_rais.up_rais
)

SUMMARY=logs/raw_run/_summary.txt
: > "$SUMMARY"

pass=0
fail=0
for mod in "${FLOWS[@]}"; do
  log="logs/raw_run/${mod//./_}.log"
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
echo "Summary: $pass passed, $fail failed (of ${#FLOWS[@]})"
echo "=========================================="
cat "$SUMMARY"
