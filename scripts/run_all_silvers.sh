#!/usr/bin/env bash
# Run every silver flow sequentially. Waits for the raw runner to finish if it
# is still going, then keeps going through silvers on failure.
set -u
cd "$(dirname "$0")/.."

# If the raw runner is still going, wait for it (best-effort).
while pgrep -f run_all_raws.sh >/dev/null 2>&1; do
  echo "waiting for run_all_raws.sh to finish..."
  sleep 10
done

FLOWS=(
  dados.silver.al_ibge_censoagro.tbl_1909_2006
  dados.silver.al_ibge_censoagro.tbl_1931_2006
  dados.silver.al_ibge_censoagro.tbl_2233_2006
  dados.silver.al_ibge_censoagro.tbl_2284_2006
  dados.silver.al_ibge_censoagro.tbl_2337_2006
  dados.silver.al_ibge_censoagro.tbl_2518_2006
  dados.silver.al_ibge_censoagro.tbl_2782_2006
  dados.silver.al_ibge_censoagro.tbl_6885_2017
  dados.silver.al_ibge_censoagro.tbl_6898_2017
  dados.silver.al_ibge_censoagro.tbl_6899_2017
  dados.silver.al_ibge_censoagro.tbl_6949_2017
  dados.silver.al_ibge_censoagro.tbl_6955_2017
  dados.silver.al_ibge_censoagro.tbl_6957_2017
  dados.silver.al_ibge_pam.lavoura_permanente
  dados.silver.al_ibge_pam.lavoura_temporaria
  dados.silver.al_ibge_pevs.extracao_vegetal
  dados.silver.br_ibge_pac.tbl_1407
  dados.silver.br_ibge_pas.tbl_2715
  dados.silver.br_ibge_pia.tbl_1849
  dados.silver.br_ibge_pof.tbl_2393
  dados.silver.br_ibge_pof.tbl_6970
)

mkdir -p logs/silver_run
SUMMARY=logs/silver_run/_summary.txt
: > "$SUMMARY"

pass=0
fail=0
for mod in "${FLOWS[@]}"; do
  log="logs/silver_run/${mod//./_}.log"
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
echo "Silver summary: $pass passed, $fail failed (of ${#FLOWS[@]})"
echo "=========================================="
cat "$SUMMARY"
