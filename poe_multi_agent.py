import os
import json
import pandas as pd
import numpy as np
from typing import TypedDict, Dict, Any
from langgraph.graph import StateGraph, START, END
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_ollama import ChatOllama


def log_section(title: str):
    print(f"\n{'=' * 72}")
    print(title)
    print(f"{'=' * 72}")


def log_kv(key: str, value: Any):
    print(f"   - {key}: {value}")

# ==========================================
# 1. DEFINIÇÃO DE ESTADO (Agora com debate de agentes)
# ==========================================
class PoEState(TypedDict):
    csv_file_path: str
    driver_wallet: str
    engine_size_cc: float 
    
    # Produtos de cada Agente
    math_summary: Dict[str, Any]      # O que o Agente Cientista calculou
    fraud_report: str                 # O que o Agente Investigador suspeita
    fraud_score: int                  # 0 (Limpo) a 10 (Fraude Absoluta)
    
    # Veredito Final do Juiz
    audit_passed: bool
    audit_report: str
    co2_saved_grams: int
    contract_payload: Dict[str, Any] 

# ==========================================
# 2. MOTOR DE FÍSICA (A Ferramenta do Cientista)
# ==========================================
DENSITY_AND_EMISSION = {
    "Gasolina": [737, 2310],
    "Diesel": [850, 2660],
    "Etanol": [789, 1510]
}

def estimate_maf(rpm, temp, pressure, engine_cc):
    """Estima o fluxo de massa de ar (MAF) usando a Lei dos Gases Ideais"""
    VE = 0.8
    R = 8.3146
    M_air = 28.87
    temp_k = temp + 273.15
    maf = (pressure * engine_cc * M_air * VE * rpm) / (R * temp_k * 120)
    return maf

def calculate_co2_physics(df: pd.DataFrame, engine_cc: float) -> Dict[str, Any]:
    co2_emissions = []
    fuel_type = df['fuel_type'].iloc[0] if 'fuel_type' in df.columns else "Gasolina"
    if fuel_type not in DENSITY_AND_EMISSION: fuel_type = "Gasolina"
        
    fuel_density, emission_factor_CO2 = DENSITY_AND_EMISSION[fuel_type]
    afr = 9.1 if fuel_type == "Etanol" else 14.7

    for idx, row in df.iterrows():
        maf = row.get('mass_air_flow')
        if pd.isna(maf) or maf == 0:
            rpm = row.get('rpm', 0)
            temp = row.get('intake_air_temperature', 25)
            pressure = row.get('intake_manifold_absolut_pressure', 100)
            maf = estimate_maf(rpm, temp, pressure, engine_cc) if rpm > 0 else 0

        if maf > 0:
            fuel_volume = maf / (afr * fuel_density)
            co2_emission = fuel_volume * emission_factor_CO2
            co2_emissions.append(co2_emission)
        else:
            co2_emissions.append(0)

    return round(np.sum(co2_emissions), 2)

def scientific_analysis_tool(file_path: str, engine_cc: float) -> Dict[str, Any]:
    """Ferramenta estrita e matemática. Não emite opiniões."""
    try:
        print("   [Cientista/Física] Lendo CSV e normalizando colunas numéricas...")
        df = pd.read_csv(file_path)
        print(f"   [Cientista/Física] Linhas recebidas: {len(df)}")
        for col in ['speed', 'rpm', 'intake_air_temperature', 'intake_manifold_absolut_pressure', 'mass_air_flow']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Coleta de métricas puras
        duration_seconds = len(df)
        avg_speed_kmh = df['speed'].mean() if 'speed' in df.columns else 0
        max_speed = df['speed'].max() if 'speed' in df.columns else 0
        distance_km = (avg_speed_kmh * (duration_seconds / 3600))
        
        zero_rpm_moving = len(df[(df['rpm'] < 500) & (df['speed'] > 20)]) if 'rpm' in df.columns else 0
        std_rpm = df['rpm'].std() if 'rpm' in df.columns else 0 # Desvio padrão do motor (útil para IA ver se é robô)

        actual_co2_emitted = calculate_co2_physics(df, engine_cc)
        baseline_co2_expected = distance_km * 250 
        co2_saved_grams = max(0, baseline_co2_expected - actual_co2_emitted)

        # --- Robust outlier detection (MAD / modified z-score) ---
        def modified_z_scores(s: pd.Series) -> pd.Series:
            s_valid = s.replace(0, np.nan).dropna()
            if len(s_valid) == 0:
                return pd.Series([0] * len(s), index=s.index)
            med = np.median(s_valid)
            mad = np.median(np.abs(s_valid - med))
            if mad == 0:
                return pd.Series([0] * len(s), index=s.index)
            mz = 0.6745 * (s - med) / mad
            return mz

        outlier_sensors = ['speed', 'rpm', 'mass_air_flow']
        outlier_counts = {}
        outlier_row_mask = pd.Series(False, index=df.index)
        for sensor in outlier_sensors:
            if sensor in df.columns:
                mz = modified_z_scores(df[sensor])
                is_out = mz.abs() > 3.5
                outlier_counts[sensor] = int(is_out.sum())
                outlier_row_mask = outlier_row_mask | is_out.fillna(False)
            else:
                outlier_counts[sensor] = 0

        outlier_pct = float(outlier_row_mask.sum()) / max(1, len(df))

        # --- Sensor null ratios (helpful to detect MAF degeneration) ---
        key_sensors = ['mass_air_flow', 'intake_air_temperature', 'intake_manifold_absolut_pressure']
        sensor_nulls = {s: (1.0 if s not in df.columns else float(df[s].isna().mean())) for s in key_sensors}

        # --- MAF degeneration gate and zero-CO2 degraded signal ---
        status = 'success'
        reason = None
        if all(sensor_nulls[s] >= 0.40 for s in key_sensors):
            status = 'degraded'
            reason = 'MAF estimation impossible — key sensors absent'
        if actual_co2_emitted == 0 and distance_km > 0:
            status = 'degraded'
            reason = reason or 'Calculated CO2 == 0 while distance > 0; data absent'

        print("   [Cientista/Física] Métricas calculadas com sucesso.")
        log_kv("distance_km", round(distance_km, 2))
        log_kv("duration_seconds", duration_seconds)
        log_kv("avg_speed_kmh", round(avg_speed_kmh, 2))
        log_kv("max_speed_kmh", round(max_speed, 2))
        log_kv("zero_rpm_moving_seconds", zero_rpm_moving)
        log_kv("rpm_standard_deviation", round(std_rpm, 2))
        log_kv("actual_co2_emitted_grams", round(actual_co2_emitted, 2))
        log_kv("baseline_co2_expected_grams", round(baseline_co2_expected, 2))
        log_kv("co2_saved_grams", int(co2_saved_grams))
        log_kv("outlier_pct", round(outlier_pct, 3))
        log_kv("outlier_counts", outlier_counts)
        log_kv("sensor_nulls", sensor_nulls)
        if status == 'degraded':
            log_kv('status', status)
            log_kv('reason', reason)

        return {
            "status": status,
            "reason": reason,
            "distance_km": round(distance_km, 2),
            "duration_seconds": duration_seconds,
            "max_speed_kmh": round(max_speed, 2),
            "avg_speed_kmh": round(avg_speed_kmh, 2),
            "zero_rpm_moving_seconds": zero_rpm_moving,
            "rpm_standard_deviation": round(std_rpm, 2),
            "actual_co2_emitted_grams": round(actual_co2_emitted, 2),
            "co2_saved_grams": int(co2_saved_grams),
            "outlier_counts": outlier_counts,
            "outlier_pct": round(outlier_pct, 4),
            "sensor_nulls": sensor_nulls
        }
    except Exception as e:
        print(f"   [Cientista/Física] Falha ao processar CSV: {e}")
        return {"status": "error", "message": str(e)}

# ==========================================
# 3. NÓS DOS AGENTES (O COMITÊ)
# ==========================================

def data_scientist_node(state: PoEState) -> PoEState:
    """AGENTE 1: Executa a matemática dura e passa os dados para a frente."""
    log_section("🧮 [Agente 1 - Cientista] Início da análise físico-matemática")
    log_kv("csv_file_path", state.get('csv_file_path'))
    log_kv("engine_size_cc", state.get('engine_size_cc', 1.0))
    summary = scientific_analysis_tool(state['csv_file_path'], state.get('engine_size_cc', 1.0))
    print("   [Agente 1 - Cientista] Resultado produzido e enviado ao Investigador.")
    log_kv("status", summary.get('status'))
    log_kv("co2_saved_grams", summary.get('co2_saved_grams', 0))
    return {"math_summary": summary}

def fraud_investigator_node(state: PoEState) -> PoEState:
    """AGENTE 2: Avalia o comportamento (LLM). Tenta encontrar fraudes."""
    log_section("🕵️‍♂️ [Agente 2 - Investigador] Início da auditoria de fraude")
    summary = state['math_summary']
    print("   [Investigador] Recebeu resumo matemático do Cientista.")
    log_kv("distance_km", summary.get('distance_km', 0))
    log_kv("co2_saved_grams", summary.get('co2_saved_grams', 0))
    log_kv("zero_rpm_moving_seconds", summary.get('zero_rpm_moving_seconds', 0))
    log_kv("rpm_standard_deviation", summary.get('rpm_standard_deviation', 0))
    
    if summary.get("status") == "error":
         print("   [Investigador] Resumo veio com erro. Marcando como risco máximo.")
         return {"fraud_report": "Erro na leitura dos dados. Não foi possível auditar.", "fraud_score": 10}
     # If the scientist reports degraded data (e.g., MAF sensors absent), treat as maximum risk
    if summary.get("status") == "degraded":
        print("   [Investigador] Resumo degradado detectado pelo Cientista. Marcando como risco máximo.")
        reason = summary.get('reason', 'Degraded data from scientist')
        return {"fraud_report": f"Dados degradados: {reason}", "fraud_score": 10}

    try:
        llm = ChatOllama(model="gemma4:e4b", temperature=0, base_url="http://localhost:11434") # Temperatura baixa para ser analítico
        prompt = f"""
        You are an elite Cyber-Physical Security Investigator analyzing vehicle telemetry for a Carbon Credit system.
        Review this mathematical summary generated by the data engineering team:
        {json.dumps(summary, indent=2)}
        
        Look for behavioral anomalies that suggest GPS spoofing, synthetic data generation, or tampering.
        Examples:
        - 'zero_rpm_moving_seconds' > 0 means the car is moving with the engine off (Highly suspicious, unless it's a known hybrid).
        - Extremely low 'rpm_standard_deviation' (< 50) suggests a robot/script generated the data, not a human foot on a pedal.
        - Impossible max speeds.
        
        Provide a concise analysis report. End your report with a FRAUD_SCORE from 0 (Perfectly human/clean) to 10 (Obvious fraud).
        Example format: "Analysis: [your reasoning]. FRAUD_SCORE: 8"
        """
        print("   [Investigador] Chamando LLM para avaliação comportamental...")
        response = llm.invoke([SystemMessage(content=prompt)])
        report = response.content
        
        # Extrair o score com um simples parse (fallback para 0 se falhar)
        score = 0
        if "FRAUD_SCORE:" in report:
            try:
                score_str = report.split("FRAUD_SCORE:")[1].strip()
                score = int(''.join(filter(str.isdigit, score_str)))
            except:
                score = 5 # Score neutro em caso de falha de parse
                
        print("   [Investigador] LLM retornou análise textual.")
        log_kv("fraud_score", min(score, 10))
        print(f"   [Investigador] Trecho do relatório: {str(report)[:240]}...")
        return {"fraud_report": report, "fraud_score": min(score, 10)}
        
    except Exception as e:
        print(f"   [Aviso] LLM indisponível para Investigador ({e}). Usando heurística.")
        # Heuristic fallback: combine zero-rpm evidence and outlier rate
        if summary.get("status") == "degraded":
            score = 10
        else:
            if summary.get("zero_rpm_moving_seconds", 0) > 5:
                score = 10
            elif summary.get("outlier_pct", 0) > 0.20:
                score = 8
            else:
                score = 0
        log_kv("fraud_score_heuristico", score)
        return {"fraud_report": "Heurística de fallback utilizada.", "fraud_score": score}

def judge_oracle_node(state: PoEState) -> PoEState:
    """AGENTE 3: Toma a decisão final baseada no Cientista e no Investigador."""
    log_section("⚖️ [Agente 3 - Juiz Oráculo] Julgamento final")
    
    summary = state['math_summary']
    fraud_report = state['fraud_report']
    fraud_score = state['fraud_score']
    print("   [Juiz] Evidências recebidas.")
    log_kv("fraud_score", fraud_score)
    log_kv("distance_km", summary.get('distance_km', 0))
    log_kv("co2_saved_grams", summary.get('co2_saved_grams', 0))
    
    try:
        llm = ChatOllama(model="gemma4:e4b", temperature=0.1, base_url="http://localhost:11434")
        prompt = f"""
        You are the Chief Auditor (The Oracle) for a Blockchain Proof of Economy system.
        You must make the final decision on whether to mint Carbon Credits for a driver.
        
        EVIDENCE A (Mathematical Reality):
        {json.dumps(summary, indent=2)}
        
        EVIDENCE B (Fraud Investigator's Report):
        {fraud_report}
        Risk Score: {fraud_score}/10
        
        DECISION RULES:
        1. If the Fraud Risk Score is 7 or higher, you MUST reject the trip.
        2. If the distance is 0 or CO2 saved is 0, reject.
        3. Otherwise, approve it and praise the stoichiometric reduction.
        
        OUTPUT FORMAT:
        First line: MUST be exactly 'VERDICT: APPROVED' or 'VERDICT: REJECTED'.
        Following lines: A comprehensive, professional audit report summarizing the physical data and the security checks. This report will be saved permanently on the blockchain.
        """
        print("   [Juiz] Consultando LLM para laudo de auditoria final...")
        response = llm.invoke([SystemMessage(content=prompt)])
        final_report = response.content
        
        is_approved = "VERDICT: APPROVED" in final_report.upper()
        log_kv("veredito", 'APROVADO ✅' if is_approved else 'REJEITADO ❌')
        print(f"   [Juiz] Relatório completo:\n{final_report}")
        
        return {
            "audit_passed": is_approved,
            "audit_report": final_report,
            "co2_saved_grams": summary.get("co2_saved_grams", 0) if is_approved else 0
        }
    except Exception as e:
        print(f"   [Aviso] LLM indisponível para o Juiz ({e}). Decisão automática (Score < 5).")
        is_approved = fraud_score < 5
        log_kv("veredito_automatico", 'APROVADO ✅' if is_approved else 'REJEITADO ❌')
        return {
            "audit_passed": is_approved,
            "audit_report": f"Decisão automática baseada no score de risco {fraud_score}.",
            "co2_saved_grams": summary.get("co2_saved_grams", 0) if is_approved else 0
        }

def contract_preparer_node(state: PoEState) -> PoEState:
    """O Nó final que empacota para o Smart Contract"""
    log_section("🧾 [Nó Final - Blockchain] Preparação de payload")
    if not state.get("audit_passed"):
        print("   [Blockchain] Transação cancelada devido à rejeição do Juiz.")
        return {"contract_payload": {}}
        
    print("   [Blockchain] Preparando transação para a rede Besu...")
    payload = {
        "function_to_call": "mintVerifiedCredit",
        "arguments": {
            "driver": state["driver_wallet"],
            "co2Grams": state["co2_saved_grams"],
            "tokenURI": "ipfs://<METADATA>", 
            "auditReport": state["audit_report"] 
        }
    }
    log_kv("function_to_call", payload["function_to_call"])
    log_kv("driver", payload["arguments"]["driver"])
    log_kv("co2Grams", payload["arguments"]["co2Grams"])
    return {"contract_payload": payload}

# ==========================================
# 4. CONSTRUÇÃO DO GRAFO (O WORKFLOW)
# ==========================================
workflow = StateGraph(PoEState)

# Adicionando os Agentes do Comitê
workflow.add_node("Scientist", data_scientist_node)
workflow.add_node("Investigator", fraud_investigator_node)
workflow.add_node("Judge", judge_oracle_node)
workflow.add_node("ContractPreparer", contract_preparer_node)

# O Fluxo do Comitê
workflow.add_edge(START, "Scientist")
workflow.add_edge("Scientist", "Investigator")
workflow.add_edge("Investigator", "Judge")
workflow.add_edge("Judge", "ContractPreparer")
workflow.add_edge("ContractPreparer", END)

poe_oracle_app = workflow.compile()

# ==========================================
# 5. EXECUÇÃO
# ==========================================
if __name__ == "__main__":
    test_dir = "../data_synthetic/csv_testes_polo_viagem_2"
    if not os.path.isdir(test_dir):
         print(f"❌ Erro: A pasta '{test_dir}' não foi encontrada.")
         exit()

    test_files = sorted(
        [
            os.path.join(test_dir, file_name)
            for file_name in os.listdir(test_dir)
            if file_name.lower().endswith(".csv")
        ]
    )

    if not test_files:
        print(f"❌ Erro: Nenhum arquivo CSV encontrado em '{test_dir}'.")
        exit()

    log_section("🚦 INICIANDO COMITÊ DE AUDITORIA IA (LANGGRAPH)")
    print("Fluxo: Scientist -> Investigator -> Judge -> ContractPreparer")
    log_kv("total_arquivos_csv", len(test_files))
    log_kv("driver_wallet", "0xABCDEF...")
    log_kv("engine_size_cc", 1.0)

    for index, test_csv in enumerate(test_files, start=1):
        log_section(f"🧪 TESTE {index}/{len(test_files)} | Arquivo: {os.path.basename(test_csv)}")
        initial_state = {
            "csv_file_path": test_csv,
            "driver_wallet": "0xABCDEF...",
            "engine_size_cc": 1.0
        }

        log_kv("csv_file_path", initial_state["csv_file_path"])
        final_state = poe_oracle_app.invoke(initial_state)

        log_section("🚀 RESULTADO FINAL DO WORKFLOW")
        log_kv("audit_passed", final_state.get('audit_passed'))
        log_kv("fraud_score", final_state.get('fraud_score'))
        log_kv("co2_saved_grams", final_state.get('co2_saved_grams'))
        print("\n--- PAYLOAD FINAL PARA O SMART CONTRACT ---")
        if final_state.get('contract_payload'):
            print(json.dumps(final_state['contract_payload'], indent=2))
        else:
            print("{}")