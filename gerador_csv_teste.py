import pandas as pd
import numpy as np
import os

def load_base_data(base_file_path):
    """Carrega o CSV original ou cria um dataframe sintético básico se não existir."""
    if os.path.exists(base_file_path):
        print(f"✅ Arquivo base '{base_file_path}' encontrado. Gerando mutações a partir dele...")
        df = pd.read_csv(base_file_path)
    else:
        print(f"⚠️ Arquivo '{base_file_path}' não encontrado. Criando base 100% sintética para os testes...")
        # Cria uma base sintética de 60 segundos (1 minuto) de direção normal
        df = pd.DataFrame({
            "VIN": ["9BWAH5BZ4KP599863"] * 60,
            "speed": np.linspace(20, 60, 60), # Acelera suavemente de 20 a 60 km/h
            "rpm": np.linspace(1500, 2500, 60) + np.random.normal(0, 50, 60), # RPM varia com um pouco de ruído humano
            "intake_air_temperature": [30.0] * 60,
            "intake_manifold_absolut_pressure": [100.0] * 60,
            "mass_air_flow": np.linspace(5.0, 15.0, 60), # MAF normal
            "fuel_type": ["Gasolina"] * 60
        })
        
    # Garantir que a coluna fuel_type exista (seu script usa ela)
    if 'fuel_type' not in df.columns:
        df['fuel_type'] = "Gasolina"
        
    return df

def generate_scenarios(base_df, output_dir):
    """Gera diversos CSVs modificados para testar os agentes do LangGraph."""
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"\n🏭 Iniciando fábrica de cenários na pasta '{output_dir}/'...")

    # ---------------------------------------------------------
    # CENÁRIOS POSITIVOS (Para gerar Créditos de Carbono)
    # ---------------------------------------------------------
    
    # 1. Viagem Perfeita (Base limpa)
    df_clean = base_df.copy()
    df_clean.to_csv(f"{output_dir}/01_viagem_perfeita.csv", index=False)
    print("   -> Criado: 01_viagem_perfeita.csv (Controle/Ideal)")

    # 2. Eco-Motorista (Velocidade constante, RPM muito baixo)
    df_eco = base_df.copy()
    df_eco['speed'] = 50.0
    df_eco['rpm'] = df_eco['rpm'] * 0.6  # Reduz RPM em 40%
    if 'mass_air_flow' in df_eco.columns:
        df_eco['mass_air_flow'] = df_eco['mass_air_flow'] * 0.6 # Menos ar = menos injeção = alta economia
    df_eco.to_csv(f"{output_dir}/02_eco_motorista.csv", index=False)
    print("   -> Criado: 02_eco_motorista.csv (Deve gerar alto crédito)")

    # 3. Viagem com Etanol (Testando a bifurcação da física estequiométrica)
    df_etanol = base_df.copy()
    df_etanol['fuel_type'] = 'Etanol'
    df_etanol.to_csv(f"{output_dir}/03_viagem_etanol.csv", index=False)
    print("   -> Criado: 03_viagem_etanol.csv (Testa o multiplicador AFR de 9.1)")

    # 4. Falha do Sensor MAF (Testando o Fallback para a Lei dos Gases Ideais)
    df_no_maf = base_df.copy()
    df_no_maf['mass_air_flow'] = np.nan # Deleta o MAF, forçando o script a estimar com Pressão e Temp
    df_no_maf.to_csv(f"{output_dir}/04_falha_sensor_maf_mas_valido.csv", index=False)
    print("   -> Criado: 04_falha_sensor_maf_mas_valido.csv (Deve ser Aprovado pelo Juiz)")

    # ---------------------------------------------------------
    # CENÁRIOS NEGATIVOS E FRAUDES (Onde o Agente Investigador deve brilhar)
    # ---------------------------------------------------------

    # 5. O "Pé de Chumbo" (Não é fraude, mas emite muito CO2, não deve ganhar token)
    df_agressivo = base_df.copy()
    df_agressivo['speed'] = np.random.uniform(80, 120, len(df_agressivo))
    df_agressivo['rpm'] = np.random.uniform(4000, 6000, len(df_agressivo)) # RPM nas alturas
    if 'mass_air_flow' in df_agressivo.columns:
        df_agressivo['mass_air_flow'] = df_agressivo['mass_air_flow'] * 2.5
    df_agressivo.to_csv(f"{output_dir}/05_motorista_agressivo.csv", index=False)
    print("   -> Criado: 05_motorista_agressivo.csv (Cálculo Físico deve dar 0g salvos)")

    # 6. Fraude de GPS (Teletransporte / Velocidade Impossível)
    df_gps_hack = base_df.copy()
    df_gps_hack.loc[5:10, 'speed'] = 350.0 # Picos de 350 km/h
    df_gps_hack.to_csv(f"{output_dir}/06_fraude_velocidade_impossivel.csv", index=False)
    print("   -> Criado: 06_fraude_velocidade_impossivel.csv (Juiz deve REJEITAR)")

    # 7. Fraude de "Guincho" ou Simulador (Carro em movimento com motor desligado)
    df_engine_off = base_df.copy()
    df_engine_off['speed'] = 60.0
    df_engine_off['rpm'] = 0.0 # O carro está a 60km/h mas o motor está desligado!
    df_engine_off.to_csv(f"{output_dir}/07_fraude_motor_desligado_em_movimento.csv", index=False)
    print("   -> Criado: 07_fraude_motor_desligado_em_movimento.csv (Investigador deve dar Score 10)")

    # 8. Script/Robô Gerando Dados (O Agente 2 precisa detectar o desvio padrão zero)
    df_bot = base_df.copy()
    df_bot['rpm'] = 1500.0 # Um pé humano NUNCA mantém exatos 1500 RPM cravados por uma viagem inteira
    df_bot['speed'] = 40.0
    df_bot['intake_manifold_absolut_pressure'] = 100.0
    df_bot.to_csv(f"{output_dir}/08_fraude_bot_gerador_de_dados.csv", index=False)
    print("   -> Criado: 08_fraude_bot_gerador_de_dados.csv (Física fica perfeita, mas IA deve notar o padrão robótico)")

    # 9. Anomalia de Temperatura (Sonda Lambida/Sensor Hackeado)
    df_temp_hack = base_df.copy()
    df_temp_hack['intake_air_temperature'] = -200.0 # Temperatura irreal para tentar quebrar o cálculo dos gases ideais
    df_temp_hack.to_csv(f"{output_dir}/09_fraude_sensor_temperatura_hackeado.csv", index=False)
    print("   -> Criado: 09_fraude_sensor_temperatura_hackeado.csv (Testa os limites da ferramenta Cientista)")

    # 10. Arquivo Corrompido / Vazio (Teste de Resiliência de Erro de Software)
    df_broken = pd.DataFrame() # DataFrame completamente vazio
    df_broken.to_csv(f"{output_dir}/10_arquivo_vazio_corrompido.csv", index=False)
    print("   -> Criado: 10_arquivo_vazio_corrompido.csv (Deve ser tratado graciosamente sem travar o LangGraph)")
    
    print("\n✅ Todos os cenários gerados com sucesso!")

if __name__ == "__main__":
    # Nome do seu arquivo real atual
    ARQUIVO_BASE = "../data/polo/viagem_polo_2.csv"
    PASTA_DESTINO = "csv_testes_polo_viagem_2"
    
    df_base = load_base_data(ARQUIVO_BASE)
    generate_scenarios(df_base, PASTA_DESTINO)