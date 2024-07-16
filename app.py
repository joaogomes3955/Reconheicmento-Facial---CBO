from flask import Flask, request, redirect, url_for, send_from_directory, render_template
import pandas as pd
import os

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
PROCESSED_FOLDER = 'processed'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

if not os.path.exists(PROCESSED_FOLDER):
    os.makedirs(PROCESSED_FOLDER)

def process_excel(file_path):
    df_new = pd.read_excel(file_path)

    df_new = df_new[~df_new['usuario'].str.contains('desconhecido|acesso negado', case=False, na=False)]
    df_new.columns = [col.lower() for col in df_new.columns]
    df_new = df_new.applymap(lambda s: s.lower() if type(s) == str else s)
    df_new['data_evento'] = pd.to_datetime(df_new['data_evento'], errors='coerce').dt.strftime('%d/%m/%Y')
    df_new = df_new.sort_values(by=['usuario', 'data_evento', 'horario_evento'])
    df_new = df_new.drop_duplicates()
    df_new['horario_evento'] = pd.to_datetime(df_new['horario_evento'], format='%H:%M:%S').dt.time
    df_new['datetime_evento'] = pd.to_datetime(df_new['data_evento'] + ' ' + df_new['horario_evento'].astype(str))
    df_new = df_new.sort_values(by=['usuario', 'data_evento', 'datetime_evento'])
    df_new['time_diff'] = df_new.groupby(['usuario', 'data_evento'])['datetime_evento'].diff().dt.total_seconds() / 60
    df_new = df_new[(df_new['time_diff'].isna()) | (df_new['time_diff'] >= 5)]
    df_new = df_new.drop(columns=['datetime_evento', 'time_diff'])
    df_new['tipo'] = df_new.groupby(['usuario', 'data_evento']).cumcount().apply(lambda x: 'entrada' if x % 2 == 0 else 'saida')
    df_new.loc[df_new.groupby(['usuario', 'data_evento']).head(1).index, 'tipo'] = 'entrada'
    df_new['horario_entrada'] = df_new.apply(lambda row: row['horario_evento'] if row['tipo'] == 'entrada' else None, axis=1)
    df_new['horario_saida'] = df_new.apply(lambda row: row['horario_evento'] if row['tipo'] == 'saida' else None, axis=1)
    df_new['horario_entrada'] = df_new.groupby(['usuario', 'data_evento'])['horario_entrada'].ffill()
    df_new['horario_saida'] = df_new.groupby(['usuario', 'data_evento'])['horario_saida'].bfill()
    df_new['grupo'] = df_new['grupo'].fillna('indefinido')
    df_final = df_new[df_new['tipo'] == 'entrada']
    df_final = df_final.drop(columns=['tipo', 'horario_evento'])
    df_final['diferenca_horario'] = (pd.to_datetime(df_final['horario_saida'].astype(str)) - 
                                     pd.to_datetime(df_final['horario_entrada'].astype(str))).dt.total_seconds() / 60
    df_final['diferenca_horario'] = df_final['diferenca_horario'].apply(lambda x: f"{int(x//60):02d}:{int(x%60):02d}" if pd.notna(x) else None)
    df_final['diferenca_horario_minutos'] = (pd.to_datetime(df_final['horario_saida'].astype(str)) - 
                                             pd.to_datetime(df_final['horario_entrada'].astype(str))).dt.total_seconds() / 60
    sum_diferencas_usuario = df_final.groupby('usuario')['diferenca_horario_minutos'].sum().reset_index()
    sum_diferencas_usuario['diferenca_total'] = sum_diferencas_usuario['diferenca_horario_minutos'].apply(lambda x: f"{int(x//60):02d}:{int(x%60):02d}")
    sum_diferencas_usuario = sum_diferencas_usuario.drop(columns=['diferenca_horario_minutos'])
    sum_diferencas_grupo = df_final.groupby('grupo')['diferenca_horario_minutos'].sum().reset_index()
    sum_diferencas_grupo['diferenca_total'] = sum_diferencas_grupo['diferenca_horario_minutos'].apply(lambda x: f"{int(x//60):02d}:{int(x%60):02d}")
    sum_diferencas_grupo = sum_diferencas_grupo.drop(columns=['diferenca_horario_minutos'])
    sum_diferencas_cargo_grupo = df_final.groupby(['cargo', 'grupo'])['diferenca_horario_minutos'].sum().reset_index()
    sum_diferencas_cargo_grupo['diferenca_total'] = sum_diferencas_cargo_grupo['diferenca_horario_minutos'].apply(lambda x: f"{int(x//60):02d}:{int(x%60):02d}")
    sum_diferencas_cargo_grupo = sum_diferencas_cargo_grupo.drop(columns=['diferenca_horario_minutos'])
    df_final = df_final.drop(columns=['diferenca_horario_minutos'])

    processed_file_path = os.path.join(app.config['PROCESSED_FOLDER'], 'RF_Analise.xlsx')
    with pd.ExcelWriter(processed_file_path, engine='xlsxwriter') as writer:
        df_final.to_excel(writer, sheet_name='Registros Processados', index=False)
        sum_diferencas_usuario.to_excel(writer, sheet_name='Soma por Usuario', index=False)
        sum_diferencas_grupo.to_excel(writer, sheet_name='Soma por Grupo', index=False)
        sum_diferencas_cargo_grupo.to_excel(writer, sheet_name='Soma por Cargo e Grupo', index=False)
    
    return processed_file_path

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)
    
    if file:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(file_path)
        processed_file_path = process_excel(file_path)
        return redirect(url_for('download_file', filename=os.path.basename(processed_file_path)))

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['PROCESSED_FOLDER'], filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
