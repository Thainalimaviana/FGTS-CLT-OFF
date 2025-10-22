from flask import Flask, request, jsonify, render_template, send_file
import requests
import pandas as pd
import io
import sqlite3
from datetime import datetime, timedelta
import os
from database import init_db
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
import time

app = Flask(__name__)

parar_execucao = False
token_atual = None
token_expira_em = None

TOKEN_URL = "https://webservice.facta.com.br/gera-token"
TOKEN_AUTH_HEADER = "Basic OTY1NTI6ZjRzaXV0azJ1ZWNhNDVldXhnOXc="
API_URL = "https://webservice.facta.com.br/consignado-trabalhador/autoriza-consulta"

DB_FILE = "consultas.db"
RESULT_FOLDER = "resultados"
os.makedirs(RESULT_FOLDER, exist_ok=True)
os.makedirs("recuperacoes", exist_ok=True)

executor = ThreadPoolExecutor(max_workers=3)
fila_reprocessamento = Queue()
tentativas_reprocessamento = {}
MAX_TENTATIVAS = 3

def gerar_token():
    global token_atual, token_expira_em
    try:
        resp = requests.get(
            TOKEN_URL,
            headers={"Authorization": TOKEN_AUTH_HEADER, "Accept": "application/json"},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        novo_token = data.get("token")
        if not novo_token:
            raise RuntimeError(f"Não recebi token. Resposta: {resp.text}")
        token_atual = novo_token
        token_expira_em = datetime.now() + timedelta(minutes=59)
        print(f"token gerado: {token_atual[:15]}... válido até {token_expira_em.strftime('%H:%M:%S')}")
        return token_atual
    except Exception as e:
        print(f"Erro ao gerar token: {e}")
        token_atual = None
        token_expira_em = None
        raise

def garantir_token():
    global token_atual, token_expira_em
    if token_atual is None or token_expira_em is None:
        return gerar_token()
    if datetime.now() >= token_expira_em:
        return gerar_token()
    return token_atual

init_db()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/registrar-lote", methods=["POST"])
def registrar_lote():
    data = request.get_json(silent=True) or {}
    cpfs = data.get("cpfs", [])
    lote_id = data.get("lote_id")

    if not lote_id or not isinstance(cpfs, list) or not cpfs:
        return jsonify({"erro": "lote_id e lista de cpfs são obrigatórios"}), 400

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_consultas_cpf_lote ON consultas(cpf, lote_id)")
        rows = [(cpf, "Pendente", ts, lote_id) for cpf in cpfs]
        c.executemany(
            "INSERT OR IGNORE INTO consultas (cpf, resultado, data, lote_id) VALUES (?, ?, ?, ?)", rows
        )
        conn.commit()
        conn.close()
        return jsonify({"ok": True, "lote_id": lote_id, "total_registrados": len(rows)})
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

def worker_reprocessar():
    print("Worker de reprocessamento iniciado e aguardando CPFs...")
    while True:
        try:
            if not fila_reprocessamento.empty():
                cpf, lote_id = fila_reprocessamento.get()
                tentativas = tentativas_reprocessamento.get(cpf, 0) + 1

                if tentativas > MAX_TENTATIVAS:
                    print(f"⚠️ CPF {cpf} atingiu o máximo de {MAX_TENTATIVAS} tentativas.")
                    conn = sqlite3.connect(DB_FILE)
                    c = conn.cursor()
                    c.execute("SELECT resultado FROM consultas WHERE cpf=? AND lote_id=?", (cpf, lote_id))
                    row = c.fetchone()
                    resultado_antigo = row[0] if row else ""
                    novo_resultado = (
                        f"{resultado_antigo}, "
                        f"Status: Falhou após {MAX_TENTATIVAS} tentativas, "
                        f"Mensagem: Base offline ou erro de conexão"
                    )
                    c.execute(
                        "UPDATE consultas SET resultado=?, data=? WHERE cpf=? AND lote_id=?",
                        (novo_resultado, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), cpf, lote_id)
                    )
                    conn.commit()
                    conn.close()
                    time.sleep(5)
                    continue

                tentativas_reprocessamento[cpf] = tentativas
                print(f" Reprocessando CPF {cpf} (tentativa {tentativas}/{MAX_TENTATIVAS})...")

                token = garantir_token()
                try:
                    response = requests.get(
                        API_URL,
                        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                        params={"cpf": cpf},
                        timeout=15,
                    )

                    if response.status_code == 200:
                        resp_json = response.json()
                        msg = resp_json.get("mensagem", "")
                        dados_trab = resp_json.get("dados_trabalhador", {}).get("dados", [])

                        if dados_trab:
                            info = dados_trab[0]
                            resultado_str = (
                                f"Nome: {info.get('nome','-')}, "
                                f"Data Nascimento: {info.get('dataNascimento','-')}, "
                                f"Data Admissao: {info.get('dataAdmissao','-')}, "
                                f"Valor Liberado: {info.get('valorTotalVencimentos','-')}, "
                                f"Margem: {info.get('valorMargemDisponivel','-')}, "
                                f"Elegível: {info.get('elegivel','-')}, "
                                f"Status: {'Autorizado' if info.get('elegivel') == 'SIM' or 'autorizado' in msg.lower() else 'Não autorizado'}, "
                                f"Mensagem: {msg}"
                            )

                            conn = sqlite3.connect(DB_FILE)
                            c = conn.cursor()
                            c.execute(
                                "UPDATE consultas SET resultado=?, data=? WHERE cpf=? AND lote_id=?",
                                (resultado_str, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), cpf, lote_id)
                            )
                            conn.commit()
                            conn.close()
                            print(f"CPF {cpf} reprocessado com sucesso!")
                        else:
                            print(f" CPF {cpf} ainda sem dados — voltará para a fila (tentativa {tentativas}/{MAX_TENTATIVAS}).")
                            fila_reprocessamento.put((cpf, lote_id))

                    else:
                        print(f" HTTP {response.status_code} ao reprocessar {cpf} — voltará para a fila.")
                        conn = sqlite3.connect(DB_FILE)
                        c = conn.cursor()
                        c.execute("SELECT resultado FROM consultas WHERE cpf=? AND lote_id=?", (cpf, lote_id))
                        row = c.fetchone()
                        resultado_antigo = row[0] if row else ""
                        novo_resultado = f"{resultado_antigo}, Mensagem: Erro HTTP {response.status_code} no reprocessamento"
                        c.execute(
                            "UPDATE consultas SET resultado=?, data=? WHERE cpf=? AND lote_id=?",
                            (novo_resultado, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), cpf, lote_id)
                        )
                        conn.commit()
                        conn.close()
                        fila_reprocessamento.put((cpf, lote_id))

                except Exception as e:
                    print(f"Erro ao reprocessar {cpf}: {e}")
                    conn = sqlite3.connect(DB_FILE)
                    c = conn.cursor()
                    c.execute("SELECT resultado FROM consultas WHERE cpf=? AND lote_id=?", (cpf, lote_id))
                    row = c.fetchone()
                    resultado_antigo = row[0] if row else ""
                    novo_resultado = f"{resultado_antigo}, Mensagem: Erro reprocessando ({e})"
                    c.execute(
                        "UPDATE consultas SET resultado=?, data=? WHERE cpf=? AND lote_id=?",
                        (novo_resultado, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), cpf, lote_id)
                    )
                    conn.commit()
                    conn.close()
                    fila_reprocessamento.put((cpf, lote_id))

                time.sleep(15)

            else:
                time.sleep(5)

        except Exception as e:
            print(f" Erro no loop principal do worker: {e}")
            time.sleep(10)

threading.Thread(target=worker_reprocessar, daemon=True).start()
print("🚀 Worker de reprocessamento iniciado!")

@app.route("/consultar", methods=["POST"])
def consultar():
    global parar_execucao
    data_in = request.get_json(silent=True) or {}
    cpfs = data_in.get("cpfs", [])
    lote_id = data_in.get("lote_id")

    if not isinstance(cpfs, list) or not cpfs:
        return jsonify({"erro": "Lista de CPFs vazia."}), 400

    resultados = []
    token = garantir_token()
    API_URL = "https://webservice.facta.com.br/consignado-trabalhador/autoriza-consulta"

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    ts_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for cpf in cpfs:
        c.execute(
            "INSERT OR IGNORE INTO consultas (cpf, resultado, data, lote_id) VALUES (?, ?, ?, ?)",
            (cpf, "Pendente", ts_inicio, lote_id)
        )
    conn.commit()
    conn.close()

    for cpf in cpfs:
        if parar_execucao:
            print("Consulta interrompida manualmente pelo usuário.")
            break

        resultado_final = {
            "CPF": cpf,
            "nome": "-",
            "data_nascimento": "-",
            "data_admissao": "-",
            "valor_liberado": "-",
            "margem": "-",
            "elegivel": "-",
            "mensagem": "",
            "status": "Não autorizado"
        }

        try:
            response = requests.get(
                API_URL,
                headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                params={"cpf": cpf},
                timeout=15,
            )

            print(f"[{cpf}] Status: {response.status_code}")
            print(f"[{cpf}] Response: {response.text}")

            resp_json = response.json() if response.status_code == 200 else {}

            if str(resp_json.get("mensagem", "")).lower().startswith("token inválido"):
                print(f"[{cpf}] Token inválido, gerando novo token...")
                token = gerar_token()
                response = requests.get(
                    API_URL,
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    params={"cpf": cpf},
                    timeout=15,
                )
                resp_json = response.json()

            if response.status_code == 200 and not resp_json.get("erro", True):
                msg = resp_json.get("mensagem", "")
                dados_trab = resp_json.get("dados_trabalhador", {}).get("dados", [])
                if dados_trab:
                    info = dados_trab[0]
                    resultado_final.update({
                        "nome": info.get("nome") or "-",
                        "data_nascimento": info.get("dataNascimento") or "-",
                        "data_admissao": info.get("dataAdmissao") or "-",
                        "valor_liberado": info.get("valorTotalVencimentos") or "-",
                        "margem": info.get("valorMargemDisponivel") or "-",
                        "elegivel": info.get("elegivel") or "-",
                        "mensagem": msg,
                        "status": "Autorizado" if info.get("elegivel") == "SIM" or "autorizado" in msg.lower() else "Não autorizado"
                    })
                else:
                    resultado_final["mensagem"] = msg or "Sem dados retornados"
            else:
                resultado_final["mensagem"] = resp_json.get("mensagem", f"Erro HTTP {response.status_code}")

        except Exception as e:
            erro_texto = str(e)
            resultado_final["mensagem"] = f"Erro: {erro_texto}"

            if "HTTPSConnectionPool" in erro_texto or "Max retries exceeded" in erro_texto:
                print(f"⚠️ {cpf} apresentou erro de conexão — será reprocessado em 15s.")
                fila_reprocessamento.put((cpf, lote_id))
                conn = sqlite3.connect(DB_FILE)
                c = conn.cursor()
                c.execute(
                    "UPDATE consultas SET resultado=?, data=? WHERE cpf=? AND lote_id=?",
                    (f"Status: Reprocessando (1/{MAX_TENTATIVAS}), Mensagem: {erro_texto}",
                     datetime.now().strftime("%Y-%m-%d %H:%M:%S"), cpf, lote_id)
                )
                conn.commit()
                conn.close()

        resultados.append(resultado_final)

        ts_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        resultado_str = (
            f"Nome: {resultado_final['nome']}, "
            f"Data Nascimento: {resultado_final['data_nascimento']}, "
            f"Data Admissao: {resultado_final['data_admissao']}, "
            f"Valor Liberado: {resultado_final['valor_liberado']}, "
            f"Margem: {resultado_final['margem']}, "
            f"Elegível: {resultado_final['elegivel']}, "
            f"Status: {resultado_final['status']}, "
            f"Mensagem: {resultado_final['mensagem']}"
        )

        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        if lote_id:
            c.execute(
                "UPDATE consultas SET resultado=?, data=? WHERE cpf=? AND lote_id=?",
                (resultado_str, ts_now, resultado_final["CPF"], lote_id)
            )
            if c.rowcount == 0:
                c.execute(
                    "INSERT INTO consultas (cpf, resultado, data, lote_id) VALUES (?, ?, ?, ?)",
                    (resultado_final["CPF"], resultado_str, ts_now, lote_id)
                )
        else:
            c.execute(
                "INSERT INTO consultas (cpf, resultado, data) VALUES (?, ?, ?)",
                (resultado_final["CPF"], resultado_str, ts_now)
            )
        conn.commit()
        conn.close()

    parar_execucao = False
    return jsonify(resultados)

@app.route("/status-lote", methods=["GET"])
def status_lote():
    lote_id = request.args.get("lote_id")
    if not lote_id:
        return jsonify({"erro": "lote_id é obrigatório"}), 400

    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("""
            SELECT cpf, resultado, data 
            FROM consultas 
            WHERE lote_id = ? 
            AND id IN (
                SELECT MAX(id) FROM consultas WHERE lote_id = ? GROUP BY cpf
            )
            ORDER BY id ASC
        """, (lote_id, lote_id))

        registros = c.fetchall()
        conn.close()

        dados = []
        for cpf, resultado, data in registros:
            cpf = cpf.strip().zfill(11)
            partes = {}
            for pedaco in resultado.split(","):
                if ":" in pedaco:
                    chave, valor = pedaco.split(":", 1)
                    partes[chave.strip()] = valor.strip()

            dados.append({
                "CPF": cpf,
                "Nome": partes.get("Nome", "-"),
                "Data Nascimento": partes.get("Data Nascimento", "-"),
                "Data Admissao": partes.get("Data Admissao", "-"),
                "Valor Liberado": partes.get("Valor Liberado", "-"),
                "Margem": partes.get("Margem", "-"),
                "Elegível": partes.get("Elegível", "-"),
                "Status": partes.get("Status", "-"),
                "Mensagem": partes.get("Mensagem", "-"),
                "Data": data
            })

        return jsonify(dados)
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route("/recuperar-consultas-excel", methods=["GET"])
def recuperar_excel():
    try:
        lote_id = request.args.get("lote_id")
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        if not lote_id:
            c.execute("SELECT lote_id FROM consultas WHERE lote_id IS NOT NULL ORDER BY id DESC LIMIT 1")
            row = c.fetchone()
            if not row:
                conn.close()
                return jsonify({"erro": "Nenhuma consulta encontrada"}), 404
            lote_id = row[0]

        c.execute("SELECT cpf, resultado, data FROM consultas WHERE lote_id = ? ORDER BY id DESC", (lote_id,))
        consultas = c.fetchall()
        conn.close()

        if not consultas:
            return jsonify({"erro": "Nenhuma consulta encontrada"}), 404

        dados_expandidos = []
        for cpf, resultado, data in consultas:
            partes = {}
            for pedaco in resultado.split(","):
                if ":" in pedaco:
                    chave, valor = pedaco.split(":", 1)
                    partes[chave.strip()] = valor.strip()

            dados_expandidos.append({
                "CPF": cpf,
                "Nome": partes.get("Nome", "-"),
                "Data Nascimento": partes.get("Data Nascimento", "-"),
                "Data Admissao": partes.get("Data Admissao", "-"),
                "Valor Liberado": partes.get("Valor Liberado", "-"),
                "Margem": partes.get("Margem", "-"),
                "Elegível": partes.get("Elegível", "-"),
                "Status": partes.get("Status", "-"),
                "Mensagem": partes.get("Mensagem", "-"),
                "Data": data
            })

        df = pd.DataFrame(dados_expandidos)
        arquivo_excel = os.path.join(RESULT_FOLDER, f"consultas_lote_{lote_id}.xlsx")
        df.to_excel(arquivo_excel, index=False)
        return send_file(arquivo_excel, as_attachment=True)

    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route("/baixar-excel", methods=["POST"])
def baixar_excel():
    data = request.get_json(silent=True) or {}
    dados = data.get("resultados", [])

    if not dados:
        try:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute("SELECT lote_id FROM consultas ORDER BY id DESC LIMIT 1")
            row = c.fetchone()
            if row:
                lote_id = row[0]
                c.execute("SELECT cpf, resultado, data FROM consultas WHERE lote_id=?", (lote_id,))
                consultas = c.fetchall()
                conn.close()

                dados = []
                for cpf, resultado, data in consultas:
                    partes = {
                        k.strip(): v.strip()
                        for k, v in (p.split(":") for p in resultado.split(",") if ":" in p)
                    }
                    dados.append({
                        "CPF": cpf,
                        "Nome": partes.get("Nome", "-"),
                        "Data Nascimento": partes.get("Data Nascimento", "-"),
                        "Data Admissao": partes.get("Data Admissao", "-"),
                        "Valor Liberado": partes.get("Valor Liberado", "-"),
                        "Margem": partes.get("Margem", "-"),
                        "Elegível": partes.get("Elegível", "-"),
                        "Status": partes.get("Status", "-"),
                        "Mensagem": partes.get("Mensagem", "-"),
                        "Data": data
                    })
        except Exception as e:
            print(f"Erro ao recuperar pendentes: {e}")

    if not dados:
        return jsonify({"erro": "Nenhum dado disponível para exportar."}), 400

    df = pd.DataFrame(dados)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Resultados")
    output.seek(0)

    download_name = f"FGTS_FACTA_CLT_ON_{datetime.now():%d-%m_%Hh%M}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=download_name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@app.route("/parar", methods=["POST"])
def parar():
    global parar_execucao
    parar_execucao = True
    print("Execução interrompida manualmente pelo usuário.")
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(debug=True, port=8800)
