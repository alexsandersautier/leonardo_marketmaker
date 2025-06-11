# captura_dados_rlp.py
import threading
import sqlite3
import time
from queue import Queue, Empty
import cd3_connector
import logging
from collections import defaultdict

def format_horario(h):
    h = str(h).zfill(9)
    return f"{h[:2]}:{h[2:4]}:{h[4:6]}.{h[6:9]}"

def parse_gqt_message(msg):
    parts = msg.split(":")
    if not msg.startswith("V:") or len(parts) < 12:
        return None
    preco = int(parts[4])
    return {
        "tipo_mensagem": parts[0],
        "ativo": parts[1],
        "operacao": parts[2],
        "horario": parts[3],
        "preco": preco,
        "corretora_comprou": parts[5],
        "corretora_vendeu": parts[6],
        "quantidade": int(parts[7]) if parts[7].isdigit() else 0,
        "id_negocio": parts[8],
        "condicao_trade": parts[9],
        "agressor": parts[10],
        "condicao_trade_original": parts[11] if len(parts) > 11 else ""
    }

class Receiver:
    def __init__(self, user, password, ativo="WINM25", db_path="negocios_log.db"):
        self.ativo = ativo
        self._conn = cd3_connector.CD3Connector(
            user, password,
            self._on_disconnect,
            self._on_message,
            self._on_connect,
            active_conflated=True,
            log_level=logging.INFO,
            log_path="C:\\logs_cd3connector"
        )
        self._queue = Queue()
        self._signal = threading.Event()
        self._restart_conn = True
        self._consumer = threading.Thread(target=self._process_messages)
        self._consumer.start()

        self.ultimo_agressor = None
        self.ultimo_preco = None
        self.rlp_liquido = 0
        self.db_path = db_path

        self.agressao_compra = 0
        self.agressao_venda = 0
        self.saldo_compra = 0
        self.saldo_venda = 0
        self.passivo_compra = 0
        self.passivo_venda = 0
        self.exposicao_pf = 0
        self.historico_pf = []
        self.market_maker_map = defaultdict(int)

        self._setup_database()

    def _setup_database(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS negocios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ativo TEXT,
                operacao TEXT,
                horario TEXT,
                preco INTEGER,
                quantidade INTEGER,
                corretora_comprou TEXT,
                corretora_vendeu TEXT,
                agressor TEXT,
                rlp TEXT,
                rlp_liquido INTEGER,
                id_negocio TEXT,
                condicao_trade TEXT
            )
        """)
        self.conn.commit()

    def start(self):
        self._conn.start()

    def join(self):
        self._signal.set()
        self._consumer.join()

    def _on_connect(self):
        print("Conectado ao CD3!")
        self._conn.send_command(f"gqt {self.ativo} s")

    def _on_disconnect(self):
        print("Desconectado do CD3!")
        if self._restart_conn:
            print("Reconectando automaticamente...")
            self._conn.start()

    def _on_message(self, msg: str):
        self._queue.put(msg)

    def _process_messages(self):
        while not self._signal.is_set():
            try:
                msg = self._queue.get(timeout=1)
            except Empty:
                continue

            if msg.startswith("V:"):
                trade = parse_gqt_message(msg)
                if trade:
                    if trade['agressor'] == 'A':
                        self.ultimo_agressor = "COMPRA"
                        self.ultimo_preco = trade['preco']
                    elif trade['agressor'] == 'V':
                        self.ultimo_agressor = "VENDA"
                        self.ultimo_preco = trade['preco']

                    rlp_tag = "NÃO"
                    ajuste_rlp = 0
                    if trade['condicao_trade'] == '2' or 'RL' in trade['condicao_trade_original']:
                        if self.ultimo_agressor == "COMPRA":
                            rlp_tag = "RLP COMPRADOR"
                            ajuste_rlp = int(trade['quantidade'])
                        elif self.ultimo_agressor == "VENDA":
                            rlp_tag = "RLP VENDEDOR"
                            ajuste_rlp = -int(trade['quantidade'])
                        else:
                            rlp_tag = "RLP INDEFINIDO"
                            ajuste_rlp = 0
                        self.rlp_liquido += ajuste_rlp

                    # Métricas
                    if trade['agressor'] == 'A':
                        self.agressao_compra += trade['quantidade']
                        self.saldo_compra += trade['quantidade']
                    elif trade['agressor'] == 'V':
                        self.agressao_venda += trade['quantidade']
                        self.saldo_venda += trade['quantidade']
                    else:
                        if trade['operacao'] == 'C':
                            self.passivo_compra += trade['quantidade']
                        elif trade['operacao'] == 'V':
                            self.passivo_venda += trade['quantidade']

                    # Pessoa física (exemplo usando corretora 630 - XP)
                    if trade['corretora_vendeu'] in ['630', '127']:
                        self.exposicao_pf += trade['quantidade']
                        self.historico_pf.append((trade['preco'], trade['quantidade']))

                    # Market maker
                    if trade['corretora_comprou'] == trade['corretora_vendeu']:
                        self.market_maker_map[trade['corretora_comprou']] += 1

                    horario_formatado = format_horario(trade['horario'])

                    print(f"{trade['ativo']} {horario_formatado} {trade['preco']} qtd={trade['quantidade']} RLP={rlp_tag} Agressor={trade['agressor']}")

                    tentativas = 0
                    while tentativas < 10:
                        try:
                            self.cursor.execute("""
                                INSERT INTO negocios (
                                    ativo, operacao, horario, preco, quantidade,
                                    corretora_comprou, corretora_vendeu, agressor,
                                    rlp, rlp_liquido, id_negocio, condicao_trade
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                trade['ativo'], trade['operacao'], horario_formatado, trade['preco'], trade['quantidade'],
                                trade['corretora_comprou'], trade['corretora_vendeu'], trade['agressor'],
                                rlp_tag, self.rlp_liquido, trade['id_negocio'], trade['condicao_trade']
                            ))
                            self.conn.commit()
                            break
                        except sqlite3.OperationalError as e:
                            if "database is locked" in str(e):
                                print("Banco travado, tentando novamente...")
                                time.sleep(2)
                                tentativas += 1
                            else:
                                raise

            elif msg.lower() in [
                "invalid login.",
                "software key not found.",
                "you don't have any permission for this software."
            ]:
                print("Erro crítico recebido:", msg)
                self._restart_conn = False
                return
            self._queue.task_done()

def main():
    receiver = Receiver("leonardo_socket", "Zlc14D", ativo="WINM25")
    receiver.start()
    receiver.join()

if __name__ == "__main__":
    main()
