import sqlite3
import pandas as pd


class SQLiteAgent:

    def __init__(self):
        # 3 independent DB files
        self.engine_db = "engine_data.db"
        self.service_db = "service_history.db"
        self.body_db = "body_shop_data.db"

    # ---------------- PRIVATE DB CONNECT ----------------
    def _connect(self, db_name):
        return sqlite3.connect(db_name)

    # ---------------- WRITE DF → DB ----------------
    def sync_engine(self, df):
        if df is None or df.empty:
            return
        conn = self._connect(self.engine_db)
        df.to_sql("engine_data", conn, if_exists="replace", index=False)
        conn.close()

    def sync_service(self, df):
        if df is None or df.empty:
            return
        conn = self._connect(self.service_db)
        df.to_sql("service_history", conn, if_exists="replace", index=False)
        conn.close()

    def sync_body(self, df):
        if df is None or df.empty:
            return
        conn = self._connect(self.body_db)
        df.to_sql("body_shop", conn, if_exists="replace", index=False)
        conn.close()

    # ---------------- READ DB BY VEHICLE ----------------
    def get_engine_by_vehicle(self, vehicle_number: str):
        return self._select_by_vehicle(self.engine_db, "engine_data", vehicle_number)

    def get_service_by_vehicle(self, vehicle_number: str):
        return self._select_by_vehicle(self.service_db, "service_history", vehicle_number)

    def get_body_by_vehicle(self, vehicle_number: str):
        return self._select_by_vehicle(self.body_db, "body_shop", vehicle_number)

    # ---------------- INTERNAL SELECT ----------------
    def _select_by_vehicle(self, db_name, table_name, vehicle_number):
        conn = self._connect(db_name)
        try:
            query = f"""
                SELECT *
                FROM {table_name}
                WHERE vehicle_number = ?
            """
            df = pd.read_sql(query, conn, params=(vehicle_number,))
        except Exception:
            df = pd.DataFrame()
        conn.close()
        return df
