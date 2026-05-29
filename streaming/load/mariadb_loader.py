import pymysql
from config.settings import DB_CONFIG

class MariaLoader:

    def __init__(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor()

    def upsert(self, table, data):
        try:
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            updates = ", ".join([f"{k}=%s" for k in data.keys()])

            sql = f"""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {updates}
            """

            values = list(data.values()) * 2
            
            print("SQL:", sql)
            print("Values:", values)
            
            self.cursor.execute(sql, values)
            self.conn.commit()
            
            print("✅ upsert exitoso:", data)

        except Exception as e:
            print("❌ error en upsert:", e)
            raise e

    def delete(self, table, pk_name, id_value):
        sql = f"DELETE FROM {table} WHERE {pk_name}=%s"
        self.cursor.execute(sql, (id_value,))
        self.conn.commit()
