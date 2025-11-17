import pandas as pd
import sqlite3
from pathlib import Path

# =========================
# 1) غيّر هنا اسم ملف الإكسل
# =========================
EXCEL_FILE = "db.xlsx"   # اكتب هنا اسم ملف الإكسل بتاعك
SQLITE_DB = "data.db"         # اسم ملف قاعدة البيانات اللي هيتعمل

def main():
    excel_path = Path(EXCEL_FILE)

    if not excel_path.is_file():
        raise FileNotFoundError(f"لم يتم العثور على ملف الإكسل: {excel_path.resolve()}")

    print(f"جاري قراءة ملف الإكسل: {excel_path.name}")

    # sheet_name=None => يقرأ كل الشيتات في الملف
    all_sheets = pd.read_excel(excel_path, sheet_name=None)

    # فتح / إنشاء قاعدة بيانات SQLite
    conn = sqlite3.connect(SQLITE_DB)

    try:
        for sheet_name, df in all_sheets.items():
            # تجهيز اسم الجدول من اسم الشيت
            table_name = str(sheet_name).strip().replace(" ", "_")

            print(f"- جاري حفظ الشيت '{sheet_name}' في جدول SQLite اسمه '{table_name}'")

            # كتابة الداتا في جدول SQLite
            df.to_sql(table_name, conn, if_exists="replace", index=False)

        print("\n✓ تم التحويل بنجاح.")
        print(f"قاعدة البيانات الناتجة: {Path(SQLITE_DB).resolve()}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
