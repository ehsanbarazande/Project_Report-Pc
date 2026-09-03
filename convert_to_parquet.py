from pathlib import Path
import pandas as pd

# تنظیمات مسیرها
BASE_DIR = Path(__file__).resolve().parent
UPLOADS_DIR = BASE_DIR / "uploads"
PARQUET_DIR = BASE_DIR / "parquet_data"
PARQUET_DIR.mkdir(exist_ok=True)

# تعریف تمام فایل‌هایی که باید تبدیل شوند
# کلید: نام خروجی parquet، مقدار: نام فایل اکسل در پوشه uploads
FILES = {
    "person": "person.xlsx",
    "master": "master.xlsx",
    "history": "history.xlsx",
    "vendor_master": "vendor_master.xlsx",
    "vendor_history": "vendor_history.xlsx",

}

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def main():
    for key, filename in FILES.items():
        src = UPLOADS_DIR / filename
        
        if not src.exists():
            print(f"[ERROR] فایل در مسیر زیر یافت نشد: {src}")
            continue

        print(f"[READ] در حال پردازش فایل: {src.name} ...")
        
        try:
            # خواندن فایل
            df = pd.read_excel(src, engine="openpyxl")
            df = normalize_columns(df)

            # ذخیره فایل در مسیر parquet_data
            out = PARQUET_DIR / f"{key}.parquet"
            df.to_parquet(out, index=False, engine="pyarrow", compression="snappy")
            print(f"[OK] فایل {out.name} با موفقیت ذخیره شد.")
            
        except Exception as e:
            print(f"[ERROR] خطا در تبدیل {filename}: {e}")

if __name__ == "__main__":
    main()
