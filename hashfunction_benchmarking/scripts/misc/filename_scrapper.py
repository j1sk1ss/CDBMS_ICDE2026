import os
import csv

def is_short_filename(filename):
    if '.' not in filename:
        return False
    
    name, ext = os.path.splitext(filename)
    ext = ext.lstrip('.')
    return len(name) <= 8 and len(ext) <= 3

def main(names_csv="short_names.csv", exts_csv="short_exts.csv"):
    names = set()
    exts = set()

    for root, _, files in os.walk("/", topdown=True, onerror=lambda e: None):
        for file in files:
            try:
                if is_short_filename(file):
                    name, ext = os.path.splitext(file)
                    ext = ext.lstrip('.')
                    names.add(name)
                    exts.add(ext)
            except Exception:
                continue

    with open(names_csv, "w", newline="", encoding="utf-8") as f_name:
        writer = csv.writer(f_name)
        writer.writerow(["name"])
        for name in sorted(names):
            writer.writerow([name])

    with open(exts_csv, "w", newline="", encoding="utf-8") as f_ext:
        writer = csv.writer(f_ext)
        writer.writerow(["extension"])
        for ext in sorted(exts):
            writer.writerow([ext])

    print(f"Found {len(names)} names and {len(exts)} extensions.")
    print(f"CSV-files: {names_csv}, {exts_csv}")

if __name__ == "__main__":
    main()
